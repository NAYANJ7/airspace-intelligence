"""
Entry point.

    python -m src.main            -> continuous polling (Ctrl+C or SIGTERM to stop)
    python -m src.main --once     -> a single poll cycle then exit

Credit tracking: OpenSky doesn't reliably return X-Rate-Limit-Remaining for
this account (confirmed in Phase 1), so remaining credits can't be read
from the API directly. This instead estimates cost-per-request from the
bounding box area (OpenSky's own published pricing: <=25 sq deg = 1 credit,
25-100 = 2, 100-400 = 3, >400 or worldwide = 4) and keeps a running total
for THIS PROCESS's lifetime, printed every few seconds. Restarting the
container resets the counter to 0 even though OpenSky's real daily budget
doesn't reset until UTC midnight - this is a per-run estimate, not a
persisted ledger.
"""

import argparse
import signal
import sys
import time

from . import config
from .deduplicator import ChangeDetector
from .kafka_producer import KafkaPublisher
from .normalizer import normalize_states
from .opensky_client import (
    OpenSkyAPIError, OpenSkyAuthError, OpenSkyClient,
    OpenSkyNetworkError, OpenSkyRateLimitError,
)
from .storage import write_processed
from .validator import validate_observations

_RETRYABLE_ERRORS = (OpenSkyAuthError, OpenSkyRateLimitError, OpenSkyAPIError, OpenSkyNetworkError)
_MAX_BACKOFF_SECONDS = 300


def estimate_credit_cost(bbox: dict) -> int:
    """OpenSky's published /states/all pricing, by bounding-box area."""
    if not bbox:
        return 4
    area = (bbox["lamax"] - bbox["lamin"]) * (bbox["lomax"] - bbox["lomin"])
    if area <= 25:
        return 1
    if area <= 100:
        return 2
    if area <= 400:
        return 3
    return 4


class CreditTracker:
    """In-memory, per-run ESTIMATE only - see module docstring."""

    def __init__(self, cost_per_request: int, daily_budget: int):
        self.cost_per_request = cost_per_request
        self.daily_budget = daily_budget
        self.requests_made = 0
        self.credits_used = 0

    def record_successful_request(self) -> None:
        self.requests_made += 1
        self.credits_used += self.cost_per_request

    def status_line(self) -> str:
        remaining = self.daily_budget - self.credits_used
        return (
            f"[credits] ~{self.credits_used}/{self.daily_budget} used this run "
            f"(~{remaining} left of today's budget) - {self.requests_made} request(s) "
            f"@ {self.cost_per_request} credit(s) each"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OpenSky ingestion service.")
    parser.add_argument("--once", action="store_true", help="Run a single poll cycle and exit.")
    parser.add_argument("--lamin", type=float, default=None)
    parser.add_argument("--lomin", type=float, default=None)
    parser.add_argument("--lamax", type=float, default=None)
    parser.add_argument("--lomax", type=float, default=None)
    return parser.parse_args()


def run_cycle(
    client: OpenSkyClient,
    detector: ChangeDetector,
    kafka_publisher: KafkaPublisher,
    bbox: dict,
    cycle_num: int,
    credit_tracker: CreditTracker,
) -> None:
    """One poll -> normalize -> validate -> detect changes -> save (Parquet + Kafka) -> print summary."""
    result = client.get_states(**bbox)
    credit_tracker.record_successful_request()

    if not result["time"]:
        print(f"Cycle {cycle_num}: API response had no snapshot time - skipping.")
        return

    df = normalize_states(result["states"], result["time"])
    received = len(df)

    valid_df, invalid_df = validate_observations(df)
    valid_count, invalid_count = len(valid_df), len(invalid_df)

    changed_df, duplicates = detector.filter_changed(valid_df)

    try:
        kafka_connected, kafka_produced = kafka_publisher.publish(changed_df)
    except Exception as e:
        print(f"Kafka error (ignored, Parquet unaffected): {e}", file=sys.stderr)
        kafka_connected, kafka_produced = False, 0

    saved_path = write_processed(changed_df, config.PROCESSED_DATA_DIR)

    print("=" * 50)
    print("OPEN SKY INGESTION")
    print("=" * 18)
    print(f"Cycle: {cycle_num}")
    print(f"Records received: {received}")
    print(f"Valid records: {valid_count}")
    print(f"Invalid records: {invalid_count}")
    print(f"New observations: {len(changed_df)}")
    print(f"Duplicates skipped: {duplicates}")
    print(f"Saved to: {saved_path if saved_path else '(nothing new to save this cycle)'}")
    print(f"Kafka topic: aircraft_states")
    print(f"Kafka: {'CONNECTED' if kafka_connected else 'UNAVAILABLE'}")
    print(f"Kafka messages produced: {kafka_produced}")
    print(credit_tracker.status_line())
    print(f"Next poll in: {config.POLL_INTERVAL_SECONDS} seconds")
    print("=" * 50)


def _sleep_with_credit_heartbeat(total_seconds: float, credit_tracker: CreditTracker) -> None:
    """Sleeps in small chunks, printing the credit status each chunk - so the
    running total is visible any time you check, not just right after a
    cycle, and so Ctrl+C/SIGTERM during a long wait is noticed within a few
    seconds rather than up to a full poll interval."""
    elapsed = 0.0
    while elapsed < total_seconds:
        chunk = min(config.CREDIT_HEARTBEAT_SECONDS, total_seconds - elapsed)
        time.sleep(chunk)  # a KeyboardInterrupt here propagates straight up - see main()
        elapsed += chunk
        print(credit_tracker.status_line())


def main() -> int:
    config.validate_config()
    args = parse_args()

    # `docker compose stop` sends SIGTERM, not SIGINT (Ctrl+C) - map it onto
    # the same KeyboardInterrupt handling below so shutdown is clean either way.
    def _handle_sigterm(signum, frame):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    bbox = {"lamin": args.lamin, "lomin": args.lomin, "lamax": args.lamax, "lomax": args.lomax}
    bbox = {k: v for k, v in bbox.items() if v is not None}
    if bbox and len(bbox) != 4:
        print(
            "ERROR: To filter by bounding box you must provide all four of "
            "--lamin/--lomin/--lamax/--lomax together.",
            file=sys.stderr,
        )
        return 1

    client = OpenSkyClient(config.opensky_clientid, config.opensky_clientsecret)
    detector = ChangeDetector()
    kafka_publisher = KafkaPublisher(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    credit_tracker = CreditTracker(
        cost_per_request=estimate_credit_cost(bbox),
        daily_budget=config.DAILY_CREDIT_BUDGET,
    )

    if args.once:
        try:
            run_cycle(client, detector, kafka_publisher, bbox, cycle_num=1, credit_tracker=credit_tracker)
        except _RETRYABLE_ERRORS as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 1
        return 0

    print("Starting continuous polling. Ctrl+C (or `docker compose stop`) to stop.")
    print(credit_tracker.status_line())
    cycle = 0
    backoff = 1
    try:
        while True:
            cycle += 1
            try:
                run_cycle(client, detector, kafka_publisher, bbox, cycle_num=cycle, credit_tracker=credit_tracker)
                backoff = 1
            except _RETRYABLE_ERRORS as e:
                print(f"ERROR on cycle {cycle}: {e}", file=sys.stderr)
                print(f"Waiting {backoff}s before retrying (not hammering the API)...", file=sys.stderr)
                _sleep_with_credit_heartbeat(backoff, credit_tracker)
                backoff = min(backoff * 2, _MAX_BACKOFF_SECONDS)
                continue
            _sleep_with_credit_heartbeat(config.POLL_INTERVAL_SECONDS, credit_tracker)
    except KeyboardInterrupt:
        print("\nStop signal received - shutting down cleanly.")

    print(credit_tracker.status_line())
    return 0


if __name__ == "__main__":
    sys.exit(main())