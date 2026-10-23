"""one-short verification: read up to  5 messages from aircraft_states,print,exit """

import json,sys
from confluent_kafka import Consumer

TOPIC="aircraft_states"
MAX_MESSAGES= 5
POLL_TIMEOUT_SECONDS= 10

def main()->int:
    consumer=Consumer({
        "bootstrap.servers":"localhost:9092",
        "group.id":"aircraft_states_test_consumer",
        "auto.offset.reset":"earliest",
        "enable.auto.commit":False, # repeatable test runs, always re read from start 
    })

    consumer.subscribe([TOPIC])
    print("="*50);print("KAFKA CONSUMER TEST");print("="*19)

    received,consecutive_errors=0,0
    try:
        while received<MAX_MESSAGES:
            msg=consumer.poll(timeout=POLL_TIMEOUT_SECONDS)
            if msg is None:
                print(f"\nNo message received within {POLL_TIMEOUT_SECONDS}s-stopping")
                break
            if msg.error():
                consecutive_errors+=1
                print(f"consumer error: {msg.error()}",file=sys.stderr)
                if consecutive_errors>=3:
                    break
                continue
            consecutive_errors=0
            received+=1
            key=msg.key().decode("utf-8") if msg.key else None
            value=json.loads(msg.value().decode("utf-8"))
            print(f"\n Message {received}\n Key:{key}\nValue.")
            print(json.dumps(value,indent=2))
    finally:
        consumer.close()

    print()
    if received>=MAX_MESSAGES:
        print(f"Received {received} messages successfully.")
        print("Kafka connection and message delivery are working.")

    else:
        print(f"received {received}/{MAX_MESSAGES} messages before stopping")
    print("="*50)
    return 0 if received>0 else 1

if __name__=="__main__":
    sys.exit(main())