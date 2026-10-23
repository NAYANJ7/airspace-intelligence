"""icao24 is the message key: same aircraft -> same partition -> ordered history."""

import json,logging
from typing import Tuple
import pandas as pd 

logger=logging.getLogger(__name__)
TOPIC="aircraft_states"

def json_safe(value):
    if pd.isna(value):
        return None
    if isinstance(value,pd.Timestamp):
        return value.isoformat()
    if hasattr(value,"item"):
        return value.item()
    return value

def row_to_message(record:dict)->Tuple[bytes,bytes]:
    key=str(record["icao24"]).encode("utf-8")
    value=json.dumps({k: json_safe(v) for k,v in record.items()}).encode("utf-8")
    return key,value

class KafkaPublisher:
    def __init__(self,bootstrap_servers:str="localhost:9092"):
        self._bootstrap_servers=bootstrap_servers
        self._producer=None

    def _get_producer(self):
        if self._producer is not None:
            return self._producer
        try:
            from confluent_kafka import Producer
            producer=Producer({"bootstrap.servers":self._bootstrap_servers})
            producer.list_topics(timeout=5)#actively probes the broker
            self._producer=producer
        except Exception as e:
            logger.warning("Kafka unavailable: %s",e)
            return None
        return self._producer

    def publish(self,df:pd.DataFrame)->Tuple[bool,int]:
        """returns (kafka_connected,messages_produced). never raises"""
        producer=self._get_producer()
        if producer is None:
            return False,0
        if df.empty:
            return True,0

        delivery_errors=[]
        def on_delivery(err,_msg):
            if err is not None:
                delivery_errors.append(str(err))
        produced=0
        for record in df.to_dict("records"):
            key,value=row_to_message(record)
            try:
                producer.produce(TOPIC,key=key,value=value,on_delivery=on_delivery)
                produced+=1
            except BufferError:
                producer.poll(1)
                try:
                    producer.produce(TOPIC,key=key,value=value,on_delivery=on_delivery)
                    produced+=1
                except BufferError as e:
                    logger.warning("Dropped one message- local queue full: %s",e)

        producer.flush(timeout=10)
        if delivery_errors:
            logger.warning("%d message(s) failed delivery:%s",len(delivery_errors),delivery_errors[:3])
        return True,produced-len(delivery_errors)