import os
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

class TikTokKafkaProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.topic = os.getenv("KAFKA_TOPIC")
        self.producer = self._create_producer()
        
    def _create_producer(self):
        try:
            return KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all',
                retries=3
            )
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def send_user_data(self, username, data):
        try:
            future = self.producer.send(
                self.topic,
                key=username,
                value={"type": "user_data", "data": data}
            )
            future.get(timeout=10)
            logger.info(f"Successfully sent user data for {username} to Kafka")
        except Exception as e:
            logger.error(f"Failed to send user data to Kafka: {e}")
    
    def send_video_data(self, video_id, data):
        try:
            future = self.producer.send(
                self.topic,
                key=video_id,
                value={"type": "video_data", "data": data}
            )
            future.get(timeout=10)
            logger.info(f"Successfully sent video data for {video_id} to Kafka")
        except Exception as e:
            logger.error(f"Failed to send video data to Kafka: {e}")
    
    def close(self):
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed") 