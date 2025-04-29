import logging
import time
import argparse
import schedule
import json
from datetime import datetime
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from pathlib import Path
from sqlalchemy.orm import sessionmaker

from config.config import (
    LOG_DIR, LOG_FILE, LOG_LEVEL, PROMETHEUS_PORT,
    ETL_SCHEDULE_INTERVAL, get_target_accounts
)
from tiktok_api import TikTokAPIClient
from db_models import User, Video, Base, engine
from kafka_producer import KafkaProducer

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(LOG_DIR) / LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PIPELINE_DURATION = Histogram('pipeline_duration_seconds', 'Time spent processing pipeline steps', ['step'])
PIPELINE_ERRORS = Counter('pipeline_errors_total', 'Total number of pipeline errors', ['step'])
PIPELINE_PROGRESS = Gauge('pipeline_progress', 'Current progress of the pipeline', ['step'])
DATA_VOLUME = Gauge('data_volume', 'Volume of data processed', ['type'])

def log_pipeline_step(step_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.info(f"Starting {step_name}")
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                PIPELINE_DURATION.labels(step=step_name).observe(duration)
                logger.info(f"Completed {step_name} in {duration:.2f} seconds")
                return result
            except Exception as e:
                PIPELINE_ERRORS.labels(step=step_name).inc()
                logger.error(f"Error in {step_name}: {str(e)}")
                raise
        return wrapper
    return decorator

class TikTokETLPipeline:
    def __init__(self):
        self.api_client = TikTokAPIClient()
        self.kafka_producer = KafkaProducer()
        self.target_accounts = get_target_accounts()
        self.state_file = Path(LOG_DIR) / "pipeline_state.json"
        self._load_state()
        self.setup_database()
    
    def setup_database(self):
        Base.metadata.create_all(engine)
    
    def _load_state(self):
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    state["processed_videos"] = set(state.get("processed_videos", []))
                    self.state = state
            else:
                self.state = {
                    "last_processed": {},
                    "processed_videos": set(),
                    "last_user_update": {}
                }
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            self.state = {
                "last_processed": {},
                "processed_videos": set(),
                "last_user_update": {}
            }
    
    def _save_state(self):
        try:
            state_to_save = self.state.copy()
            state_to_save["processed_videos"] = list(state_to_save["processed_videos"])
            with open(self.state_file, 'w') as f:
                json.dump(state_to_save, f)
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    def _update_progress(self, step, progress):
        PIPELINE_PROGRESS.labels(step=step).set(progress)
        logger.info(f"Progress for {step}: {progress:.2%}")
    
    @log_pipeline_step("extract_users")
    def extract_users_data(self):
        users_data = []
        total_accounts = len(self.target_accounts)
        
        for i, (username, url) in enumerate(self.target_accounts.items(), 1):
            try:
                last_update = self.state["last_user_update"].get(username)
                if last_update and (datetime.now() - datetime.fromisoformat(last_update)).total_seconds() < 3600:
                    logger.info(f"Skipping recently updated user: {username}")
                    continue
                    
                user_data = self.api_client.get_user_info(username, url)
                if user_data:
                    users_data.append(user_data)
                    self.state["last_user_update"][username] = datetime.now().isoformat()
                    self._update_progress("extract_users", i / total_accounts)
            except Exception as e:
                logger.error(f"Error extracting data for user {username}: {e}")
        DATA_VOLUME.labels(type="users").set(len(users_data))
        return users_data
    
    @log_pipeline_step("extract_videos")
    def extract_videos_data(self, users_data):
        videos_data = []
        total_users = len(users_data)
        
        for i, user_data in enumerate(users_data, 1):
            try:
                user_videos = self.api_client.get_user_videos(user_data["username"])
                new_videos = [v for v in user_videos if v["id"] not in self.state["processed_videos"]]
                videos_data.extend(new_videos)
                self._update_progress("extract_videos", i / total_users)
            except Exception as e:
                logger.error(f"Error extracting videos for user {user_data['username']}: {e}")
        DATA_VOLUME.labels(type="videos").set(len(videos_data))
        return videos_data
    
    @log_pipeline_step("transform")
    def transform_data(self, users_data, videos_data):
        users = [User(**user) for user in users_data]
        videos = [Video(**video) for video in videos_data]
        return users, videos
    
    @log_pipeline_step("load")
    def load_data(self, users, videos):
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            for user in users:
                session.merge(user)
            session.commit()
            for video in videos:
                if video.id not in self.state["processed_videos"]:
                    session.merge(video)
                    self.kafka_producer.send_video(video)
                    self.state["processed_videos"].add(video.id)
            session.commit()
            self._save_state()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    @log_pipeline_step("pipeline")
    def run_pipeline(self):
        try:
            users_data = self.extract_users_data()
            videos_data = self.extract_videos_data(users_data)
            users, videos = self.transform_data(users_data, videos_data)
            self.load_data(users, videos)
            self.state["last_processed"] = {
                "timestamp": datetime.now().isoformat(),
                "users_count": len(users),
                "videos_count": len(videos)
            }
            self._save_state()
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise
    
    def run_scheduled(self):
        schedule.every(ETL_SCHEDULE_INTERVAL).minutes.do(self.run_pipeline)
        while True:
            schedule.run_pending()
            time.sleep(1)

def main():
    parser = argparse.ArgumentParser(description="TikTok ETL Pipeline")
    parser.add_argument("--schedule", action="store_true", help="Run on schedule")
    args = parser.parse_args()
    pipeline = TikTokETLPipeline()
    start_http_server(PROMETHEUS_PORT)
    
    if args.schedule:
        pipeline.run_scheduled()
    else:
        pipeline.run_pipeline()

if __name__ == "__main__":
    main()