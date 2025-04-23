import os
import logging
import time
import argparse
import schedule
import json
from datetime import datetime
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from dotenv import load_dotenv

from tiktok_api import TikTokAPIClient
from db_models import User, Video, VideoMetricsHourly, get_db, init_db
from kafka_producer import TikTokKafkaProducer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

METRICS_PORT = int(os.getenv("PROMETHEUS_PORT", 8001))
ETL_SCHEDULE_INTERVAL = int(os.getenv("ETL_SCHEDULE_INTERVAL", 60))
ETL_RETRY_COUNT = int(os.getenv("ETL_RETRY_COUNT", 3))
ETL_RETRY_DELAY = int(os.getenv("ETL_RETRY_DELAY", 5))

API_REQUESTS = Counter('tiktok_api_requests_total', 'Total API requests')
API_ERRORS = Counter('tiktok_api_errors_total', 'Total API errors')
USERS_PROCESSED = Counter('users_processed_total', 'Total users processed')
VIDEOS_PROCESSED = Counter('videos_processed_total', 'Total videos processed')
DB_WRITES = Counter('db_writes_total', 'Total database writes')
PROCESSING_TIME = Histogram('processing_time_seconds', 'Time spent processing data')

class TikTokETLPipeline:
    def __init__(self):
        self.api_client = TikTokAPIClient()
        self.db = get_db()
        self.kafka_producer = TikTokKafkaProducer()
        
        target_accounts_str = os.getenv("TARGET_ACCOUNTS")
        target_urls_json = os.getenv("TARGET_ACCOUNT_URLS")
        
        if target_urls_json:
            try:
                self.target_accounts = json.loads(target_urls_json)
                logger.info(f"Loaded {len(self.target_accounts)} target accounts with URLs from environment")
            except json.JSONDecodeError:
                logger.error("Failed to parse TARGET_ACCOUNT_URLS from environment. Using default accounts.")
                self.target_accounts = self._get_default_accounts()
        elif target_accounts_str:
            usernames = [u.strip() for u in target_accounts_str.split(',')]
            self.target_accounts = {username: None for username in usernames}
            logger.info(f"Loaded {len(self.target_accounts)} target accounts from environment (without URLs)")
        else:
            logger.warning("No target accounts found in environment. Using default accounts.")
            self.target_accounts = self._get_default_accounts()
        
    def _get_default_accounts(self):
        return {
            "obschestvoznaika_el": "https://www.tiktok.com/@obschestvoznaika_el?_t=ZS-8s811wf5v9R&_r=1",
            "himichka_el": "https://www.tiktok.com/@himichka_el?_t=8s8CWkOM7Fw&_r=1",
            "anglichanka_el": "https://www.tiktok.com/@anglichanka_el?_t=8s8CQXHFybf&_r=1",
            "fizik_el": "https://www.tiktok.com/@fizik_el?_t=ZS-8s85SvseEQE&_r=1",
            "katya_matematichka": "https://www.tiktok.com/@katya_matematichka?_t=ZS-8s85aIz7A4F&_r=1"
        }
        
    def extract_users_data(self):
        logger.info("Extracting users data")
        start_time = time.time()
        
        try:
            users_data = self.api_client.get_multiple_users_data_sync(self.target_accounts)
            API_REQUESTS.inc(len(self.target_accounts))
            processing_time = time.time() - start_time
            PROCESSING_TIME.observe(processing_time)
            return users_data
        except Exception as e:
            logger.error(f"Error extracting users data: {e}")
            API_ERRORS.inc()
            return {}
    
    def extract_videos_data(self, user_id, full_url=None):
        logger.info(f"Extracting videos data for user {user_id}")
        
        try:
            videos_data = self.api_client.get_user_videos_sync(user_id, full_url=full_url)
            API_REQUESTS.inc()
            return videos_data
        except Exception as e:
            logger.error(f"Error extracting videos data for user {user_id}: {e}")
            API_ERRORS.inc()
            return {"videos": []}
    
    def transform_user_data(self, raw_data):
        transformed_users = []
        
        for username, data in raw_data.items():
            if "error" in data:
                logger.warning(f"Skipping user {username} due to error: {data['error']}")
                continue
                
            user_info = data.get("user_info", {})
            user_id = user_info.get("id")
            if not user_id:
                user_id = f"user_{username}_{int(time.time())}"
            
            user = {
                "id": user_id,
                "username": username,
                "display_name": user_info.get("nickname"),
                "bio": user_info.get("signature"),
                "follower_count": user_info.get("follower_count", 0),
                "following_count": user_info.get("following_count", 0),
                "updated_at": datetime.utcnow()
            }
            
            transformed_users.append(user)
            USERS_PROCESSED.inc()
            
        return transformed_users
    
    def transform_video_data(self, raw_videos, user_id):
        transformed_videos = []
        
        video_list = raw_videos.get("videos", [])
        
        for video in video_list:
            stats = video.get("statistics", {})
            
            video_id = video.get("id")
            if not video_id:
                video_id = f"video_{user_id}_{int(time.time())}_{len(transformed_videos)}"
            
            video_data = {
                "id": video_id,
                "user_id": user_id,
                "caption": video.get("desc", ""),
                "create_time": datetime.fromtimestamp(video.get("create_time", 0)),
                "like_count": stats.get("like_count", 0),
                "comment_count": stats.get("comment_count", 0),
                "view_count": stats.get("view_count", 0),
                "share_count": stats.get("share_count", 0),
                "collected_at": datetime.utcnow()
            }
            
            transformed_videos.append(video_data)
            VIDEOS_PROCESSED.inc()
            
        return transformed_videos
    
    def load_users_to_db(self, users):
        logger.info(f"Loading {len(users)} users to database")
        
        for user_data in users:
            user_id = user_data.get("id")
            
            if not user_id:
                user_id = f"user_{user_data['username']}_{int(time.time())}"
                user_data["id"] = user_id
            
            existing_user = self.db.query(User).filter(User.id == user_id).first()
            
            if existing_user:
                for key, value in user_data.items():
                    setattr(existing_user, key, value)
            else:
                new_user = User(**user_data)
                self.db.add(new_user)
            
            DB_WRITES.inc()
            
            self.kafka_producer.send_user_data(user_data["username"], user_data)
        
        self.db.commit()
        logger.info("Users loaded to database")
    
    def load_videos_to_db(self, videos):
        logger.info(f"Loading {len(videos)} videos to database")
        
        for video_data in videos:
            video_id = video_data.get("id")
            
            if not video_id:
                user_id = video_data.get("user_id", "unknown")
                video_id = f"video_{user_id}_{int(time.time())}_{hash(video_data.get('caption', ''))}"
                video_data["id"] = video_id
            
            existing_video = self.db.query(Video).filter(Video.id == video_id).first()
            
            if existing_video:
                metrics = VideoMetricsHourly(
                    video_id=video_id,
                    hour=datetime.utcnow().replace(minute=0, second=0, microsecond=0),
                    like_count=existing_video.like_count,
                    comment_count=existing_video.comment_count,
                    view_count=existing_video.view_count,
                    share_count=existing_video.share_count
                )
                self.db.add(metrics)
                
                for key, value in video_data.items():
                    setattr(existing_video, key, value)
            else:
                new_video = Video(**video_data)
                self.db.add(new_video)
            
            DB_WRITES.inc()
            
            self.kafka_producer.send_video_data(video_id, video_data)
        
        self.db.commit()
        logger.info("Videos loaded to database")
    
    def run_pipeline(self):
        logger.info("Starting ETL pipeline")
        start_time = time.time()
        
        try:
            users_data = self.extract_users_data()
            transformed_users = self.transform_user_data(users_data)
            self.load_users_to_db(transformed_users)
            
            for user in transformed_users:
                user_id = user.get("id")
                username = user.get("username")
                full_url = self.target_accounts.get(username) if isinstance(self.target_accounts, dict) else None
                videos_data = self.extract_videos_data(user_id, full_url=full_url)
                transformed_videos = self.transform_video_data(videos_data, user_id)
                self.load_videos_to_db(transformed_videos)
            
            processing_time = time.time() - start_time
            logger.info(f"ETL pipeline completed in {processing_time:.2f} seconds")
            return True
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return False

def init_metrics_server():
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Prometheus metrics server started on port {METRICS_PORT}")
    except Exception as e:
        logger.error(f"Failed to start Prometheus metrics server: {e}")

def run_scheduled_job():
    init_db()
    pipeline = TikTokETLPipeline()
    return pipeline.run_pipeline()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TikTok ETL Pipeline")
    parser.add_argument("--schedule", action="store_true", help="Run as a scheduled job every hour")
    parser.add_argument("--run-once", action="store_true", help="Run once and exit")
    parser.add_argument("--interval", type=int, help=f"Schedule interval in minutes (default: {ETL_SCHEDULE_INTERVAL})")
    args = parser.parse_args()
    
    init_metrics_server()
    interval = args.interval if args.interval else ETL_SCHEDULE_INTERVAL
    
    if args.schedule:
        logger.info(f"Starting scheduled ETL pipeline (every {interval} minutes)")
        schedule.every(interval).minutes.do(run_scheduled_job)
        run_scheduled_job()
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_scheduled_job()