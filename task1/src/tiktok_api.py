import time
import random
import logging
import asyncio
import nest_asyncio
from functools import wraps

from config.config import (
    TOKEN_CACHE_TTL
)

logger = logging.getLogger(__name__)

nest_asyncio.apply()

def log_api_call(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"API call {func.__name__} completed in {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"API call {func.__name__} failed after {duration:.2f}s: {str(e)}")
            raise
    return wrapper

class TikTokAPIClient:
    def __init__(self):
        self.max_retries = 3
        self.retry_delay = 2
        self.timeout = 60.0
        self.api_instance = None
        self._loop = asyncio.get_event_loop()
        self._token_cache = {}
        self._response_cache = {}
        
        try:
            self.api_instance = self._loop.run_until_complete(self.initialize_api())
            if not self.api_instance:
                logger.warning("Failed to initialize TikTokApi. Some TikTok API operations will fail.")
        except Exception as e:
            logger.error(f"Error initializing TikTokApi: {e}")
    
    async def initialize_api(self):
        logger.info("Initializing TikTokApi instance")
        
        ms_token = self._get_cached_token("MS_TOKEN")
        verify_fp = self._get_cached_token("TIKTOK_VERIFY_FP")
        session_id = self._get_cached_token("TIKTOK_SESSIONID")
        
        if not any([ms_token, verify_fp, session_id]):
            logger.warning("No authentication tokens found. Please run get_tokens.py to obtain them.")
        
        try:
            from TikTokApi import TikTokApi
            api = TikTokApi(custom_verify_fp=verify_fp)
            await api.create_sessions(
                ms_tokens=[ms_token] if ms_token else None,
                session_ids=[session_id] if session_id else None,
                num_sessions=1,
                headless=True
            )
            return api
        except Exception as e:
            logger.error(f"Failed to initialize TikTokApi: {e}")
            return None

    def _get_cached_token(self, token_name):
        if token_name in self._token_cache:
            token_data = self._token_cache[token_name]
            if time.time() - token_data["timestamp"] < TOKEN_CACHE_TTL:
                return token_data["value"]
        
        token_value = globals()[token_name]
        if token_value:
            self._token_cache[token_name] = {
                "value": token_value,
                "timestamp": time.time()
            }
        return token_value

    async def _make_request_with_retry(self, operation, *args, retries=0, **kwargs):
        if retries >= self.max_retries:
            raise Exception("Max retries reached for operation")
        
        try:
            return await operation(*args, **kwargs)
        except Exception as e:
            error_message = str(e).lower()
            
            if "401" in error_message or "unauthorized" in error_message:
                logger.error("Authentication failed. Please check your tokens.")
                self._clear_token_cache()
                raise
            
            if "too many requests" in error_message or "429" in error_message:
                wait_time = int(self.retry_delay * (2 ** retries))
                logger.warning(f"Rate limit hit. Waiting {wait_time} seconds")
                await asyncio.sleep(wait_time)
                return await self._make_request_with_retry(operation, *args, retries=retries+1, **kwargs)
            
            elif "503" in error_message:  # Service unavailable
                wait_time = self.retry_delay * (2 ** retries) + random.uniform(0, 1)
                logger.warning(f"Service unavailable. Waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)
                return await self._make_request_with_retry(operation, *args, retries=retries+1, **kwargs)
            
            elif "timeout" in error_message:
                wait_time = self.retry_delay * (2 ** retries) + random.uniform(1, 5)
                logger.warning(f"Timeout error. Waiting {wait_time:.2f} seconds and retrying...")
                await asyncio.sleep(wait_time)
                return await self._make_request_with_retry(operation, *args, retries=retries+1, **kwargs)
            
            else:
                logger.error(f"Request failed: {e}")
                raise

    def _clear_token_cache(self):
        self._token_cache.clear()
        logger.info("Token cache cleared")

    async def close_api(self):
        if self.api_instance:
            try:
                logger.info("Closing TikTokApi instance")
                await self.api_instance.close_sessions()
                self.api_instance = None
            except Exception as e:
                logger.error(f"Error closing TikTokApi: {e}")

    @log_api_call
    async def get_user_info(self, username, full_url=None):
        if full_url:
            extracted_username = self.extract_tiktok_username(full_url)
            if extracted_username:
                username = extracted_username
                
        logger.info(f"Getting info for user: {username}")
        
        if not self.api_instance:
            self.api_instance = await self.initialize_api()
        if not self.api_instance:
            logger.error(f"API not initialized, cannot get user info for {username}")
            return {"user_info": {}}
        
        try:
            user = await self._make_request_with_retry(
                self.api_instance.user.info, username
            )
            
            if not user:
                logger.error(f"No user data returned for {username}")
                raise Exception(f"No data returned for user {username}")
            
            user_stats = user.get("userInfo", {}).get("stats", {})
            user_info = user.get("userInfo", {})
            
            return {
                "user_info": {
                    "id": user_info.get("user", {}).get("id"),
                    "nickname": user_info.get("user", {}).get("nickname"),
                    "signature": user_info.get("user", {}).get("signature"),
                    "follower_count": user_stats.get("followerCount"),
                    "following_count": user_stats.get("followingCount"),
                    "heart_count": user_stats.get("heartCount"),
                    "video_count": user_stats.get("videoCount")
                }
            }
        except Exception as e:
            logger.error(f"Error getting user info for {username}: {e}")
            return {"user_info": {}}

    @log_api_call
    async def get_user_videos(self, username, cursor=None, count=20):
        logger.info(f"Getting videos for user: {username}")
        
        if not self.api_instance:
            self.api_instance = await self.initialize_api()
            
        if not self.api_instance:
            logger.error(f"API not initialized, cannot get videos for {username}")
            return []
        
        try:
            user_videos = await self._make_request_with_retry(
                self.api_instance.user.videos, username, count=count
            )
            
            if not user_videos:
                logger.error(f"No videos returned for {username}")
                return []
            
            videos = []
            for video in user_videos:
                stats = video.get("stats", {})
                create_time = int(video.get("createTime", 0)) 
                
                videos.append({
                    "id": video.get("id"),
                    "desc": video.get("desc", ""),
                    "create_time": create_time,
                    "statistics": {
                        "like_count": stats.get("diggCount", 0),
                        "comment_count": stats.get("commentCount", 0),
                        "view_count": stats.get("playCount", 0),
                        "share_count": stats.get("shareCount", 0)
                    }
                })
                
                if len(videos) >= count:
                    break
            
            return videos
        except Exception as e:
            logger.error(f"Error getting videos for user {username}: {e}")
            return []

    def extract_tiktok_username(self, tiktok_url):
        try:
            parts = tiktok_url.split('@')
            if len(parts) > 1:
                username = parts[1].split('?')[0].split('/')[0]
                return username
            return None
        except:
            return None
    
    def get_user_info_sync(self, username, full_url=None):
        return self._loop.run_until_complete(self.get_user_info(username, full_url))
    
    def get_user_videos_sync(self, username, cursor=None, count=20):
        return self._loop.run_until_complete(self.get_user_videos(username, cursor, count)) 