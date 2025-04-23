import os
import time
import random
import logging
import asyncio
import nest_asyncio
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
from TikTokApi import TikTokApi

load_dotenv()
nest_asyncio.apply()

logger = logging.getLogger(__name__)

class TikTokAPIClient:
    def __init__(self):
        self.max_retries = 5
        self.retry_delay = 2
        self.timeout = 60.0
        self.api_instance = None
        self._loop = asyncio.get_event_loop()
        
        try:
            self.api_instance = self._loop.run_until_complete(self.initialize_api())
            if not self.api_instance:
                logger.warning("Failed to initialize TikTokApi. Some TikTok API operations will fail.")
        except Exception as e:
            logger.error(f"Error initializing TikTokApi: {e}")
    
    async def initialize_api(self):
        logger.info("Initializing TikTokApi instance")
        ms_token = os.getenv("MS_TOKEN")
        verify_fp = os.getenv("TIKTOK_VERIFY_FP")
        session_id = os.getenv("TIKTOK_SESSIONID")
        
        if not any([ms_token, verify_fp, session_id]):
            logger.warning("No authentication tokens found in .env file. Please run get_tokens.py to obtain them.")
        
        if ms_token:
            logger.info("Using MS_TOKEN for authentication")
        if verify_fp:
            logger.info("Using TIKTOK_VERIFY_FP for authentication")
        if session_id:
            logger.info("Using TIKTOK_SESSIONID for authentication")
            
        try:
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

    async def close_api(self):
        if self.api_instance:
            try:
                logger.info("Closing TikTokApi instance")
                await self.api_instance.close_sessions()
                self.api_instance = None
            except Exception as e:
                logger.error(f"Error closing TikTokApi: {e}")

    async def _make_request_with_retry(self, operation, *args, retries=0, **kwargs):
        if retries >= self.max_retries:
            raise Exception(f"Max retries reached for operation")
        
        try:
            return await operation(*args, **kwargs)
        except Exception as e:
            error_message = str(e).lower()
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

    async def get_user_videos(self, user_id, cursor=None, count=20, full_url=None):
        username = user_id  
        
        if full_url:
            extracted_username = self.extract_tiktok_username(full_url)
            if extracted_username:
                username = extracted_username
                
        logger.info(f"Getting videos for user: {username}")
        
        if not self.api_instance:
            self.api_instance = await self.initialize_api()
            
        if not self.api_instance:
            logger.error(f"API not initialized, cannot get videos for {username}")
            return {"videos": []}
        
        try:
            user_videos = await self._make_request_with_retry(
                self.api_instance.user.videos, username, count=count
            )
            
            if not user_videos:
                logger.error(f"No videos returned for {username}")
                return {"videos": []}
            
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
            
            return {"videos": videos}
        except Exception as e:
            logger.error(f"Error getting videos for user {username}: {e}")
            return {"videos": []}

    async def get_video_stats(self, video_id):
        """Get stats for a specific video"""
        logger.info(f"Getting stats for video ID: {video_id}")
        
        if not self.api_instance:
            self.api_instance = await self.initialize_api()
            
        if not self.api_instance:
            logger.error(f"API not initialized, cannot get stats for video {video_id}")
            return {"stats": {}}
        
        try:
            video_data = await self._make_request_with_retry(
                self.api_instance.video.info, video_id
            )
            
            if not video_data:
                logger.error(f"No data returned for video {video_id}")
                raise Exception(f"No data returned for video {video_id}")
            
            stats = video_data.get("itemInfo", {}).get("itemStruct", {}).get("stats", {})
            
            return {
                "stats": {
                    "like_count": stats.get("diggCount", 0),
                    "comment_count": stats.get("commentCount", 0),
                    "view_count": stats.get("playCount", 0),
                    "share_count": stats.get("shareCount", 0)
                }
            }
        except Exception as e:
            logger.error(f"Error getting video stats for {video_id}: {e}")
            return {"stats": {}}

    async def get_multiple_users_data(self, username_urls, max_workers=2):
        if not self.api_instance:
            self.api_instance = await self.initialize_api()
            
        if not self.api_instance:
            logger.error("API not initialized, cannot get multiple users data")
            return {}
        
        if isinstance(username_urls, dict):
            items = list(username_urls.items())
        else:
            items = [(username, None) for username in username_urls]
        
        async def process_user(username, url):
            try:
                if url:
                    extracted_username = self.extract_tiktok_username(url)
                    if extracted_username:
                        username = extracted_username
                
                result = await self.get_user_info(username)
                
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                return (username, result)
            except Exception as e:
                logger.error(f"Error fetching data for {username}: {e}")
                return (username, {"error": str(e)})
        users_data = {}
        for i in range(0, len(items), max_workers):
            batch = items[i:i + max_workers]
            tasks = [process_user(username, url) for username, url in batch]
            batch_results = await asyncio.gather(*tasks)
            
            for username, data in batch_results:
                users_data[username] = data
            if i + max_workers < len(items):
                await asyncio.sleep(2 + random.uniform(1, 3))
        
        return users_data

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
    
    def get_user_videos_sync(self, user_id, cursor=None, count=20, full_url=None):
        return self._loop.run_until_complete(self.get_user_videos(user_id, cursor, count, full_url))
        
    def get_video_stats_sync(self, video_id):
        return self._loop.run_until_complete(self.get_video_stats(video_id))
        
    def get_multiple_users_data_sync(self, username_urls, max_workers=2):
        return self._loop.run_until_complete(self.get_multiple_users_data(username_urls, max_workers)) 