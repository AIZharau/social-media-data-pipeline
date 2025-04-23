#!/usr/bin/env python

import os
import sys
import logging
import asyncio
import argparse
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("token_extractor")

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')

async def get_tiktok_tokens(username, password, headless=False):
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            await page.goto('https://www.tiktok.com/login')
            logger.info("Open login page")
            
            await page.wait_for_load_state('networkidle')
            
            try:
                login_method = await page.query_selector('text=Use phone / email / username')
                if login_method:
                    await login_method.click()
                    await page.wait_for_timeout(1000)
                
                login_with_email = await page.query_selector('text=Email/Username')
                if login_with_email:
                    await login_with_email.click()
                    await page.wait_for_timeout(1000)
            except Exception as e:
                logger.info(f"Switching to email login is not required: {e}")
            
            await page.fill('input[name="username"]', username)
            await page.fill('input[type="password"]', password)
            
            login_button = await page.query_selector('button[type="submit"]')
            await login_button.click()
            
            logger.info("Waiting for authorization to complete...")
            try:
                await page.wait_for_url('https://www.tiktok.com/', timeout=5000)
            except:
                logger.warning("It may require entering a captcha or additional verification")
                logger.info("Please complete the authorization manually in the window that opens")
                
                auth_completed = False
                for _ in range(24):
                    current_url = page.url
                    if 'tiktok.com' in current_url and '/login' not in current_url:
                        auth_completed = True
                        break
                    await page.wait_for_timeout(5000)
                
                if not auth_completed:
                    logger.error("Authorization timeout exceeded")
                    await browser.close()
                    return None
            
            logger.info("Authorization successful, we receive tokens...")
            cookies = await context.cookies()
            
            tokens = {}
            cookie_mapping = {
                'msToken': 'MS_TOKEN',
                's_v_web_id': 'TIKTOK_VERIFY_FP',
                'sessionid': 'TIKTOK_SESSIONID'
            }
            
            for cookie in cookies:
                name = cookie.get('name')
                if name in cookie_mapping:
                    env_name = cookie_mapping[name]
                    tokens[env_name] = cookie.get('value')
            
            logger.info(f"Received {len(tokens)} tokens")
            
            await page.screenshot(path='tiktok_auth_screenshot.png')
            
            await browser.close()
            return tokens
        
        except Exception as e:
            logger.error(f"Error while getting tokens: {e}")
            await page.screenshot(path='tiktok_auth_error.png')
            await browser.close()
            return None

def update_env_file(tokens):
    if not tokens:
        return False
    
    logger.info(f"Updating .env: {ENV_PATH}")
    
    load_dotenv(ENV_PATH)
    
    for key, value in tokens.items():
        set_key(ENV_PATH, key, value)
        logger.info(f"{key} token has already updated")
    
    return True

async def main_async(username, password, headless=False):
    tokens = await get_tiktok_tokens(username, password, headless)
    if tokens:
        update_env_file(tokens)
        logger.info("Tokens successfully received and saved in .env")
        return True
    else:
        logger.error("Failed to get tokens")
        return False

def main():
    parser = argparse.ArgumentParser(description="TikTok Token Extractor")
    parser.add_argument("username", help="Username or email TikTok")
    parser.add_argument("password", help="Password TikTok")
    parser.add_argument("--headless", action="store_true", help="Run in background without UI")
    
    args = parser.parse_args()
    
    success = asyncio.run(main_async(args.username, args.password, args.headless))
    
    if success:
        return 0
    return 1

if __name__ == "__main__":
    sys.exit(main()) 