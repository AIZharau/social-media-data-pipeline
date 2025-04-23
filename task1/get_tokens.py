#!/usr/bin/env python
import sys
import asyncio
from src.token_extractor import main_async

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python get_tokens.py username password [--headless]")
        sys.exit(1)
    
    username = sys.argv[1]
    password = sys.argv[2]
    headless = "--headless" in sys.argv    
    success = asyncio.run(main_async(username, password, headless))
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1) 