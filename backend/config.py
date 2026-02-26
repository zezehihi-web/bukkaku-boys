"""アプリケーション設定・環境変数管理"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# パス設定
BASE_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = BASE_DIR / "results"
DB_PATH = BASE_DIR / "backend" / "akikaku.db"
ATBB_JSON_PATH = RESULTS_DIR / "properties_database_list.json"

# ATBB認証
ATBB_LOGIN_ID = os.getenv("ATBB_LOGIN_ID", "001089150164")
ATBB_PASSWORD = os.getenv("ATBB_PASSWORD", "zezehihi893")

# イタンジBB認証
ITANJI_EMAIL = os.getenv("ITANJI_EMAIL", "")
ITANJI_PASSWORD = os.getenv("ITANJI_PASSWORD", "")
ITANJI_TOP_URL = "https://bukkakun.com/"

# いえらぶBB認証
IERABU_EMAIL = os.getenv("IERABU_EMAIL", "")
IERABU_PASSWORD = os.getenv("IERABU_PASSWORD", "")
IERABU_LOGIN_URL = "https://bb.ielove.jp/ielovebb/login/index"

# いい生活スクエア認証
ES_SQUARE_EMAIL = os.getenv("ES_SQUARE_EMAIL", "")
ES_SQUARE_PASSWORD = os.getenv("ES_SQUARE_PASSWORD", "")
ES_SQUARE_LOGIN_URL = "https://rent.es-square.net/login"

# LINE通知
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_ID = os.getenv("LINE_USER_ID", "")

# Slack通知
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# API設定
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
