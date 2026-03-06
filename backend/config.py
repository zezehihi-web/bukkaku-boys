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
ATBB_LOGIN_ID = os.getenv("ATBB_LOGIN_ID", "")
ATBB_PASSWORD = os.getenv("ATBB_PASSWORD", "")

# イタンジBB認証
ITANJI_EMAIL = os.getenv("ITANJI_EMAIL", "")
ITANJI_PASSWORD = os.getenv("ITANJI_PASSWORD", "")
ITANJI_TOP_URL = "https://bukkakun.com/"

# いい生活スクエア認証
ES_SQUARE_EMAIL = os.getenv("ES_SQUARE_EMAIL", "")
ES_SQUARE_PASSWORD = os.getenv("ES_SQUARE_PASSWORD", "")
ES_SQUARE_LOGIN_URL = "https://rent.es-square.net/login"

# GoWeb認証（100kadou.net系）
GOWEB_LOGIN_URL = os.getenv("GOWEB_LOGIN_URL", "https://ab.100kadou.net/accounts/login")
GOWEB_USER_ID = os.getenv("GOWEB_USER_ID", "")
GOWEB_PASSWORD = os.getenv("GOWEB_PASSWORD", "")

# いえらぶBB認証
IERABU_BB_LOGIN_URL = "https://bb.ielove.jp/ielovebb/login/index/"
IERABU_BB_ID = os.getenv("IERABU_EMAIL", "")
IERABU_BB_PASSWORD = os.getenv("IERABU_PASSWORD", "")

# リアルネットプロ認証
REALPRO_URL = os.getenv("REALPRO_URL", "https://www.realnetpro.com/index.php")
REALPRO_ID = os.getenv("REALPRO_ID", "")
REALPRO_PASS = os.getenv("REALPRO_PASS", "")

# 物確.com / いい生活B2B はマルチテナント（管理会社ごとに認証情報が異なる）
# 認証情報は backend/credentials_map.py で管理し、
# 環境変数は {CREDENTIAL_KEY}_URL / {CREDENTIAL_KEY}_ID / {CREDENTIAL_KEY}_PASS の命名規則。
# 例: CIC_URL=https://cic.bukkaku.jp  CIC_ID=user  CIC_PASS=pass

# LINE通知
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_ID = os.getenv("LINE_USER_ID", "")

# Slack通知
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "")

# API設定
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
