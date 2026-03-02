import os
import sys
import time
import json
import sqlite3
import requests
import hashlib
import signal
import io
import re
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path

# OCRライブラリのインポート（オプション）- 後で初期化
OCR_AVAILABLE = False
OCR_TYPE = None  # 'pytesseract' or 'easyocr'

# Windows環境での絵文字表示対応
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

print("=" * 50)
print("ATBB リストスクレイピングスクリプトを開始します")
print("=" * 50)

import random

try:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.support import expected_conditions as EC
    print("✅ Seleniumライブラリをインポートしました")
except ImportError as e:
    print(f"❌ ライブラリのインポートエラー: {e}")
    print("以下のコマンドでインストールしてください: pip install selenium")
    sys.exit(1)

# undetected-chromedriverを使用（ボット検出回避）
USE_UNDETECTED = True
try:
    import undetected_chromedriver as uc
    print("✅ undetected-chromedriver を使用します（ボット対策）")
except ImportError:
    USE_UNDETECTED = False
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    print("ℹ️ 通常のSeleniumを使用します（pip install undetected-chromedriver でボット対策可能）")

# OCRライブラリの初期化
try:
    import easyocr
    OCR_AVAILABLE = True
    OCR_TYPE = 'easyocr'
    print("✅ OCRライブラリが利用可能です（easyocr）- 初期化中...")
    OCR_READER = easyocr.Reader(['ja', 'en'], gpu=False, verbose=False)
    print("✅ easyocr 初期化完了")
except ImportError:
    try:
        from PIL import Image
        import pytesseract
        pytesseract.get_tesseract_version()
        OCR_AVAILABLE = True
        OCR_TYPE = 'pytesseract'
        OCR_READER = None
        print("✅ OCRライブラリが利用可能です（pytesseract）")
    except Exception as e:
        print(f"ℹ️ OCRライブラリが見つかりません: {e}")
        print("   インストール方法: pip install easyocr")
        OCR_READER = None

# ========= 設定 =========
from dotenv import load_dotenv
load_dotenv()

LOGIN_ID = os.environ.get("ATBB_LOGIN_ID", "")
PASSWORD = os.environ.get("ATBB_PASSWORD", "")

TARGET_URL = "https://atbb.athome.co.jp/front-web/mainservlet/bfcm003s201"

# テストモード: Trueにすると最初の10件のみ処理（動作確認用）
TEST_MODE = False
TEST_LIMIT = 10

SHIKUGUN_BATCH_SIZE = 3  # 3市区町村ずつ検索（5000件上限対策）

# 対象の都道府県 (ID, 県名)
TARGET_PREFECTURES = [
    ("13", "東京都"),
    ("11", "埼玉県"),
    ("12", "千葉県"),
    ("14", "神奈川県")
]

# 結果ファイルパス（SQLite + JSON互換）
RESULTS_DIR = "results"
JSON_FILEPATH = os.path.join(RESULTS_DIR, "properties_database_list.json")
DB_PATH = str(Path(__file__).resolve().parent / "backend" / "akikaku.db")

# ========= Chrome設定 =========
print("🔧 Chrome設定を開始します...")

def human_delay(min_sec=0.1, max_sec=0.3):
    """人間らしいランダムな待機時間（高速化版）"""
    time.sleep(random.uniform(min_sec, max_sec))

if USE_UNDETECTED:
    print("  → undetected-chromedriver でブラウザを起動中...")
    chrome_options = uc.ChromeOptions()
    chrome_options.page_load_strategy = 'eager'  # DOM Ready即返り（画像待ちしない）
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = uc.Chrome(options=chrome_options, use_subprocess=True, version_main=145)
else:
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'eager'  # DOM Ready即返り（画像待ちしない）
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    print("  → ChromeDriverManagerをインストール中...")
    service = Service(ChromeDriverManager().install())
    print("  → Chromeブラウザを起動中...")
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        '''
    })

wait = WebDriverWait(driver, 30)
print("✅ Chromeブラウザの起動が完了しました")

# グローバル変数
interrupted = False
all_properties = []
deferred_ocr_items = []  # OCR延期リスト: [{property_key, img_url, name}, ...]
# バックグラウンド画像ダウンローダー（スキャン中に裏でDL開始）
_bg_executor = ThreadPoolExecutor(max_workers=8)
_bg_futures = {}  # property_key -> Future[bytes|None]

def _download_single(url):
    """バックグラウンドで1画像をダウンロード"""
    try:
        resp = requests.get(url, timeout=10)
        return resp.content if resp.status_code == 200 else None
    except Exception:
        return None

def signal_handler(sig, frame):
    global interrupted
    print("\n\n⚠️ 中断シグナルを受信しました。安全に終了します...")
    interrupted = True
    save_data_to_files()
    if driver:
        try:
            driver.quit()
        except:
            pass
    sys.exit(0)

if sys.platform == 'win32':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
else:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def wait_and_accept_alert():
    try:
        WebDriverWait(driver, 2).until(EC.alert_is_present())
        driver.switch_to.alert.accept()
        return True
    except:
        return False


def is_driver_alive(drv):
    """WebDriverが生きているか確認"""
    try:
        _ = drv.current_url
        return True
    except Exception:
        return False


def recreate_driver():
    """WebDriverを再作成する（接続タイムアウト時の復旧用）"""
    global driver, wait
    print("   🔄 WebDriverを再作成します...")
    try:
        driver.quit()
    except Exception:
        pass
    time.sleep(3)

    if USE_UNDETECTED:
        chrome_options = uc.ChromeOptions()
        chrome_options.page_load_strategy = 'eager'
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        driver = uc.Chrome(options=chrome_options, use_subprocess=True, version_main=145)
    else:
        options = webdriver.ChromeOptions()
        options.page_load_strategy = 'eager'
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': 'Object.defineProperty(navigator, "webdriver", { get: () => undefined })'
        })

    wait = WebDriverWait(driver, 30)
    print("   ✅ WebDriver再作成完了")
    return driver


def relogin_and_navigate_to_search(drv, area_id, prefecture_name):
    """再ログインして検索結果ページまで復帰する"""
    print(f"   🔄 再ログイン → {prefecture_name}の検索結果ページまで復帰中...")
    drv.get("https://members.athome.jp/portal")
    wait_for_page_ready(drv, timeout=15)

    try:
        login_field = WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.ID, "loginFormText"))
        )
        login_field.send_keys(LOGIN_ID)
        pass_field = drv.find_element(By.ID, "passFormText")
        pass_field.send_keys(PASSWORD)
        submit = WebDriverWait(drv, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='submit']"))
        )
        drv.execute_script("arguments[0].click();", submit)
        time.sleep(3)
        wait_for_page_ready(drv, timeout=15)
    except Exception as e:
        print(f"   ⚠️ 再ログイン失敗: {e}")
        return False

    # 同時ログインエラー対応
    if "ConcurrentLoginException.jsp" in drv.current_url:
        try:
            force_btn = WebDriverWait(drv, 5).until(EC.element_to_be_clickable(
                (By.XPATH, "//input[@type='button' and contains(@value,'強制終了させてATBBを利用する')]")
            ))
            drv.execute_script("arguments[0].click();", force_btn)
            wait_and_accept_alert()
            time.sleep(3)
        except Exception:
            pass

    print(f"   ✅ 再ログイン完了")
    return True

def wait_for_page_ready(drv, timeout=10, max_retries=2):
    """ページ読み込み完了を待機（スタック対策付き）

    Chromeタブがクルクル回り続けてreadyStateがcompleteにならない場合、
    timeout秒後に自動リロードしてリトライする。
    """
    for attempt in range(max_retries + 1):
        try:
            WebDriverWait(drv, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            return True  # 読み込み完了
        except Exception:
            if attempt < max_retries:
                print(f"   ⚠️ ページ読み込みが{timeout}秒以上かかっています → リロード試行 ({attempt+1}/{max_retries})")
                try:
                    drv.refresh()
                    time.sleep(2)
                except Exception:
                    pass
            else:
                print(f"   ⚠️ ページ読み込みタイムアウト（{max_retries}回リロード後も完了せず）→ 続行")
                return False


def wait_for_cards_ready(drv, timeout=10):
    """物件カードの出現を待機（画像読み込みは待たない・超高速版）"""
    try:
        WebDriverWait(drv, timeout).until(
            lambda d: d.execute_script(
                "return document.readyState !== 'loading' && "
                "document.querySelectorAll('.property_card').length > 0"
            )
        )
        return True
    except Exception:
        return False


def check_and_wait_for_captcha():
    try:
        captcha_selectors = [
            "iframe[src*='recaptcha']",
            "iframe[title*='reCAPTCHA']",
            ".g-recaptcha",
            "#recaptcha",
            "iframe[src*='google.com/recaptcha']"
        ]
        captcha_found = False
        for selector in captcha_selectors:
            try:
                elem = driver.find_element(By.CSS_SELECTOR, selector)
                if elem.is_displayed():
                    captcha_found = True
                    break
            except:
                continue
        if captcha_found:
            print("\n" + "="*50)
            print("⚠️ reCAPTCHA が検出されました！")
            print("   自動で30秒待機します（手動解決をお待ちしています）...")
            # 全自動モード: input()は使わず、待機のみ
            time.sleep(30)
            human_delay(0.5, 1.0)
            return True
    except:
        pass
    return False

# ============================================================================
# 差分更新（インクリメンタル）機能 — SQLite版
# ============================================================================
def make_property_key(prop):
    """物件の一意キーを生成（名前+号室+所在地）"""
    name = prop.get('名前', '')
    room = prop.get('号室', '')
    addr = prop.get('所在地', '')
    return f"{name}|{room}|{addr}"


def _get_db():
    """SQLite接続を取得（同期版）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def upsert_properties_to_db(properties, prefecture):
    """スクレイプ結果をSQLiteにupsert（同期版）

    - 既存物件: last_seen + フィールド更新
    - 新規物件: INSERT
    - 戻り値: 今回upsertしたproperty_keyのセット
    """
    now = datetime.now().isoformat()
    inserted = 0
    updated = 0
    current_keys = set()

    conn = _get_db()
    try:
        cursor = conn.cursor()
        for prop in properties:
            key = make_property_key(prop)
            if not key or key == '||':
                continue
            current_keys.add(key)

            # 既存レコード確認
            cursor.execute(
                "SELECT id FROM atbb_properties WHERE property_key = ?",
                (key,)
            )
            existing = cursor.fetchone()

            if existing:
                # UPDATE: last_seen更新 + フィールド上書き
                cursor.execute("""
                    UPDATE atbb_properties SET
                        rent=?, management_fee=?, deposit=?, key_money=?,
                        layout=?, area=?, floors=?, address=?,
                        build_year=?, transport=?, structure=?,
                        transaction_type=?, management_company=?,
                        publish_date=?, property_id=?, prefecture=?,
                        status='募集中', last_seen=?, updated_at=?
                    WHERE property_key = ?
                """, (
                    prop.get('賃料', ''), prop.get('管理費等', ''),
                    prop.get('敷金', ''), prop.get('礼金', ''),
                    prop.get('間取り', ''), prop.get('専有面積', ''),
                    prop.get('階建/階', ''), prop.get('所在地', ''),
                    prop.get('築年月', ''), prop.get('交通', ''),
                    prop.get('建物構造', ''), prop.get('取引態様', ''),
                    prop.get('管理会社情報', ''), prop.get('公開日', ''),
                    prop.get('物件番号', ''), prefecture,
                    now, now, key,
                ))
                updated += 1
            else:
                # 新規物件 → そのままINSERT
                cursor.execute("""
                    INSERT INTO atbb_properties (
                        property_key, name, room_number,
                        rent, management_fee, deposit, key_money,
                        layout, area, floors, address,
                        build_year, transport, structure,
                        transaction_type, management_company,
                        publish_date, property_id, prefecture,
                        status, first_seen, last_seen
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    key, prop.get('名前', ''), prop.get('号室', ''),
                    prop.get('賃料', ''), prop.get('管理費等', ''),
                    prop.get('敷金', ''), prop.get('礼金', ''),
                    prop.get('間取り', ''), prop.get('専有面積', ''),
                    prop.get('階建/階', ''), prop.get('所在地', ''),
                    prop.get('築年月', ''), prop.get('交通', ''),
                    prop.get('建物構造', ''), prop.get('取引態様', ''),
                    prop.get('管理会社情報', ''), prop.get('公開日', ''),
                    prop.get('物件番号', ''), prefecture,
                    '募集中', now, now,
                ))
                inserted += 1

        conn.commit()
    finally:
        conn.close()

    print(f"   📊 DB更新: 新規{inserted} / 更新{updated}")
    return current_keys


def mark_disappeared_properties(prefecture, current_keys):
    """今回のスクレイプで見つからなかった物件を募集終了にマーク

    ※ 物件はDBから削除せず、statusを'募集終了'に変更するだけ
    """
    now = datetime.now().isoformat()
    conn = _get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT property_key FROM atbb_properties
            WHERE prefecture = ? AND status = '募集中'
        """, (prefecture,))
        all_keys = {row['property_key'] for row in cursor.fetchall()}

        disappeared = all_keys - current_keys
        if disappeared:
            placeholders = ','.join('?' * len(disappeared))
            cursor.execute(f"""
                UPDATE atbb_properties SET status = '募集終了', updated_at = ?
                WHERE property_key IN ({placeholders})
            """, [now] + list(disappeared))
            conn.commit()
            print(f"   📋 {prefecture}: {len(disappeared)}件を募集終了にマーク")
        else:
            print(f"   📋 {prefecture}: 募集終了の物件なし")
    finally:
        conn.close()


def get_db_count(prefecture=None):
    """DB内のレコード数を取得"""
    conn = _get_db()
    try:
        if prefecture:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM atbb_properties WHERE prefecture = ? AND status = '募集中'",
                (prefecture,)
            )
        else:
            cursor = conn.execute("SELECT COUNT(*) FROM atbb_properties WHERE status = '募集中'")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def load_existing_data():
    """既存データの件数を確認（互換性のため残す）"""
    count = get_db_count()
    if count > 0:
        print(f"📂 既存DBデータ: {count}件（募集中）")
    return count


def save_data_to_files():
    """中間保存 — SQLite版ではページごとにupsert済みのため不要
    （互換性のため空関数として残す）
    """
    pass


def batch_ocr_rent_images():
    """全ページスキャン完了後に賃料画像を一括でOCR処理

    1. 画像URLを並列ダウンロード（ThreadPoolExecutor, 10並列）
    2. EasyOCRで順次処理
    3. DB更新
    """
    global deferred_ocr_items

    if not deferred_ocr_items:
        print("\n📋 OCR処理対象の賃料画像なし")
        return

    if not OCR_AVAILABLE or OCR_READER is None:
        print(f"\n⚠️ OCRライブラリ未使用（{len(deferred_ocr_items)}件の賃料画像をスキップ）")
        return

    # 重複除去（同じproperty_keyの画像は1回だけ）
    seen_keys = set()
    unique_items = []
    for item in deferred_ocr_items:
        if item['property_key'] not in seen_keys:
            seen_keys.add(item['property_key'])
            unique_items.append(item)
    deferred_ocr_items = unique_items

    print(f"\n{'='*50}")
    print(f"🔍 賃料OCR一括処理を開始（{len(deferred_ocr_items)}件）")
    print(f"{'='*50}")

    # --- Phase 1: バックグラウンドDL済み画像を回収 + 未DL分を追加DL ---
    print(f"   📥 画像回収中（スキャン中にバックグラウンドDL済み）...")
    download_start = time.time()

    downloaded = []
    extra_downloads = []  # バックグラウンドDLされなかった分
    for item in deferred_ocr_items:
        key = item['property_key']
        future = _bg_futures.get(key)
        if future:
            try:
                img_data = future.result(timeout=30)
                if img_data:
                    downloaded.append({
                        'property_key': key,
                        'image_data': img_data,
                        'name': item['name'],
                    })
            except Exception:
                pass
        else:
            extra_downloads.append(item)

    # 未DL分があれば追加ダウンロード
    if extra_downloads:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures_extra = {}
            for item in extra_downloads:
                futures_extra[executor.submit(_download_single, item['img_url'])] = item
            for future in as_completed(futures_extra):
                item = futures_extra[future]
                try:
                    img_data = future.result()
                    if img_data:
                        downloaded.append({
                            'property_key': item['property_key'],
                            'image_data': img_data,
                            'name': item['name'],
                        })
                except Exception:
                    pass

    dl_elapsed = time.time() - download_start
    print(f"   ✅ 画像回収完了: {len(downloaded)}/{len(deferred_ocr_items)}件 ({dl_elapsed:.1f}秒)")

    if not downloaded:
        print(f"   ⚠️ ダウンロードできた画像なし → OCR中止")
        return

    # --- Phase 2: OCR一括処理 + DB更新 ---
    print(f"   🔍 OCR処理中...")
    ocr_start = time.time()
    ocr_success = 0
    conn = _get_db()
    now = datetime.now().isoformat()

    try:
        cursor = conn.cursor()
        for i, item in enumerate(downloaded):
            try:
                results = OCR_READER.readtext(item['image_data'])
                rent_text = ''
                for result in results:
                    text = result[1]
                    price_match = re.search(r'([\d,\.]+)\s*万?円?', text)
                    if price_match:
                        rent_text = price_match.group(0).strip()
                        if '万' not in rent_text and '円' not in rent_text:
                            rent_text += '万円'
                        break

                if rent_text:
                    rent_normalized = normalize_rent(rent_text)
                    if rent_normalized:
                        cursor.execute("""
                            UPDATE atbb_properties SET rent = ?, updated_at = ?
                            WHERE property_key = ?
                        """, (rent_normalized, now, item['property_key']))
                        ocr_success += 1

                # 進捗表示（50件ごと）
                if (i + 1) % 50 == 0:
                    conn.commit()
                    elapsed = time.time() - ocr_start
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    remaining = (len(downloaded) - i - 1) / rate if rate > 0 else 0
                    print(f"      ... {i+1}/{len(downloaded)}件処理済み "
                          f"({ocr_success}件成功, 残り約{remaining:.0f}秒)")

            except Exception:
                pass

        conn.commit()
    finally:
        conn.close()

    ocr_elapsed = time.time() - ocr_start
    print(f"   ✅ OCR完了: {ocr_success}/{len(downloaded)}件の賃料を取得 ({ocr_elapsed:.1f}秒)")
    print(f"   ⏱️ 合計: ダウンロード{dl_elapsed:.1f}秒 + OCR{ocr_elapsed:.1f}秒")

# ============================================================================
# 賃料テキストの正規化
# ============================================================================
def normalize_rent(rent_text):
    """賃料テキストを正規化して円単位に変換"""
    if not rent_text or rent_text == '要確認':
        return ''
    # スクリプト混入チェック
    if 'Image(' in rent_text or 'function' in rent_text:
        return ''
    m = re.search(r'([\d,\.]+)\s*万円', rent_text)
    if m:
        try:
            return f"{int(float(m.group(1).replace(',', '')) * 10000)}円"
        except:
            return rent_text
    if re.search(r'[\d,]+円', rent_text):
        return rent_text.replace(',', '')
    # 数値のみの場合
    m = re.search(r'[\d,\.]+', rent_text)
    if m:
        return rent_text
    return ''


# ============================================================================
# 画像（賃料）からテキストを抽出・解読するロジック
# ============================================================================
def extract_rent_from_image(img_element):
    """賃料画像からテキストを抽出（alt → CDNダウンロード+OCR → 要素スクリーンショット+OCR）"""
    rent_text = ''
    try:
        rent_text = img_element.get_attribute("alt") or img_element.get_attribute("title") or ''

        if not rent_text and OCR_AVAILABLE and OCR_READER is not None:
            img_src = img_element.get_attribute("src")
            if img_src:
                # 方法1: CDN URLから直接ダウンロード
                try:
                    img_response = requests.get(img_src, timeout=5)
                    if img_response.status_code == 200:
                        results = OCR_READER.readtext(img_response.content)
                        for result in results:
                            text = result[1]
                            price_match = re.search(r'([\d,\.]+)\s*万?円?', text)
                            if price_match:
                                rent_text = price_match.group(0).strip()
                                if '万' not in rent_text and '円' not in rent_text:
                                    rent_text += '万円'
                                break
                except Exception:
                    pass

            # 方法2: Selenium要素スクリーンショットでOCR
            if not rent_text:
                try:
                    img_png = img_element.screenshot_as_png
                    if img_png:
                        results = OCR_READER.readtext(img_png)
                        for result in results:
                            text = result[1]
                            price_match = re.search(r'([\d,\.]+)\s*万?円?', text)
                            if price_match:
                                rent_text = price_match.group(0).strip()
                                if '万' not in rent_text and '円' not in rent_text:
                                    rent_text += '万円'
                                break
                except Exception:
                    pass
    except Exception:
        pass

    # 万円等の正規化
    if rent_text and '万円' in rent_text:
        try:
            num = float(re.sub(r'[^\d\.]', '', rent_text.replace('万円', '')))
            rent_text = f"{int(num * 10000)}円"
        except:
            pass
    elif '円' in rent_text and ',' in rent_text:
        rent_text = rent_text.replace(',', '')

    return rent_text if rent_text else '要確認'

# ============================================================================
# 詳細ページアクセスによるデータ品質改善（フェーズ0）
# 一覧ページではマスクされている物件名(AT)・住所(▲)・賃料(画像)を
# 詳細ページにアクセスして正式な情報を取得する
# ============================================================================
ENRICH_DETAILS = False  # 詳細ページ補完はスキップ（高速化: リスト一覧のみで十分）

# エラー統計カウンタ
enrich_stats = {
    'total': 0,
    'success': 0,
    'name_found': 0,
    'addr_found': 0,
    'rent_found': 0,
    'company_found': 0,
    'btn_not_found': 0,
    'page_error': 0,
    'first_error_saved': False,
}

def find_value_by_label(drv, label):
    """汎用ラベル検索: 複数のHTML構造パターンを順に試して値テキストを返す

    対応パターン:
      - td.common-head / td.common-data （ATBBの標準テーブル）
      - th / td
      - dt / dd
      - label / span
      - 任意の要素 / following-sibling
    """
    patterns = [
        # パターン1: td.common-head + td.common-data (ATBB標準)
        (By.XPATH,
         f"//td[contains(@class, 'common-head') and contains(text(), '{label}')]"
         f"/following-sibling::td[contains(@class, 'common-data')]"),
        # パターン2: td + following-sibling td (クラスなし)
        (By.XPATH,
         f"//td[contains(text(), '{label}')]/following-sibling::td[1]"),
        # パターン3: th + td
        (By.XPATH,
         f"//th[contains(text(), '{label}')]/following-sibling::td[1]"),
        # パターン4: dt + dd
        (By.XPATH,
         f"//dt[contains(text(), '{label}')]/following-sibling::dd[1]"),
        # パターン5: label + span/div
        (By.XPATH,
         f"//label[contains(text(), '{label}')]/following-sibling::*[1]"),
        # パターン6: 任意の要素 + following-sibling (最も広い)
        (By.XPATH,
         f"//*[contains(text(), '{label}')]/following-sibling::*[1]"),
    ]

    for by, selector in patterns:
        try:
            elem = drv.find_element(by, selector)
            text = elem.text.strip()
            if text:
                return text
        except:
            continue
    return ''


def enrich_property_from_detail(drv, wait_obj, prop_data, button_index=None, btn_id=None):
    """詳細ページにアクセスして正式な物件名・住所・賃料・管理会社を取得

    実際のATBB詳細ページDOM構造に基づくセレクタ:
      - 物件名: div.title-bar > p.name
      - 所在地: td.common-head[text()='所在地'] + td.common-data（span含む）
      - 賃料: td.common-data.payment 内の img alt/title → price_value_div → OCR
      - 管理会社: 登録会員セクション内 span.large.bold + TEL正規表現
      - 物件番号: span.bukkenno[data-bukkenno]
    """
    global enrich_stats
    enrich_stats['total'] += 1

    bukken_no = prop_data.get('物件番号', '')
    detail_tab_handle = None
    original_handle = drv.current_window_handle

    try:
        # =============================================
        # ボタン特定: ID方式を最優先（JS抽出で取得したbtnId）
        # =============================================
        detail_btn = None

        if btn_id:
            # 方法0: JS抽出時に取得したボタンID（最も確実）
            try:
                detail_btn = drv.find_element(By.ID, btn_id)
            except:
                pass

        if detail_btn is None and button_index is not None:
            # 方法1: インデックスで直接取得
            all_buttons = drv.find_elements(By.CSS_SELECTOR, "button[name='shosai'], button[id^='shosai']")
            if button_index < len(all_buttons):
                detail_btn = all_buttons[button_index]

        if detail_btn is None and bukken_no:
            # 方法2: onclick属性で特定
            try:
                detail_btn = drv.find_element(
                    By.CSS_SELECTOR, f"button[onclick*=\"'{bukken_no}'\"]"
                )
            except:
                pass

        if detail_btn is None and bukken_no:
            # 方法3: ID で特定
            try:
                detail_btn = drv.find_element(By.ID, f"shosai_{bukken_no}")
            except:
                pass

        if detail_btn is None:
            enrich_stats['btn_not_found'] += 1
            print(f"      ⚠️ 詳細ボタンが見つかりません (idx={button_index}, 物件番号={bukken_no})")
            return prop_data

        # =============================================
        # 新タブで詳細ページを開く（一覧ページを壊さない）
        # =============================================
        original_handles = set(drv.window_handles)

        # formのtargetを_blankに設定してからボタンクリック
        try:
            drv.execute_script("""
                var forms = document.querySelectorAll('form');
                for (var i = 0; i < forms.length; i++) {
                    forms[i].setAttribute('target', '_blank');
                }
            """)
            drv.execute_script("arguments[0].click();", detail_btn)
        except Exception:
            drv.execute_script("arguments[0].click();", detail_btn)

        wait_and_accept_alert()
        human_delay(1.0, 2.0)

        # 新タブが開いたか確認（最大5秒待つ）
        new_tab_found = False
        for _ in range(10):
            new_handles = set(drv.window_handles) - original_handles
            if new_handles:
                detail_tab_handle = new_handles.pop()
                drv.switch_to.window(detail_tab_handle)
                new_tab_found = True
                break
            time.sleep(0.5)

        if not new_tab_found:
            detail_tab_handle = None

        # formのtargetを元に戻す
        if detail_tab_handle:
            try:
                drv.switch_to.window(original_handle)
                drv.execute_script("""
                    var forms = document.querySelectorAll('form');
                    for (var i = 0; i < forms.length; i++) {
                        forms[i].removeAttribute('target');
                    }
                """)
                drv.switch_to.window(detail_tab_handle)
            except:
                drv.switch_to.window(detail_tab_handle)

        # 詳細ページの読み込み待ち
        try:
            WebDriverWait(drv, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except:
            pass
        human_delay(0.5, 1.0)

        # =============================================
        # 詳細ページに遷移したか確認（一覧ページのままなら中断）
        # =============================================
        is_detail_page = drv.execute_script("""
            // 詳細ページには div.title-bar があり、一覧ページには .property_card が複数ある
            var titleBar = document.querySelector('div.title-bar p.name, .title-bar .name');
            var cards = document.querySelectorAll('.property_card');
            return titleBar !== null || cards.length <= 1;
        """)
        if not is_detail_page:
            print(f"      ⚠️ 詳細ページへの遷移失敗（一覧ページのまま）- スキップ")
            enrich_stats['page_error'] += 1
            return prop_data

        # =============================================
        # JavaScript一括取得（1回のJS実行で全データ取得）
        # =============================================
        detail_data = drv.execute_script("""
            var result = {};

            // 物件名: div.title-bar > p.name
            var nameElem = document.querySelector('div.title-bar p.name, .title-bar .name');
            result.name = nameElem ? nameElem.textContent.trim() : '';

            // 所在地: td.common-head + td.common-data
            var heads = document.querySelectorAll('td.common-head');
            for (var i = 0; i < heads.length; i++) {
                var headText = heads[i].textContent.trim();

                if (headText === '所在地' && !result.addr) {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) {
                        var clone = dataCell.cloneNode(true);
                        // 地図ボタンやスクリプトを除去
                        var removes = clone.querySelectorAll('.map, script, button, [onclick*="Chizu"]');
                        for (var r = 0; r < removes.length; r++) removes[r].remove();
                        result.addr = clone.textContent.trim().replace(/\\s+/g, '');
                    }
                }

                // 管理費等
                if (headText.indexOf('管理費') >= 0 && !result.kanrihi) {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) result.kanrihi = dataCell.textContent.trim();
                }

                // 間取り
                if (headText === '間取り' && !result.madori) {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) result.madori = dataCell.textContent.trim();
                }

                // 交通
                if (headText === '交通' && !result.kotsu) {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) result.kotsu = dataCell.textContent.trim();
                }

                // 築年月
                if (headText === '築年月' && !result.chiku) {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) result.chiku = dataCell.textContent.trim();
                }

                // 建物構造
                if (headText === '建物構造' && !result.kouzou) {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) result.kouzou = dataCell.textContent.trim();
                }

                // 専有面積
                if (headText === '専有面積' && !result.menseki) {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) result.menseki = dataCell.textContent.trim();
                }

                // 階建/階
                if (headText === '階建/階' && !result.kai) {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) result.kai = dataCell.textContent.trim();
                }
            }

            // 賃料: price_value_div → price_txt_div → img alt/title
            result.rent = '';
            // 方法A: price_value_div のテキスト（JSで動的に設定された場合）
            var priceValueDivs = document.querySelectorAll('[id^="price_value_div"]');
            for (var i = 0; i < priceValueDivs.length; i++) {
                var t = priceValueDivs[i].textContent.trim();
                if (t) { result.rent = t; break; }
            }
            // 方法B: price_txt_div のテキスト
            if (!result.rent) {
                var priceTxtDivs = document.querySelectorAll('[id^="price_txt_div"]');
                for (var i = 0; i < priceTxtDivs.length; i++) {
                    var t = priceTxtDivs[i].textContent.trim();
                    if (t) { result.rent = t; break; }
                }
            }
            // 方法C: img[id^="price_img"] の alt/title
            if (!result.rent) {
                var priceImgs = document.querySelectorAll('img[id^="price_img"]');
                for (var i = 0; i < priceImgs.length; i++) {
                    var alt = priceImgs[i].alt || priceImgs[i].title || '';
                    if (alt) { result.rent = alt; break; }
                }
            }

            // 物件番号: span.bukkenno[data-bukkenno]
            var bukkenElem = document.querySelector('span.bukkenno[data-bukkenno], [data-bukkenno]');
            result.bukkenNo = bukkenElem ? bukkenElem.getAttribute('data-bukkenno') : '';

            // 管理会社: 登録会員セクション span.large.bold
            var companyElem = document.querySelector('span.large.bold');
            result.company = companyElem ? companyElem.textContent.trim() : '';

            // TEL: bodyテキストから正規表現
            var bodyText = document.body.innerText || '';
            var telMatch = bodyText.match(/TEL[：:]\\s*([\\d\\-]+)/);
            result.tel = telMatch ? telMatch[1] : '';

            // 取引態様
            for (var i = 0; i < heads.length; i++) {
                if (heads[i].textContent.trim() === '取引態様') {
                    var dataCell = heads[i].nextElementSibling;
                    if (dataCell) result.torihiki = dataCell.textContent.trim();
                    break;
                }
            }

            return result;
        """)

        if not detail_data:
            detail_data = {}

        # =============================================
        # 物件名の反映
        # =============================================
        full_name = detail_data.get('name', '')
        if full_name:
            # フリガナを除去
            full_name = re.sub(r'\([ァ-ヶー]+\)', '', full_name).strip()
            if full_name and full_name not in ('AT', 'AT ', '') and len(full_name) > 1:
                if not any(kw in full_name for kw in ['ログイン', 'メニュー', '検索', 'ATBB']):
                    enrich_stats['name_found'] += 1
                    if '/' in full_name:
                        parts = full_name.rsplit('/', 1)
                        prop_data['名前'] = parts[0].strip()
                        room = parts[1].strip()
                        if room and room != '-':
                            prop_data['号室'] = room
                    else:
                        prop_data['名前'] = full_name

        # =============================================
        # 所在地の反映
        # =============================================
        addr_text = detail_data.get('addr', '')
        if addr_text and '▲' not in addr_text and len(addr_text) > 3:
            prop_data['所在地'] = addr_text
            enrich_stats['addr_found'] += 1

        # =============================================
        # 賃料の反映（画像テキスト → OCRフォールバック）
        # =============================================
        rent_text = detail_data.get('rent', '')

        # OCRフォールバック（賃料がJSで取れなかった場合）
        if not rent_text and OCR_AVAILABLE:
            try:
                rent_img = drv.find_element(By.CSS_SELECTOR, "td.common-data.payment img[id^='price_img'], img[id^='price_img']")
                rent_text = extract_rent_from_image(rent_img)
            except:
                pass

        if rent_text and rent_text != '要確認':
            enrich_stats['rent_found'] += 1
            m = re.search(r'([\d,\.]+)\s*万円', rent_text)
            if m:
                try:
                    prop_data['賃料'] = f"{int(float(m.group(1).replace(',', '')) * 10000):,}円"
                except:
                    prop_data['賃料'] = rent_text
            elif re.search(r'[\d,]+円', rent_text):
                prop_data['賃料'] = rent_text
            else:
                prop_data['賃料'] = rent_text

        # =============================================
        # 管理会社情報の反映
        # =============================================
        company_name = detail_data.get('company', '')
        company_tel = detail_data.get('tel', '')
        if company_name or company_tel:
            prop_data['管理会社情報'] = f"{company_name} {company_tel}".strip()
            enrich_stats['company_found'] += 1

        # =============================================
        # その他フィールドの補完（一覧で取れなかった場合）
        # =============================================
        field_map = {
            '間取り': 'madori', '交通': 'kotsu', '築年月': 'chiku',
            '建物構造': 'kouzou', '専有面積': 'menseki', '階建/階': 'kai',
            '管理費等': 'kanrihi', '取引態様': 'torihiki',
        }
        for jp_key, js_key in field_map.items():
            val = detail_data.get(js_key, '')
            if val and not prop_data.get(jp_key):
                prop_data[jp_key] = val

        # =============================================
        # 物件番号の補完
        # =============================================
        bkn = detail_data.get('bukkenNo', '')
        if bkn:
            prop_data['物件番号'] = bkn

        enrich_stats['success'] += 1

    except Exception as e:
        enrich_stats['page_error'] += 1
        print(f"      ⚠️ 詳細ページ取得エラー (idx={button_index}, 物件番号={bukken_no}): {e}")
        if not enrich_stats['first_error_saved']:
            try:
                os.makedirs(RESULTS_DIR, exist_ok=True)
                drv.save_screenshot(os.path.join(RESULTS_DIR, "enrich_error.png"))
                with open(os.path.join(RESULTS_DIR, "enrich_error.html"), 'w', encoding='utf-8') as f:
                    f.write(drv.page_source)
                enrich_stats['first_error_saved'] = True
                print(f"      📸 エラーのスクリーンショットを保存しました")
            except:
                pass

    finally:
        # =============================================
        # 一覧ページに戻る（新タブ方式 or back()）
        # =============================================
        try:
            if detail_tab_handle:
                drv.close()
                drv.switch_to.window(original_handle)
            else:
                drv.back()
                human_delay(0.5, 1.0)
                try:
                    WebDriverWait(drv, 10).until(
                        lambda d: d.execute_script("return document.readyState") == "complete"
                    )
                except:
                    pass
                wait_and_accept_alert()

            # 一覧ページに戻れたか確認（.property_cardが複数あるはず）
            try:
                card_count = len(drv.find_elements(By.CSS_SELECTOR, ".property_card"))
                if card_count < 2:
                    print(f"      ⚠️ 一覧ページ復帰確認: property_card={card_count}件（期待値>1）")
            except:
                pass

        except Exception as nav_e:
            print(f"      ⚠️ 一覧復帰エラー: {nav_e}")
            try:
                drv.switch_to.window(original_handle)
            except:
                pass
        human_delay(0.3, 0.6)

    return prop_data


def print_enrich_stats():
    """詳細取得の統計を表示"""
    s = enrich_stats
    print(f"\n   📊 詳細取得統計:")
    print(f"      処理: {s['total']}件 | 成功: {s['success']}件")
    print(f"      物件名取得: {s['name_found']}件 | 所在地取得: {s['addr_found']}件")
    print(f"      賃料取得: {s['rent_found']}件 | 管理会社取得: {s['company_found']}件")
    if s['btn_not_found'] > 0:
        print(f"      ボタン未検出: {s['btn_not_found']}件")
    if s['page_error'] > 0:
        print(f"      ページエラー: {s['page_error']}件")

# ============================================================================
# JavaScript一括取得方式の物件抽出（超高速版）
# ブラウザ内でJSを1回実行し、全物件のテキスト+ボタン属性をまとめて返す
# → Seleniumの個別通信（1件あたり5-6往復）を完全に排除
# ============================================================================
JS_EXTRACT_ALL = """
var cards = document.querySelectorAll('.property_card');
var results = [];
for (var i = 0; i < cards.length; i++) {
    var card = cards[i];

    // 物件名: .name から取得
    var nameElem = card.querySelector('.name');
    var name = nameElem ? nameElem.textContent.trim() : '';

    // 物件種別: .type
    var typeElem = card.querySelector('.type');
    var type = typeElem ? typeElem.textContent.trim() : '';

    // 公開日: .date
    var dateElem = card.querySelector('.date');
    var pubDate = dateElem ? dateElem.textContent.trim() : '';

    // 所在地: .map-address のテキスト全体（地図リンク等を除外）
    var addrElem = card.querySelector('.map-address');
    var addr = '';
    if (addrElem) {
        var clone = addrElem.cloneNode(true);
        var removes = clone.querySelectorAll('.map, [onclick*="Chizu"], .fa-location-dot, script');
        for (var m = 0; m < removes.length; m++) removes[m].remove();
        addr = clone.textContent.trim().replace(/\\s+/g, '');
    }

    // テーブルデータ: .info 内の th → td のペアを全取得
    // ※物件番号はJS画像生成のためスキップ（data-bukkenno属性で取得）
    var ths = card.querySelectorAll('.info th');
    var tableData = {};
    for (var j = 0; j < ths.length; j++) {
        var th = ths[j];
        var key = th.textContent.trim();
        // 物件番号セルはJS関数が入るのでスキップ
        if (key === '物件番号') continue;
        var td = th.nextElementSibling;
        if (td && td.tagName === 'TD') {
            var val = td.textContent.trim();
            // スクリプト混入チェック
            if (val.indexOf('Image(') < 0 && val.indexOf('function') < 0) {
                tableData[key] = val;
            }
        }
    }

    // 支払情報: .payment 内の dt/dd ペア（賃料は画像なのでスキップ）
    var paymentDts = card.querySelectorAll('.payment dt');
    var paymentData = {};
    for (var j = 0; j < paymentDts.length; j++) {
        var dt = paymentDts[j];
        var dd = dt.nextElementSibling;
        if (dd && dd.tagName === 'DD') {
            var key = dt.textContent.trim();
            if (key !== '賃料') {
                var val = dd.textContent.trim();
                if (val && val.indexOf('Image(') < 0) {
                    paymentData[key] = val;
                }
            }
        }
    }

    // 賃料: 画像（alt/title → price_value_div → price_txt_div）
    // ※ATBBでは kakakuChinryoImage() で画像生成、alt属性は空の場合が多い
    var rentText = '';
    var priceImg = card.querySelector('img[id^="price_img"]');
    if (priceImg) {
        rentText = priceImg.alt || priceImg.title || '';
    }
    if (!rentText) {
        var priceTxtDiv = card.querySelector('[id^="price_value_div"]');
        if (priceTxtDiv) {
            var t = priceTxtDiv.textContent.trim();
            if (t && t.indexOf('Image(') < 0) rentText = t;
        }
    }
    if (!rentText) {
        var priceTxtOuter = card.querySelector('[id^="price_txt_div"]');
        if (priceTxtOuter) {
            var t = priceTxtOuter.textContent.trim();
            if (t && t.indexOf('Image(') < 0) rentText = t;
        }
    }
    // 賃料画像のインデックスとURLを保存（後でOCR用）
    var priceImgIdx = priceImg ? priceImg.id.replace('price_img_', '') : '';
    var priceImgSrc = priceImg ? priceImg.src : '';

    // 物件番号: div.bkn_no_copy[data-bukkenno] 属性から取得
    var bukkenNoElem = card.querySelector('.bkn_no_copy[data-bukkenno], [data-bukkenno]');
    var bukkenNo = bukkenNoElem ? bukkenNoElem.getAttribute('data-bukkenno') : '';

    // 管理会社: .company（テキストで取得可能）
    var companyElem = card.querySelector('.company a, .company');
    var company = companyElem ? companyElem.textContent.trim() : '';

    // 電話番号: .tel（テキストで取得可能）
    var telElem = card.querySelector('.tel a, .tel');
    var tel = telElem ? telElem.textContent.trim().replace(/^TEL\\s*[:：]\\s*/, '') : '';

    // 取引態様: .property_data 内の dt/dd から
    var torihiki = '';
    var dlDts = card.querySelectorAll('.property_data dt');
    for (var j = 0; j < dlDts.length; j++) {
        if (dlDts[j].textContent.trim() === '取引態様') {
            var nextDD = dlDts[j].nextElementSibling;
            if (nextDD) torihiki = nextDD.textContent.trim();
        }
    }

    // 詳細ボタン（button#shosai_N）
    var btn = card.querySelector('button[id^="shosai"]');
    var btnId = btn ? btn.id : '';

    results.push({
        name: name,
        type: type,
        pubDate: pubDate,
        addr: addr,
        tableData: tableData,
        paymentData: paymentData,
        rentText: rentText,
        priceImgIdx: priceImgIdx,
        priceImgSrc: priceImgSrc,
        bukkenNo: bukkenNo,
        company: company,
        tel: tel,
        torihiki: torihiki,
        btnId: btnId
    });
}
return results;
"""

def find_and_extract_properties(drv):
    """JS一括実行で全物件データを高速抽出（property_card DOM構造から直接取得）"""
    properties = []

    # ページ全体をスクロールして遅延レンダリングのカードを強制描画（高速版）
    try:
        card_count = drv.execute_script("return document.querySelectorAll('.property_card').length;")
        # 全カードを1回のJS実行で強制描画（段階スクロール+トップ復帰を一括実行）
        drv.execute_script("""
            var cards = document.querySelectorAll('.property_card');
            if (cards.length > 0) {
                var step = Math.max(1, Math.floor(cards.length / 5));
                for (var i = 0; i < cards.length; i += step) {
                    cards[i].scrollIntoView({behavior: 'instant'});
                }
                cards[cards.length - 1].scrollIntoView({behavior: 'instant'});
                window.scrollTo(0, 0);
            }
        """)
        time.sleep(0.1)  # 最小限の描画待ち
    except Exception:
        pass

    try:
        raw_items = drv.execute_script(JS_EXTRACT_ALL)
    except Exception as e:
        print(f"      ⚠️ JS抽出エラー: {e}")
        # フォールバック: 旧方式（テキスト抽出）を試行
        return find_and_extract_properties_fallback(drv)

    if not raw_items:
        return properties

    # === デバッグ: JS抽出結果の品質チェック ===
    areas_from_js = set()
    empty_table_count = 0
    for item in raw_items:
        td = item.get('tableData', {})
        area = td.get('専有面積', '')
        if area:
            areas_from_js.add(area)
        if not td.get('間取り') and not td.get('専有面積'):
            empty_table_count += 1
    print(f"      [DEBUG] JS抽出: {len(raw_items)}件, tableData空={empty_table_count}件, ユニーク面積={len(areas_from_js)}種")
    if len(areas_from_js) == 1 and len(raw_items) > 5:
        print(f"      ⚠️ [DEBUG] 全カードが同一面積! 値={areas_from_js.pop()} → DOM遅延レンダリングの可能性")
    if empty_table_count > len(raw_items) * 0.5:
        print(f"      ⚠️ [DEBUG] {empty_table_count}/{len(raw_items)}件のtableDataが空 → 全カード個別スクロールで再試行")
        # 各カードを個別にスクロールして強制描画→再抽出
        try:
            drv.execute_script("""
                var cards = document.querySelectorAll('.property_card');
                for (var i = 0; i < cards.length; i++) {
                    cards[i].scrollIntoView({behavior: 'instant'});
                }
                window.scrollTo(0, 0);
            """)
            time.sleep(0.15)
            raw_items_retry = drv.execute_script(JS_EXTRACT_ALL)
            if raw_items_retry:
                # 再試行結果の品質チェック
                retry_empty = sum(1 for item in raw_items_retry
                                  if not item.get('tableData', {}).get('間取り')
                                  and not item.get('tableData', {}).get('専有面積'))
                print(f"      [DEBUG] 再試行結果: {len(raw_items_retry)}件, tableData空={retry_empty}件")
                if retry_empty < empty_table_count:
                    raw_items = raw_items_retry
                    print(f"      ✅ 再試行でデータ改善 (空: {empty_table_count}→{retry_empty}件)")
        except Exception as e:
            print(f"      ⚠️ 再試行エラー: {e}")

    # 最初3件のtableDataを出力
    for idx, item in enumerate(raw_items[:3]):
        td = item.get('tableData', {})
        print(f"      [DEBUG] card[{idx}] tableData: 面積={td.get('専有面積','')}, 間取り={td.get('間取り','')}, 築年月={td.get('築年月','')}")

    for item in raw_items:
        data = {
            '名前': '', '号室': '', '賃料': '', '管理費等': '', '礼金': '', '敷金': '',
            '間取り': '', '専有面積': '', '階建/階': '', '所在地': '', '築年月': '',
            '交通': '', '建物構造': '', '取引態様': '', '管理会社情報': '', '公開日': '',
            '物件番号': '', '抽出日時': datetime.now().isoformat()
        }

        # --- 物件名と号室 ---
        raw_name = item.get('name', '')
        if raw_name:
            # フリガナを除去: "物件名(フリガナ)" → "物件名"
            raw_name = re.sub(r'\([ァ-ヶー]+\)', '', raw_name).strip()
            # "/-" を除去（号室なしの場合）
            raw_name = re.sub(r'/\s*-\s*$', '', raw_name).strip()
            if '/' in raw_name:
                parts = raw_name.rsplit('/', 1)
                data['名前'] = parts[0].strip()
                data['号室'] = parts[1].strip()
            else:
                data['名前'] = raw_name

        # --- 所在地 ---
        addr = item.get('addr', '')
        if addr and addr != '▲' and len(addr) > 3:
            data['所在地'] = addr

        # --- テーブルデータ（間取り, 専有面積, 階建/階, 築年月, 交通, 建物構造） ---
        table_data = item.get('tableData', {})
        for key in ['間取り', '専有面積', '階建/階', '築年月', '交通', '建物構造']:
            val = table_data.get(key, '')
            if val:
                data[key] = val.strip()

        # 物件番号（data-bukkenno属性から取得 - テーブル内はJS関数のため不使用）
        bukken_no = item.get('bukkenNo', '')
        if bukken_no:
            data['物件番号'] = bukken_no

        # --- 支払情報（管理費等, 礼金, 敷金） ---
        payment_data = item.get('paymentData', {})
        if payment_data.get('管理費等'):
            data['管理費等'] = payment_data['管理費等']
        if payment_data.get('礼金'):
            data['礼金'] = payment_data['礼金']
        if payment_data.get('敷金'):
            data['敷金'] = payment_data['敷金']

        # --- 賃料（画像から取得した場合） ---
        rent_text = item.get('rentText', '')
        if rent_text:
            rent_text = normalize_rent(rent_text)
            if rent_text:
                data['賃料'] = rent_text

        # 賃料が取れなかった場合、画像URLを保存（最後に一括OCR）
        if not data['賃料']:
            data['_price_img_idx'] = item.get('priceImgIdx', '')
            data['_price_img_src'] = item.get('priceImgSrc', '')

        # --- 管理会社情報 ---
        company = item.get('company', '')
        tel = item.get('tel', '')
        if company or tel:
            data['管理会社情報'] = f"{company} {tel}".strip()

        # --- 取引態様 ---
        torihiki = item.get('torihiki', '')
        if torihiki:
            data['取引態様'] = torihiki.strip()

        # --- 公開日 ---
        pub_date = item.get('pubDate', '')
        if pub_date:
            data['公開日'] = pub_date

        # --- 詳細ボタンID（enrichment用） ---
        btn_id = item.get('btnId', '')
        if btn_id:
            data['_btn_id'] = btn_id

        # 物件名がある場合のみ追加
        if data.get('名前') and data['名前'] not in ('AT', 'AT ', ''):
            properties.append(data)
        elif data.get('物件番号'):
            # 名前がなくても物件番号があれば追加（詳細ページで補完）
            if not data['名前']:
                data['名前'] = '(詳細ページで取得)'
            properties.append(data)

    # --- 賃料OCR: 画像URLを延期リストに保存 + バックグラウンドDL開始 ---
    rent_missing = [p for p in properties if not p.get('賃料') and p.get('_price_img_src')]
    if rent_missing:
        for prop in rent_missing:
            src = prop.get('_price_img_src', '')
            if src:
                key = make_property_key(prop)
                deferred_ocr_items.append({
                    'property_key': key,
                    'img_url': src,
                    'name': prop.get('名前', ''),
                })
                # バックグラウンドでダウンロード開始（スキャンと並行）
                if key not in _bg_futures:
                    _bg_futures[key] = _bg_executor.submit(_download_single, src)
        print(f"      📋 賃料OCR延期: {len(rent_missing)}件（バックグラウンドDL中）")

    # 一時フィールドを削除（_btn_idはenrichment後にmainループで削除）
    for prop in properties:
        prop.pop('_price_img_idx', None)
        prop.pop('_price_img_src', None)

    return properties


def find_and_extract_properties_fallback(drv):
    """フォールバック: 旧方式のテキスト抽出（property_cardが見つからない場合）"""
    properties = []
    try:
        buttons = drv.find_elements(By.CSS_SELECTOR, "button[name='shosai'], button[id^='shosai']")
        for btn in buttons:
            try:
                parent = btn.find_element(By.XPATH, "./ancestor::div[contains(@class, 'property_card')]")
            except:
                parent = btn.find_element(By.XPATH, "./..")
            text = parent.text if parent else ''
            if text:
                data = extract_data_from_text(text)
                onclick = btn.get_attribute('onclick') or ''
                m = re.search(r"'(\d+)'", onclick)
                if m and not data['物件番号']:
                    data['物件番号'] = m.group(1)
                if data.get('名前'):
                    properties.append(data)
    except Exception as e:
        print(f"      ⚠️ フォールバック抽出エラー: {e}")
    return properties

def extract_data_from_text(text):
    """テキストから物件データを正規表現で抽出"""
    data = {
        '名前': '', '号室': '', '賃料': '', '管理費等': '', '礼金': '', '敷金': '',
        '間取り': '', '専有面積': '', '階建/階': '', '所在地': '', '築年月': '',
        '交通': '', '建物構造': '', '取引態様': '', '管理会社情報': '', '公開日': '',
        '物件番号': '', '抽出日時': datetime.now().isoformat()
    }

    lines = text.split('\n')

    # ---- 物件名と号室 ----
    # 「No.X 貸マンション 物件名/号室」形式を探す
    for line in lines[:5]:
        line = line.strip()
        # 「貸マンション」「貸アパート」等を含む行を探す
        if any(k in line for k in ['貸マンション', '貸アパート', '貸戸建', '新築貸']):
            name_text = line
            # No.X プレフィックスを除去
            name_text = re.sub(r'^No\.\d+\s*', '', name_text)
            # 種別プレフィックスを除去
            name_text = re.sub(r'^(新築貸アパート|新築貸マンション|貸アパート|貸マンション|貸戸建)\s*', '', name_text)
            if '/' in name_text:
                parts = name_text.rsplit('/', 1)
                data['名前'] = parts[0].strip()
                data['号室'] = parts[1].strip()
            else:
                data['名前'] = name_text.strip()
            break

    # 名前が取れなかった場合、最初の行を使う
    if not data['名前'] and lines:
        first_line = lines[0].strip()
        first_line = re.sub(r'^No\.\d+\s*', '', first_line)
        first_line = re.sub(r'^(新築貸アパート|新築貸マンション|貸アパート|貸マンション|貸戸建)\s*', '', first_line)
        if '/' in first_line:
            parts = first_line.rsplit('/', 1)
            data['名前'] = parts[0].strip()
            data['号室'] = parts[1].strip()
        elif first_line and len(first_line) > 1:
            data['名前'] = first_line

    # ---- 各フィールドを正規表現で抽出 ----
    m = re.search(r'管理費等\s*([\d,\.]+円|なし|-)', text)
    if m: data['管理費等'] = m.group(1).strip()

    m = re.search(r'礼金\s*([\d\.]+ヶ月|なし|-)', text)
    if m: data['礼金'] = m.group(1).strip()

    m = re.search(r'敷金\s*([\d\.]+ヶ月|なし|-)', text)
    if m: data['敷金'] = m.group(1).strip()

    m = re.search(r'間取り\s*([\dA-Za-z]+[LDKS]*)', text)
    if m: data['間取り'] = m.group(1).strip()

    m = re.search(r'専有面積\s*([\d\.]+㎡)', text)
    if m: data['専有面積'] = m.group(1).strip()

    m = re.search(r'階建/階\s*([^\n]+)', text)
    if m: data['階建/階'] = m.group(1).strip()

    m = re.search(r'所在地\s*([^\n]+)', text)
    if m:
        loc = m.group(1).strip()
        data['所在地'] = re.sub(r'\s*(地図|地図を見る)$', '', loc)

    m = re.search(r'築年月\s*([\d/]+)', text)
    if m: data['築年月'] = m.group(1).strip()

    # 交通（次のフィールドラベルまで取得、ただし他フィールド混入を防ぐ）
    m = re.search(r'交通\s*([^\n]+)', text)
    if m:
        transport = m.group(1).strip()
        # 「専有面積」等が混入している場合はカット
        for stop_word in ['専有面積', '階建/階', '築年月', '坪単価']:
            idx = transport.find(stop_word)
            if idx > 0:
                transport = transport[:idx].strip()
        data['交通'] = transport

    m = re.search(r'建物構造\s*(\S+)', text)
    if m:
        structure = m.group(1).strip()
        # 「物件番号」等が混入している場合はカット
        for stop_word in ['物件番号', '取引態様']:
            idx = structure.find(stop_word)
            if idx > 0:
                structure = structure[:idx].strip()
        data['建物構造'] = structure

    m = re.search(r'取引態様\s*[★]?(\S+)', text)
    if m: data['取引態様'] = m.group(1).strip()

    # 会社情報
    m = re.search(r'TEL\s*:\s*([^\n]+)', text)
    if m:
        tel = m.group(1).strip()
        for i, line in enumerate(lines):
            if 'TEL' in line and i > 0:
                company = lines[i-1].replace('★貸主', '').replace('★', '').replace('媒介', '').strip()
                data['管理会社情報'] = f"{company} {tel}"
                break

    m = re.search(r'公開日[：:]\s*([\d/]+)', text)
    if m: data['公開日'] = m.group(1).strip()

    # 賃料（テキストから）
    m = re.search(r'賃料\s*([\d,\.]+円|[\d,\.]+万円)', text)
    if m:
        data['賃料'] = m.group(1).strip()

    return data


def chunked(lst, n):
    """リストをn個ずつのバッチに分割"""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def get_shikugun_list(drv, area_id):
    """検索ページの市区郡セレクトボックスからoption一覧を取得

    Returns: [(value, text), ...] のリスト
    """
    try:
        options = drv.execute_script("""
        var select = document.getElementById('sentaku1ZenShikugun_""" + area_id + """');
        if (!select) return [];
        var result = [];
        for (var i = 0; i < select.options.length; i++) {
            var opt = select.options[i];
            if (opt.value) {
                result.push([opt.value, opt.text.trim()]);
            }
        }
        return result;
        """)
        return options or []
    except Exception as e:
        print(f"   ⚠️ 市区郡リスト取得エラー: {e}")
        return []


def navigate_to_shikugun_page(drv, wait_obj, area_id, prefecture_name):
    """検索ページから所在地選択画面まで遷移する（バッチごとに再利用）

    Returns: True if successful, False otherwise
    """
    drv.get(TARGET_URL)
    human_delay(0.5, 1.0)
    wait_and_accept_alert()

    # 賃貸居住用(06)を選択
    wait_obj.until(EC.presence_of_element_located((By.NAME, "atbbShumokuDaibunrui")))
    shumoku_radio = drv.find_element(By.CSS_SELECTOR, "input[name='atbbShumokuDaibunrui'][value='06']")
    drv.execute_script("arguments[0].click();", shumoku_radio)

    # すべてのエリアチェックを外し、対象の都道府県のみチェック
    area_boxes = drv.find_elements(By.CSS_SELECTOR, "input[name='area']")
    for box in area_boxes:
        if box.is_selected():
            drv.execute_script("arguments[0].click();", box)
    target_box = drv.find_element(By.CSS_SELECTOR, f"input[name='area'][value='{area_id}']")
    if not target_box.is_selected():
        drv.execute_script("arguments[0].click();", target_box)

    # 所在地検索ボタン
    search_btn = wait_obj.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='所在地検索']")))
    drv.execute_script("arguments[0].click();", search_btn)
    wait_and_accept_alert()
    human_delay(1.0, 1.5)
    wait_for_page_ready(drv)
    wait_and_accept_alert()
    return True


# ============================================================================
# メイン処理
# ============================================================================
try:
    # 既存データを読み込む（差分更新用）
    existing_data = load_existing_data()

    # ---------------------------------------------------------
    # 1. ログイン（atbb_scraping.py と同じロジック）
    # ---------------------------------------------------------
    print("🚀 ATBB ログイン開始")
    driver.get("https://members.athome.jp/portal")

    login_id_field = wait.until(EC.presence_of_element_located((By.ID, "loginFormText")))
    login_id_field.send_keys(LOGIN_ID)
    password_field = wait.until(EC.presence_of_element_located((By.ID, "passFormText")))
    password_field.send_keys(PASSWORD)

    submit_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='submit']")))
    driver.execute_script("arguments[0].click();", submit_btn)

    # ポータル画面への遷移を待機
    try:
        WebDriverWait(driver, 10).until(lambda d: "portal" in d.current_url or len(d.find_elements(By.LINK_TEXT, "物件検索")) > 0)
    except:
        pass
    print("✅ ログイン成功 → ポータルへ遷移")

    # ---------------------------------------------------------
    # 2. 物件検索 → 流通物件検索へ移動（atbb_scraping.py と同じロジック）
    # ---------------------------------------------------------
    try:
        obj_link = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.LINK_TEXT, "物件検索")))
        driver.execute_script("arguments[0].click();", obj_link)
        print("📁 物件検索ページへ")
    except:
        try:
            obj_link = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.LINK_TEXT, "物件・会社検索")))
            driver.execute_script("arguments[0].click();", obj_link)
            print("📁 物件・会社検索ページへ")
        except:
            obj_link = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "物件検索")))
            driver.execute_script("arguments[0].click();", obj_link)
            print("📁 物件検索ページへ（部分一致）")

    # 流通物件検索ボタンを探す
    try:
        human_delay(0.3, 0.5)
        ryutsuu_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@data-action, '/atbb/nyushuSearch') and contains(., '流通物件検索')]"))
        )
        try:
            ryutsuu_btn.click()
        except:
            driver.execute_script("arguments[0].click();", ryutsuu_btn)
        print("🏠 流通物件検索をクリック")
        human_delay(0.5, 1.0)
        wait_and_accept_alert()
    except Exception as e:
        print(f"⚠️ 流通物件検索ボタンが見つかりませんでした: {e}")
        print("  → 直接URLで遷移を試みます...")
        driver.get(TARGET_URL)
        human_delay(0.5, 1.0)

    # タブ切替（新しいタブが開く場合の対応）
    human_delay(0.3, 0.5)
    print(f"  → 現在のタブ数: {len(driver.window_handles)}")

    try:
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > 1)
        print(f"  → 新しいタブが開きました（タブ数: {len(driver.window_handles)}）")
    except:
        print(f"  → 新しいタブが開かれませんでした。現在のURL: {driver.current_url}")

    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])
        print(f"🆕 タブ切替: {driver.current_url}")
        wait_for_page_ready(driver)
    else:
        print("  → 同じタブで続行します")
        human_delay(0.5, 1.0)

    # 同時ログインエラー（強制終了画面）が出た場合の対応
    if "ConcurrentLoginException.jsp" in driver.current_url:
        print("⚠ 同時ログイン検出 → 強制終了へ")
        try:
            force_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(
                (By.XPATH, "//input[@type='button' and contains(@value,'強制終了させてATBBを利用する')]")
            ))
            driver.execute_script("arguments[0].click();", force_btn)
            wait_and_accept_alert()
            WebDriverWait(driver, 10).until(lambda d: "mainservlet/bfcm003s201" in d.current_url or "nyushuSearch" not in d.current_url)
            print("✅ 強制終了完了 → 保存条件ページへ")
        except:
            print("⚠️ 強制終了処理に失敗しました")

    # ---------------------------------------------------------
    # 3. 各都道府県ごとにループ処理
    # ---------------------------------------------------------
    display_count_changed = False  # 100件表示切替は1回だけ

    # テストモード時は東京都のみ
    prefectures_to_process = TARGET_PREFECTURES
    if TEST_MODE:
        prefectures_to_process = [TARGET_PREFECTURES[0]]
        print(f"🧪 テストモード: {prefectures_to_process[0][1]}のみ、最大{TEST_LIMIT}件")

    for area_id, prefecture_name in prefectures_to_process:
        if interrupted: break

        print(f"\n==============================================")
        print(f"🗺️ 【{prefecture_name}】 のスクレイピングを開始します (ID: {area_id})")
        print(f"==============================================")

        prefecture_count_before = len(all_properties)
        prefecture_keys = set()  # この県で今回見つかった物件キーを追跡

        # --- 市区郡リストを取得するために検索ページへ遷移 ---
        try:
            navigate_to_shikugun_page(driver, wait, area_id, prefecture_name)
        except Exception as e:
            print(f"⚠️ {prefecture_name}の検索ページ遷移エラー: {e}")
            continue

        # 市区郡リスト取得
        print("⚙️ 市区郡リストを取得中...")
        shikugun_list = get_shikugun_list(driver, area_id)
        if not shikugun_list:
            print(f"⚠️ {prefecture_name}: 市区郡リスト取得失敗 → スキップ")
            continue

        batches = list(chunked(shikugun_list, SHIKUGUN_BATCH_SIZE))
        total_batches = len(batches)
        print(f"📊 {prefecture_name}: {len(shikugun_list)}市区町村 → {total_batches}バッチに分割")

        # テストモード時は最初のバッチのみ
        if TEST_MODE:
            batches = batches[:1]
            print(f"🧪 テストモード: 最初のバッチのみ処理")

        prefecture_page_properties = []  # この県のページ物件を一時保持
        batch_failed_shikugun = []  # 失敗したバッチの市区町村名

        for batch_idx, batch in enumerate(batches):
            if interrupted: break

            batch_values = [value for value, name in batch]
            batch_names = [name for value, name in batch]
            print(f"\n🗺️ 【{prefecture_name}】バッチ {batch_idx+1}/{total_batches}: {', '.join(batch_names)} ({len(batch)}市区町村)")

            try:
                # 検索ページへ遷移（バッチごとに再ナビゲーション）
                navigate_to_shikugun_page(driver, wait, area_id, prefecture_name)

                # このバッチの市区郡のみ選択
                print(f"🏙️ バッチ内市区郡を選択中...")
                wait.until(EC.presence_of_element_located((By.ID, f"sentaku1ZenShikugun_{area_id}")))
                targets_js = json.dumps(batch_values)
                driver.execute_script(f"""
                var selectBox = document.getElementById('sentaku1ZenShikugun_{area_id}');
                var targets = {targets_js};
                for (var i = 0; i < selectBox.options.length; i++) {{
                    selectBox.options[i].selected = targets.includes(selectBox.options[i].value);
                }}
                """)
                driver.find_element(By.ID, "sentaku1SentakuButton").click()
                wait_and_accept_alert()

                # 条件入力画面へ
                wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='条件入力画面へ']"))).click()
                wait_and_accept_alert()
                human_delay(0.5, 1.0)

                # 条件入力画面 → チェックボックスは何も入れず、そのまま検索
                print("📝 条件未指定で検索実行...")
                check_and_wait_for_captcha()

                wait.until(EC.presence_of_element_located((By.NAME, "bfcm370s001")))

                current_url = driver.current_url
                try:
                    btn = driver.find_element(By.CSS_SELECTOR, "input[value='検索']")
                except:
                    btn = driver.find_element(By.XPATH, "//input[@type='submit' and contains(@value, '検索')]")

                driver.execute_script("arguments[0].click();", btn)
                wait_and_accept_alert()

                WebDriverWait(driver, 30).until(
                    lambda d: d.current_url != current_url or len(d.find_elements(By.ID, "tbl")) > 0
                )
                human_delay(1.0, 1.5)
                print("✓ 検索結果画面へ遷移成功")

            except Exception as batch_nav_err:
                print(f"⚠️ バッチ {batch_idx+1} ナビゲーションエラー: {batch_nav_err} → スキップ")
                batch_failed_shikugun.extend(batch_names)
                continue

            # ---------------------------------------------------------
            # 表示件数を100件に変更（初回のみ。セッション中は維持される）
            # ---------------------------------------------------------
            if not display_count_changed:
                try:
                    count_select = Select(driver.find_element(By.CSS_SELECTOR, "select[name='pngDisplayCount']"))
                    count_select.select_by_value("100")
                    print("🔢 表示件数を100件に変更（セッション中維持）")
                    wait_for_page_ready(driver)
                    human_delay(0.5, 1.0)
                    wait_and_accept_alert()
                    display_count_changed = True
                except Exception as e:
                    print(f"ℹ️ 表示件数の変更スキップ: {e}")

            # ---------------------------------------------------------
            # 一覧画面のスクレイピングループ
            # ---------------------------------------------------------
            page = 1
            batch_properties = []

            while not interrupted:
                batch_label = f"{prefecture_name} [{','.join(batch_names)}]"
                print(f"📄 {batch_label} - {page}ページ目を取得中...")

                # 定期的なメモリ解放（50ページごと）
                if page % 50 == 0 and page > 0:
                    print(f"   🧹 メモリ解放中... (page={page})")
                    all_properties.clear()
                    gc.collect()
                    try:
                        driver.execute_script("if(window.console && console.clear) console.clear();")
                    except Exception:
                        pass

                # カードの出現を待機（画像読み込みは待たない・高速版）
                wait_for_cards_ready(driver)

                # === 物件カード検出＆抽出（Selenium直接方式） ===
                page_properties = find_and_extract_properties(driver)

                # テストモード: 件数制限
                if TEST_MODE and page_properties:
                    remaining = TEST_LIMIT - len(all_properties)
                    if remaining <= 0:
                        print(f"🧪 テスト上限 {TEST_LIMIT}件 に達しました")
                        break
                    if len(page_properties) > remaining:
                        page_properties = page_properties[:remaining]

                if not page_properties:
                    # 検索結果なし？
                    if driver.find_elements(By.XPATH, "//*[contains(text(), '該当する物件がありません')]"):
                        print("ℹ️ 該当物件なし")
                        break

                    # リトライ: ページ読み込みが遅い可能性があるので再試行
                    retry_success = False
                    for retry in range(3):
                        print(f"⚠️ 物件カードが検出できません → リトライ {retry+1}/3 ...")
                        human_delay(2.0, 3.0)
                        try:
                            driver.refresh()
                            wait_for_page_ready(driver)
                            human_delay(1.0, 2.0)
                        except:
                            pass
                        page_properties = find_and_extract_properties(driver)
                        if page_properties:
                            print(f"✓ リトライ{retry+1}回目で {len(page_properties)}件 検出成功")
                            retry_success = True
                            break

                    if not retry_success:
                        btn_count = len(driver.find_elements(By.TAG_NAME, 'button'))
                        card_count = len(driver.find_elements(By.CSS_SELECTOR, '.property_card'))
                        print(f"❌ 物件カードが検出できません（button数: {btn_count}, .property_card数: {card_count}）")
                        print(f"   現在のURL: {driver.current_url}")
                        batch_failed_shikugun.extend(batch_names)
                        break

                # 県情報を付与
                for prop in page_properties:
                    prop['抽出県'] = prefecture_name

                # === 詳細ページエンリッチメント（高速化のためデフォルトOFF） ===
                if ENRICH_DETAILS:
                    enriched_count = 0
                    for i, prop in enumerate(page_properties):
                        if interrupted:
                            break
                        name = prop.get('名前', '')
                        addr = prop.get('所在地', '')
                        rent = prop.get('賃料', '')
                        company = prop.get('管理会社情報', '')
                        name_missing = (not name or name in ('AT', 'AT ', '', '(詳細ページで取得)') or len(name) <= 2)
                        addr_missing = (not addr or '▲' in addr or len(addr) <= 3)
                        rent_missing = (not rent or rent == '要確認')
                        company_missing = (not company)
                        needs_enrich = name_missing or addr_missing or rent_missing or company_missing
                        if needs_enrich:
                            prop_btn_id = prop.get('_btn_id', '')
                            prop = enrich_property_from_detail(driver, wait, prop, button_index=i, btn_id=prop_btn_id)
                            page_properties[i] = prop
                            enriched_count += 1
                    if enriched_count > 0:
                        print(f"   ✅ {enriched_count}件の物件情報を詳細ページで補完しました")

                # _btn_id一時フィールドを削除
                for prop in page_properties:
                    prop.pop('_btn_id', None)

                # === SQLiteにupsert（ページ単位で即時保存） ===
                page_keys = upsert_properties_to_db(page_properties, prefecture_name)
                prefecture_keys.update(page_keys)

                added_count = len(page_properties)
                all_properties.extend(page_properties)
                batch_properties.extend(page_properties)
                prefecture_page_properties.extend(page_properties)

                print(f"   => {added_count}件を処理 (バッチ内: {len(batch_properties)}件, 県内総計: {len(prefecture_page_properties)}件)")

                # テストモード: 上限チェック
                if TEST_MODE and len(all_properties) >= TEST_LIMIT:
                    print(f"🧪 テスト上限 {TEST_LIMIT}件 に達しました。ループ終了。")
                    break

                # 次のページへ（リトライ付き）
                MAX_NEXT_RETRIES = 5
                next_page_success = False
                is_last_page = False

                for next_attempt in range(MAX_NEXT_RETRIES):
                    try:
                        # ドライバーが生きているか確認
                        if not is_driver_alive(driver):
                            print(f"   ❌ WebDriver接続切れ検出 (リトライ {next_attempt+1}/{MAX_NEXT_RETRIES})")
                            driver = recreate_driver()
                            wait = WebDriverWait(driver, 30)
                            if not relogin_and_navigate_to_search(driver, area_id, prefecture_name):
                                print(f"   ❌ 再ログイン失敗")
                                continue
                            print(f"   ⚠️ ドライバー再作成後、検索再実行が必要 → 次ページ復帰を試みます")
                            break

                        # 次へボタンを探す
                        next_btn = None
                        try:
                            next_btn = driver.find_element(By.CSS_SELECTOR, "a[title='次へ']")
                        except:
                            try:
                                next_btn = driver.find_element(By.XPATH, "//a[contains(text(), '次へ')]")
                            except:
                                pass

                        if not next_btn:
                            print("ℹ️ 次へボタンがないため、終了します")
                            is_last_page = True
                            break

                        # disabledチェック
                        btn_class = next_btn.get_attribute("class") or ""
                        if "disabled" in btn_class or not next_btn.is_enabled():
                            print("ℹ️ 最後のページに到達しました")
                            is_last_page = True
                            break

                        # クリック実行
                        driver.execute_script("arguments[0].click();", next_btn)
                        wait_and_accept_alert()

                        # カード出現を直接待機（画像読み込みは待たない）
                        cards_found = wait_for_cards_ready(driver, timeout=15)

                        # 物件カードの存在確認
                        card_count = len(driver.find_elements(By.CSS_SELECTOR, '.property_card')) if not cards_found else 1
                        if card_count > 0 or cards_found:
                            page += 1
                            next_page_success = True
                            break
                        else:
                            # もう少し待ってみる
                            time.sleep(2)
                            card_count = len(driver.find_elements(By.CSS_SELECTOR, '.property_card'))
                            if card_count > 0:
                                page += 1
                                next_page_success = True
                                break
                            print(f"   ⚠️ 次ページ遷移後にカードなし (リトライ {next_attempt+1}/{MAX_NEXT_RETRIES})")

                    except Exception as e:
                        err_str = str(e)
                        print(f"   ⚠️ 次へボタンクリック失敗 (リトライ {next_attempt+1}/{MAX_NEXT_RETRIES}): {err_str}")

                        # HTTPConnectionPool timeout → ドライバー死亡の可能性
                        if "Read timed out" in err_str or "ConnectionReset" in err_str or "RemoteDisconnected" in err_str:
                            print(f"   → WebDriver接続タイムアウト → ページリフレッシュで復旧を試みます")
                            time.sleep(5)
                            if not is_driver_alive(driver):
                                print(f"   → ドライバー死亡確認 → 再作成します")
                                try:
                                    driver = recreate_driver()
                                    wait = WebDriverWait(driver, 30)
                                    if relogin_and_navigate_to_search(driver, area_id, prefecture_name):
                                        print(f"   → 再ログイン成功。検索再実行が必要 → 県頭から再実行")
                                        break
                                except Exception as recreate_err:
                                    print(f"   ❌ ドライバー再作成失敗: {recreate_err}")
                                continue
                            else:
                                # ドライバーは生きている → リフレッシュで回復
                                try:
                                    driver.refresh()
                                    wait_for_page_ready(driver, timeout=20, max_retries=2)
                                    human_delay(1.0, 2.0)
                                except Exception:
                                    pass
                                continue
                        else:
                            # その他のエラー → ページリフレッシュしてリトライ
                            try:
                                driver.refresh()
                                wait_for_page_ready(driver, timeout=15)
                                human_delay(1.0, 2.0)
                            except Exception:
                                time.sleep(3)
                            continue

                if is_last_page:
                    break
                if not next_page_success:
                    print(f"   ❌ {MAX_NEXT_RETRIES}回リトライしたが次ページに遷移できませんでした → バッチを終了")
                    batch_failed_shikugun.extend(batch_names)
                    break

            print(f"   📊 バッチ {batch_idx+1}/{total_batches} 完了: {len(batch_properties)}件")

            # テストモード上限チェック
            if TEST_MODE and len(all_properties) >= TEST_LIMIT:
                break

        # === 県ごとの差分更新: 見つからなかった物件を募集終了に ===
        prefecture_count_added = len(prefecture_page_properties)
        db_existing_count = get_db_count(prefecture_name)

        if batch_failed_shikugun:
            print(f"⚠️ {prefecture_name}: 以下の市区町村はバッチ失敗: {', '.join(batch_failed_shikugun)}")

        if prefecture_count_added == 0:
            print(f"⚠️ {prefecture_name}: 0件（該当物件なし or 取得失敗）— 募集終了マークはスキップ")
        elif db_existing_count > 0 and prefecture_count_added < db_existing_count * 0.5:
            print(f"⚠️ {prefecture_name}: {prefecture_count_added}件 取得（DB内{db_existing_count}件の{prefecture_count_added*100//db_existing_count}%）")
            print(f"   → 取得率50%未満のため、募集終了マークはスキップ（中途半端スクレイプ防止）")
        else:
            print(f"✅ {prefecture_name}: {prefecture_count_added}件 取得完了")
            mark_disappeared_properties(prefecture_name, prefecture_keys)

    # ---------------------------------------------------------
    # 賃料OCR一括処理（全ページスキャン完了後）
    # ---------------------------------------------------------
    batch_ocr_rent_images()

    # ---------------------------------------------------------
    # 最終サマリー
    # ---------------------------------------------------------
    total_db = get_db_count()
    print(f"\n🎉 完了！ データは SQLite ({DB_PATH}) に保存されました。")
    print(f"   今回処理: {len(all_properties)}件")
    print(f"   DB内募集中物件: {total_db}件")
    if not all_properties:
        print("\n⚠️ 物件データが取得できませんでした")

except KeyboardInterrupt:
    print("\n\n⚠️ 中断されました。SQLiteに既に保存済みです。")
except Exception as e:
    import traceback
    print(f"❌ エラー発生: {e}")
    traceback.print_exc()
    print("   SQLiteに既に保存済みのデータは保持されます。")
finally:
    try:
        if driver: driver.quit()
    except:
        pass
