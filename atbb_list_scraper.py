import os
import sys
import time
import json
import requests
import hashlib
import signal
import io
import re
from datetime import datetime
from urllib.parse import urlparse

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
LOGIN_ID = "001089150164"
PASSWORD = "zezehihi893"

TARGET_URL = "https://atbb.athome.co.jp/front-web/mainservlet/bfcm003s201"

# 対象の都道府県 (ID, 県名)
TARGET_PREFECTURES = [
    ("13", "東京都"),
    ("11", "埼玉県"),
    ("12", "千葉県"),
    ("14", "神奈川県")
]

# 結果ファイルパス（固定）
RESULTS_DIR = "results"
JSON_FILEPATH = os.path.join(RESULTS_DIR, "properties_database_list.json")

# ========= Chrome設定 =========
print("🔧 Chrome設定を開始します...")

def human_delay(min_sec=0.3, max_sec=0.8):
    """人間らしいランダムな待機時間（高速化版）"""
    time.sleep(random.uniform(min_sec, max_sec))

if USE_UNDETECTED:
    print("  → undetected-chromedriver でブラウザを起動中...")
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = uc.Chrome(options=chrome_options, use_subprocess=True)
else:
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
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
            print("   ブラウザ画面で「私はロボットではありません」をクリックして手動解決してください。")
            input(">> CAPTCHAを解決したらEnterキーを押してください...")
            human_delay(0.5, 1.0)
            return True
    except:
        pass
    return False

# ============================================================================
# 差分更新（インクリメンタル）機能
# ============================================================================
def make_property_key(prop):
    """物件の一意キーを生成（名前+号室+所在地）"""
    name = prop.get('名前', '')
    room = prop.get('号室', '')
    addr = prop.get('所在地', '')
    return f"{name}|{room}|{addr}"

def load_existing_data():
    """既存のJSONデータを読み込む"""
    if os.path.exists(JSON_FILEPATH):
        try:
            with open(JSON_FILEPATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"📂 既存データを読み込みました: {len(data)}件")
            return data
        except Exception as e:
            print(f"⚠️ 既存データ読み込みエラー: {e}")
    return []

def merge_and_save(new_properties, existing_properties):
    """新規スクレイピング結果と既存データをマージし、差分更新する

    - 今回取得できた物件 → 追加または更新
    - 既存にあったが今回出てこなかった物件 → 削除（=最新のみ保持）
    """
    # 今回取得した物件のキーセット
    new_keys = {}
    for prop in new_properties:
        key = make_property_key(prop)
        if key and key != '||':
            new_keys[key] = prop

    # 既存データのキーセット
    existing_keys = {}
    for prop in existing_properties:
        key = make_property_key(prop)
        if key and key != '||':
            existing_keys[key] = prop

    # 統計
    added = 0
    updated = 0
    deleted = 0
    unchanged = 0

    final_properties = []

    for key, prop in new_keys.items():
        if key in existing_keys:
            # 既存にあった → 更新（新しいデータで上書き）
            updated += 1
        else:
            # 新規物件
            added += 1
        final_properties.append(prop)

    # 既存にあったが今回出てこなかった物件はカウントするが含めない（削除）
    for key in existing_keys:
        if key not in new_keys:
            deleted += 1

    print(f"\n📊 差分更新結果:")
    print(f"   新規追加: {added}件")
    print(f"   更新: {updated}件")
    print(f"   削除（掲載終了）: {deleted}件")
    print(f"   最終件数: {len(final_properties)}件")

    return final_properties

def save_data_to_files():
    """全データを JSON ファイルに保存"""
    global all_properties

    if not all_properties:
        return

    os.makedirs(RESULTS_DIR, exist_ok=True)

    try:
        with open(JSON_FILEPATH, 'w', encoding='utf-8') as f:
            json.dump(all_properties, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"      ⚠️ JSON保存エラー: {e}")

# ============================================================================
# 画像（賃料）からテキストを抽出・解読するロジック
# ============================================================================
def extract_rent_from_image(img_element):
    rent_text = ''
    try:
        rent_text = img_element.get_attribute("alt") or img_element.get_attribute("title") or ''

        if not rent_text and OCR_AVAILABLE and OCR_READER is not None:
            img_src = img_element.get_attribute("src")
            if img_src:
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
                except Exception as e:
                    pass
    except:
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
ENRICH_DETAILS = True  # 詳細ページで物件情報を補完するか

def enrich_property_from_detail(drv, wait_obj, prop_data):
    """詳細ページにアクセスして正式な物件名・住所・賃料・管理会社を取得

    Args:
        drv: WebDriverインスタンス
        wait_obj: WebDriverWaitインスタンス
        prop_data: 一覧から取得した物件データ dict

    Returns:
        enriched prop_data dict
    """
    bukken_no = prop_data.get('物件番号', '')
    if not bukken_no:
        return prop_data

    try:
        # 詳細ボタンをクリック（物件番号からonclickで特定）
        detail_btn = None
        try:
            detail_btn = drv.find_element(
                By.CSS_SELECTOR, f"button[onclick*=\"'{bukken_no}'\"]"
            )
        except:
            try:
                detail_btn = drv.find_element(By.ID, f"shosai_{bukken_no}")
            except:
                # ボタンが見つからない場合はスキップ
                return prop_data

        # 現在のURLを記憶（戻る用）
        list_url = drv.current_url

        drv.execute_script("arguments[0].click();", detail_btn)
        wait_and_accept_alert()
        human_delay(1.5, 2.5)

        # 詳細ページの読み込み待ち
        try:
            WebDriverWait(drv, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except:
            pass

        # --- 物件名の取得 ---
        try:
            name_elem = drv.find_element(By.CSS_SELECTOR, ".title-bar .name")
            full_name = name_elem.text.strip()
            if full_name and full_name != 'AT' and len(full_name) > 1:
                # 号室を分離
                if '/' in full_name:
                    parts = full_name.rsplit('/', 1)
                    prop_data['名前'] = parts[0].strip()
                    prop_data['号室'] = parts[1].strip()
                else:
                    prop_data['名前'] = full_name
        except:
            try:
                name_elem = drv.find_element(By.XPATH, "//p[contains(@class, 'name')]")
                full_name = name_elem.text.strip()
                if full_name and full_name != 'AT' and len(full_name) > 1:
                    if '/' in full_name:
                        parts = full_name.rsplit('/', 1)
                        prop_data['名前'] = parts[0].strip()
                        prop_data['号室'] = parts[1].strip()
                    else:
                        prop_data['名前'] = full_name
            except:
                pass

        # --- 所在地の取得 ---
        try:
            addr_elem = drv.find_element(
                By.XPATH,
                "//td[contains(@class, 'common-head') and contains(text(), '所在地')]"
                "/following-sibling::td[contains(@class, 'common-data')]"
            )
            addr_text = addr_elem.text.strip()
            addr_text = addr_text.split('地図を見る')[0].strip()
            addr_text = addr_text.split('地図')[0].strip()
            if addr_text and '▲' not in addr_text and len(addr_text) > 3:
                prop_data['所在地'] = addr_text
        except:
            pass

        # --- 賃料の取得（画像のalt/title → テキスト → OCR） ---
        try:
            rent_head = drv.find_element(
                By.XPATH, "//td[contains(@class, 'common-head') and text()='賃料']"
            )
            rent_cell = rent_head.find_element(
                By.XPATH, "./following-sibling::td[contains(@class, 'payment')]"
            )
            rent_text = ''
            # 方法1: 画像のalt/title
            try:
                rent_img = rent_cell.find_element(By.CSS_SELECTOR, "img[id^='price_img']")
                rent_text = rent_img.get_attribute("alt") or rent_img.get_attribute("title") or ''
            except:
                pass
            # 方法2: 非表示divのテキスト
            if not rent_text:
                try:
                    price_div = rent_cell.find_element(By.CSS_SELECTOR, "div[id^='price_txt_div']")
                    rent_text = price_div.text.strip()
                except:
                    pass
            # 方法3: セルのテキスト
            if not rent_text:
                cell_text = rent_cell.text.strip()
                if cell_text and '管理費' not in cell_text:
                    rent_text = cell_text
            # 方法4: OCR
            if not rent_text and OCR_AVAILABLE:
                try:
                    rent_img = rent_cell.find_element(By.CSS_SELECTOR, "img[id^='price_img']")
                    rent_text = extract_rent_from_image(rent_img)
                except:
                    pass

            if rent_text and rent_text != '要確認':
                # 正規化
                m = re.search(r'([\d,\.]+)\s*万円', rent_text)
                if m:
                    try:
                        prop_data['賃料'] = f"{int(float(m.group(1).replace(',', '')) * 10000):,}円"
                    except:
                        prop_data['賃料'] = rent_text
                elif re.search(r'[\d,]+円', rent_text):
                    prop_data['賃料'] = rent_text
        except:
            pass

        # --- 管理会社情報の取得（より詳細に） ---
        try:
            page_text = drv.find_element(By.TAG_NAME, "body").text
            # 「管理会社」ラベルの値を取得
            company_name = ''
            company_tel = ''

            # 方法1: テーブルから管理会社情報
            try:
                company_elem = drv.find_element(
                    By.XPATH,
                    "//td[contains(text(), '管理会社') or contains(text(), '元付会社')]"
                    "/following-sibling::td"
                )
                company_name = company_elem.text.strip()
            except:
                pass

            # 方法2: テキストからTELを抽出
            tel_match = re.search(r'TEL\s*[：:]\s*([\d\-]+)', page_text)
            if tel_match:
                company_tel = tel_match.group(1).strip()

            # 方法3: 既存のロジックで取引会社情報
            if not company_name:
                lines = page_text.split('\n')
                for i, line in enumerate(lines):
                    if 'TEL' in line and i > 0:
                        company_name = lines[i-1].replace('★貸主', '').replace('★', '').replace('媒介', '').strip()
                        break

            if company_name or company_tel:
                prop_data['管理会社情報'] = f"{company_name} {company_tel}".strip()
        except:
            pass

        # --- 物件番号の補完 ---
        try:
            bukken_elem = drv.find_element(By.CSS_SELECTOR, ".bukkenno[data-bukkenno]")
            prop_data['物件番号'] = bukken_elem.get_attribute("data-bukkenno") or bukken_no
        except:
            pass

        # 一覧ページに戻る
        drv.back()
        human_delay(1.0, 2.0)
        try:
            WebDriverWait(drv, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except:
            pass
        wait_and_accept_alert()

    except Exception as e:
        print(f"      ⚠️ 詳細ページ取得エラー (物件番号: {bukken_no}): {e}")
        # エラー時は一覧ページに戻る
        try:
            drv.back()
            human_delay(1.0, 2.0)
            wait_and_accept_alert()
        except:
            pass

    return prop_data

# ============================================================================
# JavaScript一括取得方式の物件抽出（超高速版）
# ブラウザ内でJSを1回実行し、全物件のテキスト+ボタン属性をまとめて返す
# → Seleniumの個別通信（1件あたり5-6往復）を完全に排除
# ============================================================================
JS_EXTRACT_ALL = """
var buttons = document.querySelectorAll("button[name='shosai'], button[id^='shosai']");
var results = [];
for (var i = 0; i < buttons.length; i++) {
    var btn = buttons[i];
    var tr = btn.closest('tr');
    if (!tr) tr = btn.parentElement;
    if (!tr) continue;
    results.push({
        text: tr.innerText || '',
        onclick: btn.getAttribute('onclick') || '',
        id: btn.id || '',
        value: btn.value || ''
    });
}
return results;
"""

def find_and_extract_properties(drv):
    """JS一括実行で全物件データを高速抽出（ブラウザ通信1回のみ）"""
    properties = []

    try:
        raw_items = drv.execute_script(JS_EXTRACT_ALL)
    except Exception as e:
        print(f"      ⚠️ JS抽出エラー: {e}")
        return properties

    if not raw_items:
        return properties

    for item in raw_items:
        text = item.get('text', '')
        if not text:
            continue

        data = extract_data_from_text(text)

        # 物件番号をボタン属性から補完
        if not data['物件番号']:
            onclick = item.get('onclick', '')
            m = re.search(r"'(\d+)'", onclick)
            if m:
                data['物件番号'] = m.group(1)
        if not data['物件番号']:
            btn_id = item.get('id', '')
            m = re.search(r'shosai[_-]?(\d+)', btn_id)
            if m:
                data['物件番号'] = m.group(1)
        if not data['物件番号']:
            btn_value = item.get('value', '')
            if btn_value and btn_value.isdigit():
                data['物件番号'] = btn_value

        if data.get('名前'):
            properties.append(data)

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
        human_delay(0.5, 1.0)
        ryutsuu_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@data-action, '/atbb/nyushuSearch') and contains(., '流通物件検索')]"))
        )
        try:
            ryutsuu_btn.click()
        except:
            driver.execute_script("arguments[0].click();", ryutsuu_btn)
        print("🏠 流通物件検索をクリック")
        human_delay(1.0, 2.0)
        wait_and_accept_alert()
    except Exception as e:
        print(f"⚠️ 流通物件検索ボタンが見つかりませんでした: {e}")
        print("  → 直接URLで遷移を試みます...")
        driver.get(TARGET_URL)
        human_delay(1.5, 2.5)

    # タブ切替（新しいタブが開く場合の対応）
    human_delay(0.5, 1.0)
    print(f"  → 現在のタブ数: {len(driver.window_handles)}")

    try:
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > 1)
        print(f"  → 新しいタブが開きました（タブ数: {len(driver.window_handles)}）")
    except:
        print(f"  → 新しいタブが開かれませんでした。現在のURL: {driver.current_url}")

    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])
        print(f"🆕 タブ切替: {driver.current_url}")
        try:
            WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")
        except:
            pass
    else:
        print("  → 同じタブで続行します")
        human_delay(1.0, 2.0)

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

    for area_id, prefecture_name in TARGET_PREFECTURES:
        if interrupted: break

        print(f"\n==============================================")
        print(f"🗺️ 【{prefecture_name}】 のスクレイピングを開始します (ID: {area_id})")
        print(f"==============================================")

        # 物件検索ページへ
        driver.get(TARGET_URL)
        human_delay(1.0, 2.0)
        wait_and_accept_alert()

        print("⚙️ 種目・エリア設定中...")

        # 賃貸居住用(06)を選択
        try:
            wait.until(EC.presence_of_element_located((By.NAME, "atbbShumokuDaibunrui")))
            shumoku_radio = driver.find_element(By.CSS_SELECTOR, "input[name='atbbShumokuDaibunrui'][value='06']")
            driver.execute_script("arguments[0].click();", shumoku_radio)
        except Exception as e:
            print(f"⚠️ 賃貸居住用選択エラー: {e}")
            continue

        # すべてのエリアチェックを外し、対象の都道府県のみチェック
        try:
            area_boxes = driver.find_elements(By.CSS_SELECTOR, "input[name='area']")
            for box in area_boxes:
                if box.is_selected():
                    driver.execute_script("arguments[0].click();", box)

            target_box = driver.find_element(By.CSS_SELECTOR, f"input[name='area'][value='{area_id}']")
            if not target_box.is_selected():
                driver.execute_script("arguments[0].click();", target_box)
            print(f"✓ {prefecture_name}を選択")
        except Exception as e:
            print(f"⚠️ {prefecture_name}選択エラー: {e}")
            continue

        # 所在地検索ボタン
        try:
            search_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='所在地検索']")))
            driver.execute_script("arguments[0].click();", search_btn)
        except:
            print("⚠️ 所在地検索ボタンエラー")
            continue

        wait_and_accept_alert()
        human_delay(1.0, 2.0)

        # 市区郡全選択
        print("🏙️ 市区郡全選択")
        try:
            wait.until(EC.presence_of_element_located((By.ID, f"sentaku1ZenShikugun_{area_id}")))
            driver.execute_script(f"""
            var selectBox = document.getElementById('sentaku1ZenShikugun_{area_id}');
            for (var i = 0; i < selectBox.options.length; i++) {{
                selectBox.options[i].selected = true;
            }}
            """)
            driver.find_element(By.ID, "sentaku1SentakuButton").click()
            wait_and_accept_alert()

            # 条件入力画面へ
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='条件入力画面へ']"))).click()
            wait_and_accept_alert()
            human_delay(1.0, 2.0)
        except Exception as e:
            print(f"⚠️ 市区郡全選択エラー: {e}")
            continue

        # 条件入力画面 (客付HP)
        print("📝 客付不動産会社HPにチェックを入れて検索...")
        check_and_wait_for_captcha()

        try:
            wait.until(EC.presence_of_element_located((By.NAME, "bfcm370s001")))
            hp_check = driver.find_element(By.CSS_SELECTOR, "input[name='kokokuTensaiTaSite'][value='2']")
            if not hp_check.is_selected():
                driver.execute_script("arguments[0].click();", hp_check)

            # 検索実行
            current_url = driver.current_url
            try:
                btn = driver.find_element(By.CSS_SELECTOR, "input[value='検索']")
            except:
                btn = driver.find_element(By.XPATH, "//input[@type='submit' and contains(@value, '検索')]")

            driver.execute_script("arguments[0].click();", btn)
            wait_and_accept_alert()

            # URLが変わるまたはテーブルが見えるまで待機
            WebDriverWait(driver, 30).until(
                lambda d: d.current_url != current_url or len(d.find_elements(By.ID, "tbl")) > 0
            )
            print("✓ 検索結果画面へ遷移成功")
        except Exception as e:
            print(f"⚠️ 検索実行エラー: {e}")
            continue

        # ---------------------------------------------------------
        # 表示件数を100件に変更（初回のみ。セッション中は維持される）
        # ---------------------------------------------------------
        if not display_count_changed:
            try:
                count_select = Select(driver.find_element(By.CSS_SELECTOR, "select[name='pngDisplayCount']"))
                count_select.select_by_value("100")
                print("🔢 表示件数を100件に変更（セッション中維持）")
                # onchangeでsubmitPagingActionが発火→ページリロードを待機
                WebDriverWait(driver, 20).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                human_delay(1.0, 2.0)
                wait_and_accept_alert()
                display_count_changed = True
            except Exception as e:
                print(f"ℹ️ 表示件数の変更スキップ: {e}")

        # ---------------------------------------------------------
        # 一覧画面のスクレイピングループ
        # ---------------------------------------------------------
        page = 1

        while not interrupted:
            print(f"📄 {prefecture_name} - {page}ページ目を取得中...")

            # ページの読み込み完了を待機（固定waitではなくWebDriverWait）
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except:
                pass
            human_delay(0.5, 1.0)

            # === 物件カード検出＆抽出（Selenium直接方式） ===
            page_properties = find_and_extract_properties(driver)

            if not page_properties:
                # 検索結果なし？
                if driver.find_elements(By.XPATH, "//*[contains(text(), '該当する物件がありません')]"):
                    print("ℹ️ 該当物件なし")
                    break
                print(f"⚠️ 物件カードが検出できません（ボタン数: {len(driver.find_elements(By.TAG_NAME, 'button'))}）")
                break

            # 県情報を付与
            for prop in page_properties:
                prop['抽出県'] = prefecture_name

            # === 詳細ページで物件情報を補完（フェーズ0） ===
            if ENRICH_DETAILS:
                enriched_count = 0
                for i, prop in enumerate(page_properties):
                    if interrupted:
                        break
                    name = prop.get('名前', '')
                    addr = prop.get('所在地', '')
                    # マスクされたデータ（AT、▲）の場合のみ詳細ページにアクセス
                    needs_enrich = (
                        (not name or name in ('AT', 'AT ', '') or len(name) <= 2) or
                        (not addr or '▲' in addr or len(addr) <= 3) or
                        (not prop.get('賃料') or prop.get('賃料') == '要確認') or
                        (not prop.get('管理会社情報'))
                    )
                    if needs_enrich and prop.get('物件番号'):
                        print(f"      🔍 詳細取得 ({i+1}/{len(page_properties)}): {name or '(名前なし)'}")
                        prop = enrich_property_from_detail(driver, wait, prop)
                        page_properties[i] = prop
                        enriched_count += 1
                if enriched_count > 0:
                    print(f"   ✅ {enriched_count}件の物件情報を詳細ページで補完しました")

            added_count = len(page_properties)
            all_properties.extend(page_properties)

            print(f"   => {added_count}件の物件データを追加 (総計: {len(all_properties)}件)")

            # 5ページごとに中間保存
            if page % 5 == 0:
                save_data_to_files()

            # 次のページへ
            next_btn = None
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "a[title='次へ']")
            except:
                try:
                    next_btn = driver.find_element(By.XPATH, "//a[contains(text(), '次へ')]")
                except:
                    pass

            if next_btn:
                try:
                    if "disabled" in (next_btn.get_attribute("class") or "") or not next_btn.is_enabled():
                        print("ℹ️ 最後のページに到達しました")
                        break

                    driver.execute_script("arguments[0].click();", next_btn)
                    wait_and_accept_alert()
                    page += 1
                except Exception as e:
                    print(f"⚠️ 次へボタンクリック失敗: {e}")
                    break
            else:
                print("ℹ️ 次へボタンがないため、終了します")
                break

        print(f"✅ {prefecture_name}の処理が完了しました")

    # ---------------------------------------------------------
    # 差分更新＆最終保存
    # ---------------------------------------------------------
    if all_properties:
        if existing_data:
            # 差分マージ（今回取得できなかった物件は削除される）
            all_properties = merge_and_save(all_properties, existing_data)
        else:
            print(f"\n📊 初回実行: {len(all_properties)}件の物件を保存します")

        save_data_to_files()
        print(f"\n🎉 完了！ データは {JSON_FILEPATH} に保存されました。")
        print(f"   最終物件数: {len(all_properties)}件")
    else:
        print("\n⚠️ 物件データが取得できませんでした")

except KeyboardInterrupt:
    print("\n\n⚠️ 中断されました。データを保存して終了します。")
    save_data_to_files()
except Exception as e:
    import traceback
    print(f"❌ エラー発生: {e}")
    traceback.print_exc()
    save_data_to_files()
finally:
    try:
        if driver: driver.quit()
    except:
        pass
