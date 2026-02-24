import os
import sys
import time
import csv
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

# ========= Chrome設定 =========
print("🔧 Chrome設定を開始します...")

def human_delay(min_sec=0.5, max_sec=1.5):
    """人間らしいランダムな待機時間"""
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
json_filename = None
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
            human_delay(1.0, 2.0)
            return True
    except:
        pass
    return False

def save_data_to_files():
    """全データを JSON ファイルに保存 (データベース形式)"""
    global json_filename, all_properties
    
    if not all_properties:
        return
        
    if not json_filename:
        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)
        json_filename = os.path.join(results_dir, "properties_database_list.json")
        
    try:
        # メタデータを追加せずに直接配列を入れるか、リストとして保存
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_properties, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"      ⚠️ JSON保存エラー: {e}")

# ============================================================================
# 画像（賃料）からテキストを抽出・解読するロジック
# （詳細ページのロジックを一覧用にカスタマイズ）
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
# 検索結果一覧から1件の物件データを抽出する関数
# ============================================================================
def extract_property_from_list_item(card_element):
    data = {
        '名前': '', '号室': '', '賃料': '', '管理費等': '', '礼金': '', '敷金': '', 
        '間取り': '', '専有面積': '', '階建/階': '', '所在地': '', '築年月': '', 
        '交通': '', '建物構造': '', '取引態様': '', '管理会社情報': '', '公開日': '',
        '物件番号': '', '抽出日時': datetime.now().isoformat()
    }
    
    try:
        # ---- 物件名と号室 ----
        # ".name" 要素または最初の行
        try:
            name_elem = card_element.find_element(By.CSS_SELECTOR, ".name")
            name_text = name_elem.text.strip()
        except:
            try:
                # 代替: aタグやh2などを探す
                name_elem = card_element.find_element(By.XPATH, ".//a[contains(@href, 'detail')] | .//h2 | .//h3")
                name_text = name_elem.text.strip()
            except:
                name_text = card_element.text.split('\n')[0] if card_element.text else ''

        if name_text:
            if '/' in name_text:
                parts = name_text.rsplit('/', 1) # 最後の/で分割
                data['名前'] = parts[0].strip()
                data['号室'] = parts[1].strip()
            # 「貸アパート」などのプレフィックスを除去
            data['名前'] = re.sub(r'^(新築貸アパート|新築貸マンション|貸アパート|貸マンション|貸戸建)\s*', '', data['名前'])
        
        # テキスト全体を使って正規表現で項目を抽出する
        text = card_element.text
        
        # ---- 管理費等 ----
        m = re.search(r'管理費等\s*([\d,\.]+円|なし|-)', text)
        if m: data['管理費等'] = m.group(1).strip()
        
        # ---- 礼金 ----
        m = re.search(r'礼金\s*([\d\.]+ヶ月|なし|-)', text)
        if m: data['礼金'] = m.group(1).strip()
        
        # ---- 敷金 ----
        m = re.search(r'敷金\s*([\d\.]+ヶ月|なし|-)', text)
        if m: data['敷金'] = m.group(1).strip()
        
        # ---- 間取り ----
        m = re.search(r'間取り\s*([\d\w]+)', text)
        if m: data['間取り'] = m.group(1).strip()
        
        # ---- 専有面積 ----
        m = re.search(r'専有面積\s*([\d\.]+㎡)', text)
        if m: data['専有面積'] = m.group(1).strip()
        
        # ---- 階建/階 ----
        m = re.search(r'階建/階\s*([^\n]+)', text)
        if m: data['階建/階'] = m.group(1).strip()
        
        # ---- 所在地 ----
        m = re.search(r'所在地\s*([^\n]+)', text)
        if m: 
            loc = m.group(1).strip()
            data['所在地'] = re.sub(r'\s*地図$', '', loc)
        
        # ---- 築年月 ----
        m = re.search(r'築年月\s*([\d/]+)', text)
        if m: data['築年月'] = m.group(1).strip()
        
        # ---- 交通 ----
        m = re.search(r'交通\s*([^\n]+\n[^\n]+\n[^\n]+)', text)
        if m:
            data['交通'] = m.group(1).replace('\n', ' ').strip()
        else:
             m2 = re.search(r'交通\s*([^\n]+)', text)
             if m2: data['交通'] = m2.group(1).strip()

        # ---- 建物構造 ----
        m = re.search(r'建物構造\s*([^\n]+)', text)
        if m: data['建物構造'] = m.group(1).strip()
        
        # ---- 取引態様 ----
        m = re.search(r'取引態様\s*[★]?([^\n]+)', text)
        if m: data['取引態様'] = m.group(1).strip()
        
        # ---- 会社情報 ----
        m = re.search(r'TEL :\s*([^\n]+)', text)
        if m:
            tel = m.group(1).strip()
            # その上の行（会社名）を取得
            lines = text.split('\n')
            for i, line in enumerate(lines):
                if 'TEL :' in line and i > 0:
                    company = lines[i-1].replace('★貸主', '').replace('媒介', '').strip()
                    data['管理会社情報'] = f"{company} {tel}"
                    break
        
        # ---- 公開日 ----
        m = re.search(r'公開日：\s*([\d/]+)', text)
        if m: data['公開日'] = m.group(1).strip()
        
        # ---- 物件番号 ----
        # ボタンのonclick属性などから抽出を試みる
        try:
            btn = card_element.find_element(By.CSS_SELECTOR, "button[name='shosai'], button[id^='shosai']")
            onclick = btn.get_attribute("onclick")
            if onclick:
                m = re.search(r"'(\d+)'", onclick)
                if m: data['物件番号'] = m.group(1)
        except:
            pass

        # ---- 賃料 (画像・テキスト) ----
        # 賃料はテキストとして含まれていない場合、画像として抽出
        m = re.search(r'賃料\s*([\d,\.]+円|[\d,\.]+万円)', text)
        if m:
            data['賃料'] = m.group(1).strip()
        else:
            try:
                # 料金画像のimgを探す
                price_imgs = card_element.find_elements(By.CSS_SELECTOR, "img[src*='price'], img[id*='price']")
                for img in price_imgs:
                    rent = extract_rent_from_image(img)
                    if rent != '要確認':
                        data['賃料'] = rent
                        break
            except:
                pass

    except Exception as e:
        print(f"      ⚠️ アイテム抽出エラー: {e}")
        
    return data

# ============================================================================
# メイン処理
# ============================================================================
try:
    print("🚀 ATBB ログイン開始")
    driver.get("https://members.athome.jp/portal")

    login_id_field = wait.until(EC.presence_of_element_located((By.ID, "loginFormText")))
    login_id_field.send_keys(LOGIN_ID)
    password_field = wait.until(EC.presence_of_element_located((By.ID, "passFormText")))
    password_field.send_keys(PASSWORD)
    
    submit_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='submit']")))
    driver.execute_script("arguments[0].click();", submit_btn)
    
    print("✅ ログイン成功")
    human_delay(2, 4)

    # 各都道府県ごとにループ処理
    for area_id, prefecture_name in TARGET_PREFECTURES:
        if interrupted: break
        
        print(f"\n==============================================")
        print(f"🗺️ 【{prefecture_name}】 のスクレイピングを開始します (ID: {area_id})")
        print(f"==============================================")
        
        # 物件検索ページへ（直接URL遷移）
        driver.get(TARGET_URL)
        human_delay(2, 3)

        # 重複タブ対応やアラート対応
        wait_and_accept_alert()

        # 同時ログインエラー対応
        if "ConcurrentLoginException" in driver.current_url:
            print("⚠ 同時ログイン検出 → 強制終了へ")
            try:
                force_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//input[contains(@value,'強制終了させてATBBを利用する')]")))
                driver.execute_script("arguments[0].click();", force_btn)
                wait_and_accept_alert()
                human_delay(2, 3)
            except:
                pass

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
        human_delay(2, 3)

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
            human_delay(2, 3)
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
        # 一覧画面のスクレイピングループ
        # ---------------------------------------------------------
        page = 1
        
        while not interrupted:
            print(f"📄 {prefecture_name} - {page}ページ目を取得中...")
            human_delay(2, 4) # ページごとの読み込み待機

            # カード形式またはテーブル行を探す
            item_elements = driver.find_elements(By.CSS_SELECTOR, ".property_card, [class*='property'], [class*='bukken']")
            
            if not item_elements:
                # 検索結果なし？
                if driver.find_elements(By.XPATH, "//*[contains(text(), '該当する物件がありません')]"):
                    print("ℹ️ 該当物件なし")
                    break
                # テーブル形式のみの場合
                try:
                    table = driver.find_element(By.ID, "tbl")
                    item_elements = table.find_elements(By.XPATH, ".//tr[descendant::button[contains(@name, 'shosai')]]")
                except:
                    print("⚠️ 解析可能な物件リストが見つかりません")
                    break

            print(f"   => {len(item_elements)}件の物件項目を発見")
            
            added_count = 0
            for idx, item in enumerate(item_elements):
                if interrupted: break
                
                # 詳細開かずに直接抽出
                prop_data = extract_property_from_list_item(item)
                prop_data['抽出県'] = prefecture_name
                
                # 最低限「物件名」が取れていれば保存
                if prop_data['名前']:
                    all_properties.append(prop_data)
                    added_count += 1
            
            print(f"   => {added_count}件の物件データを追加しました (総計: {len(all_properties)}件)")
            
            # 逐次保存
            if page % 1 == 0:
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
                    # 無効化されているかチェック
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

    print(f"\n🎉 すべての都道府県のスクレイピングが完了しました。(総計: {len(all_properties)}件)")
    save_data_to_files()
    print(f"データは {json_filename} に保存されました。")

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
