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
print("ATBBスクレイピングスクリプトを開始します")
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

# OCRライブラリの初期化（エンコーディング設定後に実行）
# easyocrを優先（Tesseractのインストールが不要）
try:
    import easyocr
    OCR_AVAILABLE = True
    OCR_TYPE = 'easyocr'
    # easyocrのReaderを事前に初期化（初回読み込みが遅いため）
    print("✅ OCRライブラリが利用可能です（easyocr）- 初期化中...")
    OCR_READER = easyocr.Reader(['ja', 'en'], gpu=False, verbose=False)
    print("✅ easyocr 初期化完了")
except ImportError:
    try:
        from PIL import Image
        import pytesseract
        # pytesseractの動作確認
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

# ジャンプしたいURL（物件検索トップ）
TARGET_URL = "https://atbb.athome.co.jp/front-web/mainservlet/bfcm003s201"

# ========= Chrome設定 =========
print("🔧 Chrome設定を開始します...")

# 人間らしい操作間隔を追加する関数
def human_delay(min_sec=0.5, max_sec=1.5):
    """人間らしいランダムな待機時間"""
    time.sleep(random.uniform(min_sec, max_sec))

if USE_UNDETECTED:
    # undetected-chromedriver を使用（ボット検出回避）
    print("  → undetected-chromedriver でブラウザを起動中...")
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-popup-blocking")  # ポップアップ許可
    chrome_options.add_argument("--disable-notifications")   # 通知無効
    # より自然なユーザーエージェント
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = uc.Chrome(options=chrome_options, use_subprocess=True)
else:
    # 通常のSelenium
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    # より自然なユーザーエージェント
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    print("  → ChromeDriverManagerをインストール中...")
    service = Service(ChromeDriverManager().install())
    print("  → Chromeブラウザを起動中...")
    driver = webdriver.Chrome(service=service, options=options)
    # navigator.webdriverを隠す
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        '''
    })

wait = WebDriverWait(driver, 30)
print("✅ Chromeブラウザの起動が完了しました")

# グローバル変数（中断処理用）
interrupted = False
csv_filename = None
json_filename = None

# 中断シグナルハンドラ
def signal_handler(sig, frame):
    global interrupted
    print("\n\n⚠️ 中断シグナルを受信しました。安全に終了します...")
    interrupted = True
    # データを保存
    save_data_to_files()
    print("✅ データを保存しました")
    if driver:
        try:
            driver.quit()
        except:
            pass
    sys.exit(0)

# WindowsではSIGINTとSIGTERMを設定
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
    """reCAPTCHAが表示されている場合、手動解決を待つ"""
    try:
        # reCAPTCHAの検出（複数のセレクタを試す）
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
                captcha_elem = driver.find_element(By.CSS_SELECTOR, selector)
                if captcha_elem.is_displayed():
                    captcha_found = True
                    break
            except:
                continue
        
        if captcha_found:
            print("\n" + "="*50)
            print("⚠️ reCAPTCHA が検出されました！")
            print("   ブラウザ画面で「私はロボットではありません」をクリックして")
            print("   CAPTCHAを解決してください。")
            print("   解決後、Enterキーを押して続行してください。")
            print("="*50)
            input(">> CAPTCHAを解決したらEnterキーを押してください...")
            print("✓ 続行します...")
            human_delay(1.0, 2.0)
            return True
    except Exception as e:
        pass
    return False

# データをファイルに保存する関数（1件ごとに呼び出し）
def save_data_to_files():
    """CSVとJSONファイルにデータを保存（進捗保存用・最適化版・フォルダ分け）"""
    global csv_filename, json_filename, all_data, all_properties, headers
    
    if not all_data and not all_properties:
        return
    
    # ファイル名を初期化（初回のみ）
    if not csv_filename or not json_filename:
        # 日付フォルダを作成
        date_folder = datetime.now().strftime("%Y%m%d")
        results_dir = os.path.join("results", date_folder)
        os.makedirs(results_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = os.path.join(results_dir, f"atbb_results_{timestamp}.csv")
        json_filename = os.path.join(results_dir, f"properties_{timestamp}.json")
    
    # CSVに保存（最適化：バッファリングを使用）
    if all_data:
        try:
            # ヘッダーが取得できていない場合は、列数から推測
            if not headers:
                headers = [f"列{i+1}" for i in range(len(all_data[0]) if all_data else 0)]
            
            with open(csv_filename, 'w', encoding='utf-8-sig', newline='', buffering=8192) as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(all_data)
        except Exception as e:
            print(f"      ⚠️ CSV保存エラー: {e}")
    
    # JSONに保存（最適化：インデントなし、シンプルなメタデータ）
    if all_properties:
        try:
            json_data = {
                'metadata': {
                    'total_properties': len(all_properties),
                    'last_updated': datetime.now().isoformat()
                },
                'properties': all_properties
            }
            
            # インデントなしで保存（ファイルサイズと書き込み速度を改善）
            with open(json_filename, 'w', encoding='utf-8', buffering=8192) as f:
                json.dump(json_data, f, ensure_ascii=False, separators=(',', ':'))
        except Exception as e:
            print(f"      ⚠️ JSON保存エラー: {e}")

try:
    # ---------------------------------------------------------
    # 1. ログイン
    # ---------------------------------------------------------
    print("🚀 ATBB ログイン開始")
    driver.get("https://members.athome.jp/portal")

    # ログインフォームが表示されるまで待機
    login_id_field = wait.until(EC.presence_of_element_located((By.ID, "loginFormText")))
    login_id_field.send_keys(LOGIN_ID)
    
    password_field = wait.until(EC.presence_of_element_located((By.ID, "passFormText")))
    password_field.send_keys(PASSWORD)
    
    # ログインボタンをクリック
    submit_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='submit']")))
    driver.execute_script("arguments[0].click();", submit_btn)
    
    # ポータル画面への遷移を待機（URLが変わるまで）
    try:
        WebDriverWait(driver, 10).until(lambda d: "portal" in d.current_url or len(d.find_elements(By.LINK_TEXT, "物件検索")) > 0)
    except:
        pass
    print("✅ ログイン成功 → ポータルへ遷移")

    # ---------------------------------------------------------
    # 2. 物件検索 → 流通物件検索へ移動（最適化：最短経路）
    # ---------------------------------------------------------
    # ログイン後のポータル画面が読み込まれるまで待機（最短）
    try:
        # 「物件検索」リンクが表示されるまで待機（最大5秒）
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

    # 流通物件検索ボタンを探す（最短待機）
    try:
        human_delay(0.5, 1.0)
        ryutsuu_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@data-action, '/atbb/nyushuSearch') and contains(., '流通物件検索')]"))
        )
        # 通常のクリックを試す（JavaScriptクリックはボット検出されやすい）
        try:
            ryutsuu_btn.click()
        except:
            driver.execute_script("arguments[0].click();", ryutsuu_btn)
        print("🏠 流通物件検索をクリック")
        human_delay(1.0, 2.0)
        wait_and_accept_alert()
    except Exception as e:
        print(f"⚠️ 流通物件検索ボタンが見つかりませんでした: {e}")
        # フォールバック：直接URLに遷移
        print("  → 直接URLで遷移を試みます...")
        driver.get("https://atbb.athome.co.jp/front-web/mainservlet/bfcm003s201")
        human_delay(2.0, 3.0)

    # タブ切替（ボット対策：人間らしい待機時間を追加）
    human_delay(1.0, 2.0)
    
    print(f"  → 現在のタブ数: {len(driver.window_handles)}")
    
    try:
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > 1)
        print(f"  → 新しいタブが開きました（タブ数: {len(driver.window_handles)}）")
    except:
        print(f"  → 新しいタブが開かれませんでした。現在のURL: {driver.current_url}")
    
    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])
        print(f"🆕 タブ切替: {driver.current_url}")
        
        # ページが読み込まれるまで待機
        try:
            WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")
        except:
            pass
    else:
        # タブが開かない場合、現在のページで続行を試みる
        print("  → 同じタブで続行します")
        human_delay(2.0, 3.0)

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
    # 3. 種目・エリア設定 (東京都・賃貸)
    # ---------------------------------------------------------
    print("⚙️ 種目・エリア設定中...")
    
    # ページが完全に読み込まれるまで待機
    try:
        # 種目選択のラジオボタンが表示されるまで待機
        wait.until(EC.presence_of_element_located((By.NAME, "atbbShumokuDaibunrui")))
        print("  ✓ 種目選択要素を検出")
    except Exception as e:
        print(f"  ⚠️ 種目選択要素が見つかりません: {e}")
        print(f"  現在のURL: {driver.current_url}")
        print(f"  ページタイトル: {driver.title}")
        # 少し待って再試行
        time.sleep(2)
        try:
            wait.until(EC.presence_of_element_located((By.NAME, "atbbShumokuDaibunrui")))
        except:
            raise Exception("種目選択画面に到達できませんでした")

    # 賃貸居住用(06)
    try:
        shumoku_radio = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='atbbShumokuDaibunrui'][value='06']")))
        driver.execute_script("arguments[0].click();", shumoku_radio)
        print("  ✓ 賃貸居住用を選択")
    except Exception as e:
        print(f"  ⚠️ 賃貸居住用の選択に失敗: {e}")
        raise
    
    # 東京都(13) - 要素が見つかるまで待機
    try:
        tokyo_check = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='area'][value='13']")))
        if not tokyo_check.is_selected():
            driver.execute_script("arguments[0].click();", tokyo_check)
            print("  ✓ 東京都を選択")
        else:
            print("  ✓ 東京都は既に選択済み")
    except Exception as e:
        print(f"  ⚠️ 東京都の選択に失敗: {e}")
        # 代替方法を試す
        try:
            # name属性が異なる可能性がある
            tokyo_check = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@value='13' and contains(@name, 'area')]")))
            if not tokyo_check.is_selected():
                driver.execute_script("arguments[0].click();", tokyo_check)
                print("  ✓ 東京都を選択（代替方法）")
        except:
            print(f"  ⚠️ 東京都の選択に失敗しました。続行します...")
    
    # 「所在地検索」ボタン
    try:
        search_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='所在地検索']")))
        driver.execute_script("arguments[0].click();", search_btn)
        print("  ✓ 所在地検索ボタンをクリック")
    except Exception as e:
        print(f"  ⚠️ 所在地検索ボタンのクリックに失敗: {e}")
        raise

    # ---------------------------------------------------------
    # 4. 市区郡選択 (全選択)
    # ---------------------------------------------------------
    print("🏙️ 市区郡選択画面：全エリアを選択します")
    wait.until(EC.presence_of_element_located((By.ID, "sentaku1ZenShikugun_13")))

    # JSで全選択状態にする
    driver.execute_script("""
    var selectBox = document.getElementById('sentaku1ZenShikugun_13');
    for (var i = 0; i < selectBox.options.length; i++) {
        selectBox.options[i].selected = true;
    }
    """)
    
    # 追加ボタン
    driver.find_element(By.ID, "sentaku1SentakuButton").click()
    
    # アラートが表示される可能性があるので処理
    wait_and_accept_alert()

    # 「条件入力画面へ」ボタン
    try:
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='条件入力画面へ']"))).click()
        wait_and_accept_alert()  # アラートが表示される可能性がある
    except Exception as e:
        # アラートが表示されている場合は閉じる
        wait_and_accept_alert()
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='条件入力画面へ']"))).click()
        wait_and_accept_alert()

    # ---------------------------------------------------------
    # 5. 条件入力画面 (客付HPチェック & 検索)
    # ---------------------------------------------------------
    print("📝 条件入力画面：『客付不動産会社HP』にチェックを入れます")
    
    # CAPTCHAチェック
    check_and_wait_for_captcha()
    
    wait.until(EC.presence_of_element_located((By.NAME, "bfcm370s001")))

    # 客付不動産会社HP (name="kokokuTensaiTaSite", value="2")
    hp_check = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='kokokuTensaiTaSite'][value='2']")))
    if not hp_check.is_selected():
        driver.execute_script("arguments[0].click();", hp_check)
        print("  ✓ [客付不動産会社HP] をチェックしました")
    
    # 検索実行
    print("  → 検索ボタンをクリックします...")
    current_url_before = driver.current_url
    
    try:
        search_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='検索']")))
        driver.execute_script("arguments[0].click();", search_btn)
    except:
        # 別の方法で検索ボタンを探す
        search_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and contains(@value, '検索')]")))
        driver.execute_script("arguments[0].click();", search_btn)
    
    # アラートが表示される可能性があるので処理
    wait_and_accept_alert()
    
    # URLが変わるまで待機（検索結果画面に遷移するまで）
    print("  ⏳ 検索結果画面への遷移を待機中...")
    try:
        # URLが変わるまで待つ（最大30秒）
        WebDriverWait(driver, 30).until(
            lambda d: d.current_url != current_url_before and ("bfcm370s" in d.current_url or "tbl" in d.page_source or len(find_property_cards()) > 0)
        )
        print(f"  ✓ 検索結果画面に遷移しました: {driver.current_url}")
    except:
        print(f"  ⚠️ URLが変更されませんでした。現在のURL: {driver.current_url}")

    # ---------------------------------------------------------
    # 6. 検索結果一覧画面 (100件表示へ切り替え)
    # ---------------------------------------------------------
    print("⏳ 検索結果画面の読み込みを待機中...")
    
    # 検索結果が表示されるまで待機（WebDriverWaitで最適化）
    current_url = driver.current_url
    print(f"  現在のURL: {current_url}")
    
    # 検索結果テーブルを複数の方法で探す
    table = None
    table_found = False
    
    # 方法1: ID="tbl"で探す
    try:
        table = wait.until(EC.presence_of_element_located((By.ID, "tbl")))
        print("📍 検索結果テーブルを発見（ID=tbl）")
        table_found = True
    except:
        pass
    
    # 方法2: テーブルタグで探す（複数ある場合は最初のもの）
    if not table_found:
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            if tables:
                # 検索結果らしいテーブルを探す（行数が多いもの）
                for t in tables:
                    rows = t.find_elements(By.TAG_NAME, "tr")
                    if len(rows) > 1:  # ヘッダー行以外にデータ行がある
                        table = t
                        print(f"📍 検索結果テーブルを発見（tableタグ、{len(rows)}行）")
                        table_found = True
                        break
        except:
            pass
    
    # 方法3: 検索結果が0件の場合のメッセージを確認
    if not table_found:
        try:
            no_result_msg = driver.find_element(By.XPATH, "//*[contains(text(), '該当する物件がありません') or contains(text(), '検索結果がありません') or contains(text(), '該当する物件はありません')]")
            print("  ℹ️ 検索結果が0件です")
            print("✅ 処理を終了します（データなし）")
            input(">> Enterキーを押すとブラウザを閉じます...")
            driver.quit()
            sys.exit(0)
        except:
            pass
    
    # テーブルが見つからない場合
    if not table_found:
        print("  ⚠️ テーブルが見つかりません。ページ構造を確認します...")
        print(f"  ページタイトル: {driver.title}")
        
        # ページ内のテーブル数を確認
        try:
            all_tables = driver.find_elements(By.TAG_NAME, "table")
            print(f"  ページ内のテーブル数: {len(all_tables)}")
            for i, t in enumerate(all_tables):
                try:
                    rows = t.find_elements(By.TAG_NAME, "tr")
                    print(f"    テーブル{i+1}: {len(rows)}行")
                except:
                    pass
        except:
            pass
        
        # ページソースの一部を確認（デバッグ用）
        page_source_preview = driver.page_source[:1000]
        print(f"  ページソース（最初の1000文字）: {page_source_preview}")
        raise Exception("検索結果テーブルが見つかりませんでした")

    try:
        count_select = Select(driver.find_element(By.CSS_SELECTOR, "select[name='hyoujiKensu']"))
        count_select.select_by_value("100")
        print("🔢 表示件数を100件に変更しました")
        wait_and_accept_alert()
        time.sleep(3)
    except Exception as e:
        print("ℹ️ 表示件数の変更スキップ (要素が見つからないか既に100件)")

    # ---------------------------------------------------------
    # 7. 検索結果のスクレイピング
    # ---------------------------------------------------------
    print("📊 検索結果のデータ取得を開始します...")
    
    all_data = []
    all_properties = []  # JSON形式で保存する物件データ
    headers = []
    page_num = 1
    
    # テーブルを探す関数
    def find_result_table():
        # 方法1: ID="tbl"で探す
        try:
            return driver.find_element(By.ID, "tbl")
        except:
            pass
        
        # 方法2: テーブルタグで探す
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            for t in tables:
                rows = t.find_elements(By.TAG_NAME, "tr")
                if len(rows) > 1:  # ヘッダー行以外にデータ行がある
                    return t
        except:
            pass
        
        return None
    
    # 物件カードを探す関数（カード形式の検索結果用・100件のみ取得）
    def find_property_cards():
        """カード形式の検索結果から物件カードを取得（100件のみ）"""
        cards = []
        try:
            # 方法1: 「詳細」ボタンがある要素を探す（最も確実な方法）
            # 詳細ボタンがある = 物件カードが存在する
            detail_buttons = driver.find_elements(By.CSS_SELECTOR, "button[name='shosai'], button[id^='shosai']")
            
            # 各ボタンの親要素（物件カード）を取得
            for btn in detail_buttons:
                try:
                    # ボタンの親要素を探す（物件カード）
                    # より具体的なセレクタで探す
                    parent = btn.find_element(By.XPATH, "./ancestor::*[contains(@class, 'property') or contains(@class, 'bukken') or contains(@class, 'card') or contains(@class, 'item')][1]")
                    if parent and parent not in cards:
                        cards.append(parent)
                except:
                    # 親要素が見つからない場合は、ボタン自体をカードとして扱う
                    if btn not in cards:
                        cards.append(btn)
            
            # 100件に制限（実際の物件カードのみ）
            if len(cards) > 100:
                cards = cards[:100]
                print(f"      ℹ️ 物件カードを100件に制限しました（検出: {len(detail_buttons)}件）")
        except:
            pass
        
        # 方法2: property_cardクラスを持つ要素を探す（フォールバック）
        if not cards:
            try:
                cards = driver.find_elements(By.CSS_SELECTOR, ".property_card, [class*='property'], [class*='bukken']")
                if len(cards) > 100:
                    cards = cards[:100]
            except:
                pass
        
        return cards
    
    # 検索結果画面から基本情報を抽出する関数
    def extract_list_info(card_element):
        """検索結果画面のカードから基本情報を抽出"""
        info = {}
        try:
            card_text = card_element.text
            
            # 物件名（最初の行から抽出）
            try:
                lines = card_text.split('\n')
                if lines:
                    # 「No.1 貸マンション 宮島ビル/303」のような形式
                    first_line = lines[0]
                    if '貸マンション' in first_line or '貸アパート' in first_line or '貸戸建' in first_line:
                        parts = first_line.split()
                        if len(parts) > 2:
                            info['物件名'] = ' '.join(parts[2:])
            except:
                pass
            
            # 賃料
            if '賃料' in card_text:
                try:
                    import re
                    rent_match = re.search(r'賃料\s*([\d,\.]+万円?)', card_text)
                    if rent_match:
                        info['賃料'] = rent_match.group(1)
                except:
                    pass
            
            # 間取り
            if '間取り' in card_text:
                try:
                    import re
                    layout_match = re.search(r'間取り\s*([^\n]+)', card_text)
                    if layout_match:
                        info['間取り'] = layout_match.group(1).strip()
                except:
                    pass
            
            # 所在地
            if '所在地' in card_text:
                try:
                    import re
                    address_match = re.search(r'所在地\s*([^\n]+)', card_text)
                    if address_match:
                        info['所在地'] = address_match.group(1).strip()
                except:
                    pass
            
            # 交通
            if '交通' in card_text:
                try:
                    import re
                    access_match = re.search(r'交通\s*([^\n]+)', card_text)
                    if access_match:
                        info['交通'] = access_match.group(1).strip()
                except:
                    pass
            
            # 専有面積
            if '専有面積' in card_text:
                try:
                    import re
                    area_match = re.search(r'専有面積\s*([\d,\.]+㎡)', card_text)
                    if area_match:
                        info['専有面積'] = area_match.group(1)
                except:
                    pass
            
            # 築年月
            if '築年月' in card_text:
                try:
                    import re
                    age_match = re.search(r'築年月\s*([\d/]+)', card_text)
                    if age_match:
                        info['築年数'] = age_match.group(1)
                except:
                    pass
            
        except Exception as e:
            print(f"      ⚠️ 一覧情報抽出エラー: {e}")
        
        return info
    
    # 詳細ページから情報を抽出する関数
    def extract_property_details():
        """詳細ページから物件情報を抽出"""
        details = {}
        
        try:
            # ページが読み込まれるまで待機
            time.sleep(3)
            
            # 物件名（title-bar内のnameクラス）
            try:
                property_name = driver.find_element(By.CSS_SELECTOR, ".title-bar .name").text.strip()
                details['物件名'] = property_name
            except:
                try:
                    property_name = driver.find_element(By.XPATH, "//p[contains(@class, 'name')]").text.strip()
                    details['物件名'] = property_name
                except:
                    details['物件名'] = ''
            
            # 物件番号（data-bukkenno属性から取得）
            try:
                bukken_no_elem = driver.find_element(By.CSS_SELECTOR, ".bukkenno[data-bukkenno]")
                details['物件番号'] = bukken_no_elem.get_attribute("data-bukkenno")
            except:
                try:
                    bukken_no_text = driver.find_element(By.XPATH, "//*[contains(text(), '物件番号')]/following-sibling::*[1]").text.strip()
                    details['物件番号'] = bukken_no_text
                except:
                    details['物件番号'] = ''
            
            # 管理番号
            try:
                kanri_no = driver.find_element(By.XPATH, "//*[contains(text(), '管理番号')]/following-sibling::*[1]").text.strip()
                details['管理番号'] = kanri_no
            except:
                details['管理番号'] = ''
            
            # 所在地（common-dataクラス内、地図ボタンの前）
            try:
                address_elem = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '所在地')]/following-sibling::td[contains(@class, 'common-data')]")
                address_text = address_elem.text.strip()
                # 地図ボタンのテキストを除去
                address_text = address_text.split('地図を見る')[0].strip()
                details['所在地'] = address_text
            except:
                details['所在地'] = ''
            
            # 賃料（複数の方法で抽出を試行）
            # ATBBでは賃料は画像として表示されるため、画像のsrc属性から取得する必要がある
            rent_text = ''
            import re
            
            # 方法1: 画像が読み込まれるまで少し待つ（JavaScriptで動的に設定される）
            time.sleep(0.5)
            
            # 方法2: price_img要素から直接取得（賃料画像は id="price_img_0-1" 等）
            try:
                # 「賃料」のラベルを持つtdの次のtd（payment）内の画像を取得
                rent_head = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and text()='賃料']")
                rent_cell = rent_head.find_element(By.XPATH, "./following-sibling::td[contains(@class, 'payment')]")
                
                # 画像のsrc属性を確認（JavaScriptで動的に設定される）
                rent_img = rent_cell.find_element(By.CSS_SELECTOR, "img[id^='price_img']")
                img_src = rent_img.get_attribute("src") or ''
                
                # 画像が読み込まれている場合、alt属性を確認
                rent_text = rent_img.get_attribute("alt") or rent_img.get_attribute("title") or ''
                
                # alt/titleがない場合、img srcから情報を取得（サイトによっては価格情報がURLに含まれる）
                if not rent_text and img_src:
                    print(f"      → 賃料画像URL: {img_src[:100]}...")
                    
                # テキストがあるか確認（非表示divにテキストがある場合）
                if not rent_text:
                    try:
                        price_txt_div = rent_cell.find_element(By.CSS_SELECTOR, "div[id^='price_txt_div']")
                        rent_text = price_txt_div.text.strip()
                    except:
                        pass
                
                # セル内のテキストを確認（画像が読み込めない場合のフォールバック）
                if not rent_text:
                    cell_text = rent_cell.text.strip()
                    if cell_text and '管理費' not in cell_text:
                        rent_text = cell_text
            except Exception as e:
                print(f"      → 賃料取得エラー（方法1）: {e}")
            
            # 方法3: 最初のpaymentクラスを持つtdを取得（賃料は通常最初）
            if not rent_text:
                try:
                    # すべてのpaymentセルを取得
                    payment_cells = driver.find_elements(By.CSS_SELECTOR, "td.common-data.payment")
                    for payment_cell in payment_cells:
                        # 直前のtdが「賃料」のみか確認（「管理費」を含まない）
                        try:
                            prev_td = payment_cell.find_element(By.XPATH, "./preceding-sibling::td[contains(@class, 'common-head')][1]")
                            prev_text = prev_td.text.strip()
                            if prev_text == '賃料':  # 完全一致
                                # 画像のalt属性を取得
                                try:
                                    rent_img = payment_cell.find_element(By.CSS_SELECTOR, "img[id^='price_img']")
                                    rent_text = rent_img.get_attribute("alt") or rent_img.get_attribute("title") or ''
                                except:
                                    pass
                                
                                if not rent_text:
                                    rent_text = payment_cell.text.strip()
                                
                                if rent_text:
                                    break
                        except:
                            pass
                except Exception as e:
                    print(f"      → 賃料取得エラー（方法3）: {e}")
            
            # 方法4: JavaScriptを実行して画像URLから価格を取得 + OCRで読み取り
            if not rent_text:
                try:
                    # JavaScriptでprice_img要素のsrcを取得
                    js_result = driver.execute_script("""
                        var imgs = document.querySelectorAll('img[id^="price_img"]');
                        var result = [];
                        for (var i = 0; i < imgs.length; i++) {
                            result.push({
                                id: imgs[i].id,
                                src: imgs[i].src,
                                alt: imgs[i].alt,
                                title: imgs[i].title
                            });
                        }
                        return result;
                    """)
                    if js_result:
                        for img_info in js_result:
                            if img_info.get('alt'):
                                rent_text = img_info['alt']
                                break
                            elif img_info.get('title'):
                                rent_text = img_info['title']
                                break
                            # alt/titleがない場合、OCRで画像から読み取る
                            elif img_info.get('src') and OCR_AVAILABLE and OCR_READER is not None:
                                try:
                                    img_url = img_info['src']
                                    # 画像をダウンロード
                                    img_response = requests.get(img_url, timeout=10)
                                    if img_response.status_code == 200:
                                        # easyocrで読み取り（事前初期化したReaderを使用）
                                        try:
                                            results = OCR_READER.readtext(img_response.content)
                                            for result in results:
                                                text = result[1]
                                                # 数字と万円を抽出
                                                price_match = re.search(r'([\d,\.]+)\s*万?円?', text)
                                                if price_match:
                                                    rent_text = price_match.group(0).strip()
                                                    # 「万円」がない場合は追加
                                                    if '万' not in rent_text and '円' not in rent_text:
                                                        rent_text += '万円'
                                                    print(f"      → OCRで賃料を抽出: {rent_text}")
                                                    break
                                            if rent_text:
                                                break
                                        except Exception as ocr_err:
                                            print(f"      → OCRエラー: {ocr_err}")
                                except Exception as dl_err:
                                    print(f"      → 賃料画像のダウンロードエラー: {dl_err}")
                        if not rent_text and js_result:
                            print(f"      → 賃料画像情報: {js_result[0]}")
                except Exception as e:
                    print(f"      → 賃料取得エラー（方法4）: {e}")
            
            # 方法5: テーブル行全体から「賃料」ラベルを探す（「管理費」を除外）
            if not rent_text:
                try:
                    # 「賃料」のみを含むtdを探す（「管理費」や「共益費」を含まない）
                    rent_heads = driver.find_elements(By.CSS_SELECTOR, "td.common-head")
                    for head in rent_heads:
                        head_text = head.text.strip()
                        if head_text == '賃料':  # 完全一致で「管理費等」を除外
                            # 次の兄弟要素を取得
                            try:
                                next_cell = head.find_element(By.XPATH, "./following-sibling::td[1]")
                                cell_text = next_cell.text.strip()
                                # 管理費の値でないことを確認
                                if cell_text and not re.match(r'^[\d,]+円$', cell_text):
                                    # 画像のalt属性を確認
                                    try:
                                        img = next_cell.find_element(By.TAG_NAME, "img")
                                        rent_text = img.get_attribute("alt") or img.get_attribute("title") or ''
                                    except:
                                        pass
                                    
                                    if not rent_text and cell_text:
                                        rent_text = cell_text
                                    
                                    if rent_text:
                                        break
                            except:
                                pass
                except Exception as e:
                    print(f"      → 賃料取得エラー（方法5）: {e}")
            
            # 賃料の正規化（「万円」を「円」に変換など）
            if rent_text:
                import re
                # 「万円」を数値に変換
                if '万円' in rent_text:
                    try:
                        rent_num_str = re.sub(r'[^\d\.]', '', rent_text.replace('万円', ''))
                        if rent_num_str:
                            rent_num = float(rent_num_str)
                            rent_text = f"{int(rent_num * 10000)}円"
                    except:
                        pass
                # カンマを削除して数値のみにする場合
                elif '円' in rent_text and ',' in rent_text:
                    rent_text = rent_text.replace(',', '')
            
            details['賃料'] = rent_text if rent_text else ''
            
            # デバッグ用ログ
            if not rent_text:
                print(f"      ⚠️ 賃料が抽出できませんでした")
                # デバッグ: ページの一部を表示
                try:
                    page_snippet = driver.find_element(By.TAG_NAME, "body").text[:500]
                    if '賃料' in page_snippet:
                        print(f"      → ページ内に「賃料」という文字は見つかりましたが、数値が抽出できませんでした")
                except:
                    pass
            else:
                print(f"      ✓ 賃料: {rent_text}")
            
            # 管理費等
            try:
                kanrihi = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '管理費等')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['管理費等'] = kanrihi
            except:
                details['管理費等'] = ''
            
            # 間取り
            try:
                layout = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '間取り')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['間取り'] = layout
            except:
                details['間取り'] = ''
            
            # 専有面積
            try:
                area = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '専有面積')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['専有面積'] = area
            except:
                details['専有面積'] = ''
            
            # 築年月
            try:
                chiku = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '築年月')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['築年月'] = chiku
            except:
                details['築年月'] = ''
            
            # 建物構造
            try:
                kozo = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '建物構造')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['建物構造'] = kozo
            except:
                details['建物構造'] = ''
            
            # 階建/階
            try:
                kai = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '階建/階')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['階建/階'] = kai
            except:
                details['階建/階'] = ''
            
            # 交通
            try:
                access = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '交通')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['交通'] = access
            except:
                details['交通'] = ''
            
            # 礼金
            try:
                reikin = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '礼金')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['礼金'] = reikin
            except:
                details['礼金'] = ''
            
            # 敷金
            try:
                shikikin = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '敷金')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['敷金'] = shikikin
            except:
                details['敷金'] = ''
            
            # 設備
            try:
                setsubi = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '設備')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['設備'] = setsubi
            except:
                details['設備'] = ''
            
            # 特記事項
            try:
                tokki = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '特記事項')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['特記事項'] = tokki
            except:
                details['特記事項'] = ''
            
            # 備考
            try:
                biko = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '備考')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['備考'] = biko
            except:
                details['備考'] = ''
            
            # 周辺環境
            try:
                shuhen = driver.find_element(By.XPATH, "//td[contains(@class, 'common-head') and contains(text(), '周辺環境')]/following-sibling::td[contains(@class, 'common-data')]").text.strip()
                details['周辺環境'] = shuhen
            except:
                details['周辺環境'] = ''
            
            # 画像URLを抽出してローカルに保存（高画質版を取得）
            # 最適化：ポップアップを開かずに、カルーセルから直接画像URLを取得（大幅に高速化）
            image_urls = []
            saved_image_paths = []
            try:
                # カルーセルから直接画像要素を取得（ポップアップを開かない）
                print(f"      → カルーセルから画像URLを取得中...")
                images = driver.find_elements(By.CSS_SELECTOR, ".gazo-sonota img, .carousel img")
                
                if images:
                    print(f"      → {len(images)}枚の画像を処理中...")
                    for img in images:
                        try:
                            img_src = img.get_attribute("src")
                            if img_src and img_src not in image_urls:
                                # サムネイルURLから高画質版URLを推測（サイズパラメータを削除）
                                high_quality_url = img_src
                                if 'height=' in high_quality_url or 'width=' in high_quality_url:
                                    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
                                    parsed = urlparse(high_quality_url)
                                    query_params = parse_qs(parsed.query)
                                    # サイズ関連のパラメータを削除
                                    for param in ['height', 'width', 'margin', 'dummy']:
                                        query_params.pop(param, None)
                                    # 新しいURLを構築
                                    new_query = urlencode(query_params, doseq=True)
                                    high_quality_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                                
                                if high_quality_url not in image_urls:
                                    image_urls.append(high_quality_url)
                        except Exception as e:
                            print(f"        ⚠️ 画像URL取得エラー: {e}")
                            continue
                    
                    print(f"      ✓ {len(image_urls)}枚の画像URLを取得しました")
                
                # 画像が見つからない場合は、画像リンクから取得
                if not image_urls:
                    carousel_image_links = driver.find_elements(By.CSS_SELECTOR, ".gazo-sonota a, .carousel a")
                    for img_link in carousel_image_links:
                        try:
                            # リンク内のimg要素のsrc属性から取得
                            img_in_link = img_link.find_element(By.TAG_NAME, "img")
                            if img_in_link:
                                img_src = img_in_link.get_attribute("src")
                                if img_src and img_src not in image_urls:
                                    # サイズパラメータを削除
                                    if 'height=' in img_src or 'width=' in img_src:
                                        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
                                        parsed = urlparse(img_src)
                                        query_params = parse_qs(parsed.query)
                                        for param in ['height', 'width', 'margin', 'dummy']:
                                            query_params.pop(param, None)
                                        new_query = urlencode(query_params, doseq=True)
                                        img_src = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                                    image_urls.append(img_src)
                        except:
                            pass
                
            except Exception as e:
                print(f"      ⚠️ 画像処理エラー: {e}")
                import traceback
                traceback.print_exc()
                # フォールバック: カルーセルから直接取得
                try:
                    images = driver.find_elements(By.CSS_SELECTOR, ".gazo-sonota img, .carousel img")
                    for img in images:
                        img_src = img.get_attribute("src")
                        if img_src and img_src not in image_urls:
                            image_urls.append(img_src)
                except:
                    pass
            
            # 画像をローカルに保存
            if image_urls:
                # 保存先ディレクトリを作成（物件番号または管理番号を使用）
                bukken_id = details.get('物件番号', '') or details.get('管理番号', '') or 'unknown'
                # ファイル名に使えない文字を置換
                bukken_id = bukken_id.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')
                
                # 日付フォルダ内に画像フォルダを作成
                date_folder = datetime.now().strftime("%Y%m%d")
                results_dir = os.path.join("results", date_folder)
                images_dir = os.path.join(results_dir, "images", bukken_id)
                os.makedirs(images_dir, exist_ok=True)
                
                print(f"      → 画像をダウンロード中... ({len(image_urls)}枚)")
                # Seleniumのセッションクッキーを取得
                cookies = driver.get_cookies()
                session = requests.Session()
                for cookie in cookies:
                    session.cookies.set(cookie['name'], cookie['value'])
                
                # User-Agentを設定（Seleniumと同じ）
                headers = {
                    'User-Agent': driver.execute_script("return navigator.userAgent;")
                }
                
                for idx, img_url in enumerate(image_urls):
                    try:
                        # 画像をダウンロード（セッションクッキーを使用）
                        response = session.get(img_url, headers=headers, timeout=10)
                        if response.status_code == 200:
                            # ファイル拡張子を取得
                            parsed_url = urlparse(img_url)
                            ext = os.path.splitext(parsed_url.path)[1] or '.jpg'
                            if not ext or ext == '.':
                                ext = '.jpg'
                            
                            # ファイル名を生成
                            filename = f"image_{idx+1:03d}{ext}"
                            filepath = os.path.join(images_dir, filename)
                            
                            # 画像を保存
                            with open(filepath, 'wb') as f:
                                f.write(response.content)
                            
                            saved_image_paths.append(filepath)
                            print(f"        ✓ {filename} を保存しました")
                        else:
                            print(f"        ⚠️ 画像のダウンロードに失敗: {img_url} (ステータスコード: {response.status_code})")
                    except Exception as e:
                        print(f"        ⚠️ 画像のダウンロードエラー: {e}")
                
                details['画像保存パス'] = images_dir
                details['画像数'] = str(len(saved_image_paths))
            else:
                details['画像保存パス'] = ''
                details['画像数'] = '0'
            
        except Exception as e:
            print(f"    ⚠️ 詳細情報抽出エラー: {e}")
            import traceback
            traceback.print_exc()
        
        return details
    
    while True:
        print(f"📄 {page_num}ページ目を処理中...")
        # ページ読み込み待機はWebDriverWaitで行うため、明示的なsleepは不要
        
        # カード形式の検索結果を探す
        property_cards = find_property_cards()
        
        # カードが見つからない場合は、表示形式の切り替えを試みる
        if not property_cards:
            print("  → カード形式が見つかりません。表示形式の切り替えを試みます...")
            
            # 表示形式切り替えボタンを探す
            try:
                # ATBBの表示形式切り替えボタンのセレクタを試す
                switch_selectors = [
                    "input[value='詳細表示']",
                    "button:contains('詳細')",
                    "a:contains('詳細表示')",
                    "input[name='hyojiKubun'][value='1']",  # 詳細表示
                    ".view-switch button",
                    "#hyojiKirikaeSyosai",
                ]
                
                for selector in switch_selectors:
                    try:
                        switch_btn = driver.find_element(By.CSS_SELECTOR, selector)
                        driver.execute_script("arguments[0].click();", switch_btn)
                        print(f"  ✓ 表示形式を切り替えました（{selector}）")
                        human_delay(2.0, 3.0)
                        
                        # 再度カードを探す
                        property_cards = find_property_cards()
                        if property_cards:
                            break
                    except:
                        continue
            except:
                pass
        
        # それでもカードが見つからない場合はテーブル形式から直接抽出
        if not property_cards:
            try:
                table = find_result_table()
                if table is None:
                    print("  ⚠️ 検索結果が見つかりません（カード形式・テーブル形式どちらも）")
                    break
                
                rows = table.find_elements(By.TAG_NAME, "tr")
                if len(rows) <= 1:
                    print("  ℹ️ 検索結果が0件です")
                    break
                
                print(f"  → テーブル形式の検索結果を処理します（{len(rows)-1}件）")
                
                # テーブルから「詳細」ボタンまたはクリック可能な行を探す
                detail_buttons = driver.find_elements(By.CSS_SELECTOR, "button[name='shosai'], input[value='詳細'], button.shosai")
                if not detail_buttons:
                    detail_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), '詳細')] | //input[@value='詳細'] | //a[contains(text(), '詳細')] | //button[@name='shosai'] | //td//button")
                
                # それでも見つからない場合、テーブル行自体をクリック対象とする
                if not detail_buttons:
                    # テーブル内のリンクを探す
                    detail_buttons = driver.find_elements(By.CSS_SELECTOR, "table tr td a[href*='bfcm'], table tr td a[onclick]")
                
                if not detail_buttons:
                    # テーブル行のクリック可能な最初のセル
                    clickable_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr[onclick], table tr[class*='click']")
                    if clickable_rows:
                        detail_buttons = clickable_rows
                
                if not detail_buttons:
                    # 最終手段：テーブルの各行（ヘッダー以外）を取得
                    print("  → 詳細ボタンが見つかりません。テーブル行を確認中...")
                    all_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr, table tr")
                    data_rows = [r for r in all_rows[1:] if r.find_elements(By.TAG_NAME, "td")]  # ヘッダー行を除外
                    if data_rows:
                        print(f"  → {len(data_rows)}件のデータ行を発見")
                        # 各行内のリンクまたはボタンを探す
                        for row in data_rows[:5]:  # 最初の5件で試す
                            links = row.find_elements(By.TAG_NAME, "a")
                            buttons = row.find_elements(By.TAG_NAME, "button")
                            if links:
                                detail_buttons.extend(links[:1])
                            elif buttons:
                                detail_buttons.extend(buttons[:1])
                
                if detail_buttons:
                    print(f"  ✓ {len(detail_buttons)}件のクリック対象を発見しました")
                    # 詳細ボタンを使って物件カードとして扱う
                    # 最大5件（テスト用）
                    TEST_LIMIT = 5
                    initial_card_count = min(len(detail_buttons), TEST_LIMIT)
                    print(f"  ℹ️ テスト用に{initial_card_count}件に制限します")
                    
                    # クリック対象のhref属性を保存（ページ遷移後も使えるように）
                    click_targets_info = []
                    for btn in detail_buttons[:initial_card_count]:
                        try:
                            href = btn.get_attribute('href') or ''
                            onclick = btn.get_attribute('onclick') or ''
                            tag = btn.tag_name
                            click_targets_info.append({'href': href, 'onclick': onclick, 'tag': tag})
                        except:
                            click_targets_info.append({'href': '', 'onclick': '', 'tag': ''})
                    
                    for card_index in range(initial_card_count):
                        if interrupted:
                            print("\n⚠️ 中断が検出されました。処理を停止します...")
                            break
                        
                        print(f"    [{card_index+1}/{initial_card_count}] 物件を処理中...")
                        
                        try:
                            # テーブル内の全リンクを再取得
                            all_links = driver.find_elements(By.CSS_SELECTOR, "table tr td a[href*='bfcm'], table tbody tr td a")
                            if not all_links:
                                all_links = driver.find_elements(By.XPATH, "//table//tr//td//a")
                            
                            if card_index >= len(all_links):
                                print(f"      ⚠️ クリック対象が見つかりません（インデックス: {card_index}）")
                                continue
                            
                            detail_button = all_links[card_index]
                            
                            # 現在のタブを保存
                            original_window = driver.current_window_handle
                            original_count = len(driver.window_handles)
                            
                            # 詳細ボタンをクリック
                            print(f"      → 詳細ページを開きます...")
                            human_delay(0.3, 0.7)
                            driver.execute_script("arguments[0].click();", detail_button)
                            wait_and_accept_alert()
                            human_delay(1.0, 2.0)
                            
                            # 新しいタブが開いたか確認
                            new_tab_opened = len(driver.window_handles) > original_count
                            
                            if new_tab_opened:
                                driver.switch_to.window(driver.window_handles[-1])
                                try:
                                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".contents_box, table")))
                                except:
                                    pass
                            else:
                                try:
                                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".contents_box, table")))
                                except:
                                    pass
                            
                            # 詳細情報を抽出
                            detail_info = extract_property_details()
                            print(f"      ✓ 詳細情報を抽出しました")
                            
                            # データを保存
                            property_obj = {
                                'id': detail_info.get('物件番号', '') + '_' + hashlib.md5(str(time.time()).encode()).hexdigest()[:8],
                                'property_number': detail_info.get('物件番号', ''),
                                'management_number': detail_info.get('管理番号', ''),
                                'name': detail_info.get('物件名', ''),
                                'address': detail_info.get('所在地', ''),
                                'rent': detail_info.get('賃料', ''),
                                'management_fee': detail_info.get('管理費等', ''),
                                'layout': detail_info.get('間取り', ''),
                                'area': detail_info.get('専有面積', ''),
                                'built_date': detail_info.get('築年月', ''),
                                'structure': detail_info.get('建物構造', ''),
                                'floor': detail_info.get('階建/階', ''),
                                'access': detail_info.get('交通', ''),
                                'reikin': detail_info.get('礼金', ''),
                                'shikikin': detail_info.get('敷金', ''),
                                'equipment': detail_info.get('設備', ''),
                                'special_notes': detail_info.get('特記事項', ''),
                                'remarks': detail_info.get('備考', ''),
                                'surroundings': detail_info.get('周辺環境', ''),
                                'images': detail_info.get('images', []),
                                'image_count': len(detail_info.get('images', [])),
                                'tags': [],
                                'extracted_at': datetime.now().isoformat(),
                                'search_keywords': [
                                    detail_info.get('物件名', ''),
                                    detail_info.get('所在地', ''),
                                    detail_info.get('間取り', ''),
                                    detail_info.get('交通', '')
                                ]
                            }
                            
                            all_data.append(property_obj)
                            
                            # 一覧画面に戻る
                            print(f"      → 一覧画面に戻ります...")
                            if new_tab_opened:
                                driver.close()
                                driver.switch_to.window(original_window)
                            else:
                                driver.back()
                            
                            human_delay(1.0, 2.0)
                            
                            # 定期的に保存
                            if len(all_data) % 5 == 0:
                                save_data_to_files(all_data, headers)
                                print(f"      ✓ データを保存しました（{len(all_data)}件）")
                            
                        except Exception as e:
                            print(f"      ⚠️ 詳細ページ処理エラー: {e}")
                            # エラー回復
                            try:
                                if len(driver.window_handles) > 1:
                                    driver.close()
                                    driver.switch_to.window(driver.window_handles[0])
                                else:
                                    driver.back()
                            except:
                                pass
                            human_delay(1.0, 2.0)
                    
                    # テーブル形式の処理完了
                    break
                else:
                    print("  ⚠️ 詳細ボタンが見つかりませんでした")
                    break
                
            except Exception as e:
                print(f"  ⚠️ 検索結果取得エラー: {e}")
                break
        
        # カード形式の検索結果を処理
        if property_cards:
            print(f"  ✓ {len(property_cards)}件の物件カードを発見しました")
            
            # 最初のページでヘッダーを設定
            if page_num == 1 and not headers:
                detail_headers = ['物件名', '物件番号', '管理番号', '所在地', '賃料', '管理費等', '間取り', '専有面積', '築年月', '建物構造', '階建/階', '交通', '礼金', '敷金', '設備', '特記事項', '備考', '周辺環境', '画像数', '画像保存パス']
                headers = detail_headers
                print(f"  ✓ ヘッダーを設定しました: {len(headers)}列")
            
            # 各物件カードを処理（インデックスベースで処理し、毎回カードを再取得）
            # 最初のカード数を取得（テスト用: 5件に制限）
            TEST_LIMIT = 5  # テスト用制限（本番時は100に変更）
            initial_card_count = min(len(property_cards), TEST_LIMIT)
            if len(property_cards) > TEST_LIMIT:
                print(f"  ℹ️ テスト用に物件カードを{TEST_LIMIT}件に制限します（検出: {len(property_cards)}件）")
            
            for card_index in range(initial_card_count):
                # 中断チェック
                if interrupted:
                    print("\n⚠️ 中断フラグが設定されました。処理を終了します...")
                    break
                
                print(f"    [{card_index+1}/{initial_card_count}] 物件を処理中...")
                
                # 毎回カードとボタンを再取得（stale elementを防ぐため）
                list_info = {}
                detail_button = None
                
                try:
                    # 物件カードを再取得
                    current_cards = find_property_cards()
                    if card_index >= len(current_cards):
                        print(f"      ⚠️ カードインデックス{card_index}が範囲外です（カード数: {len(current_cards)}）")
                        # カード数が減った場合は、残りのカードを処理
                        if len(current_cards) == 0:
                            print(f"      ℹ️ カードが0件になったため、処理を終了します")
                            break
                        # インデックスを調整
                        card_index = min(card_index, len(current_cards) - 1)
                    
                    card = current_cards[card_index]
                    
                    # 検索結果画面から基本情報を抽出
                    try:
                        list_info = extract_list_info(card)
                    except Exception as e:
                        print(f"      ⚠️ 一覧情報抽出エラー: {e}")
                        list_info = {}
                    
                    # 「詳細」ボタンを探す（最適化：ページ全体からインデックスで直接取得）
                    try:
                        # 最優先：ページ全体からインデックスで直接取得（最も高速）
                        all_detail_buttons = driver.find_elements(By.CSS_SELECTOR, "button[name='shosai'], button[id^='shosai']")
                        if card_index < len(all_detail_buttons):
                            detail_button = all_detail_buttons[card_index]
                        else:
                            # フォールバック：カードから探す
                            try:
                                detail_button = card.find_element(By.CSS_SELECTOR, "button[name='shosai'], button[id^='shosai']")
                            except:
                                pass
                    except:
                        pass
                
                except Exception as e:
                    print(f"      ⚠️ カードの再取得エラー: {e}")
                    # エラーが発生した場合は、ページ全体からボタンを探す
                    try:
                        all_detail_buttons = driver.find_elements(By.CSS_SELECTOR, "button[name='shosai'], button[id^='shosai']")
                        if card_index < len(all_detail_buttons):
                            detail_button = all_detail_buttons[card_index]
                            list_info = {}  # 一覧情報は取得できない
                        else:
                            print(f"      ℹ️ 「詳細」ボタンが見つかりませんでした（インデックス: {card_index}）")
                            continue
                    except:
                        print(f"      ℹ️ 「詳細」ボタンが見つかりませんでした")
                        continue
                
                # 詳細ページに移動して情報を抽出
                detail_info = {}
                if detail_button:
                    try:
                        # 現在のタブハンドルを保存
                        original_window_handle = driver.current_window_handle
                        original_window_handles_count = len(driver.window_handles)
                        
                        # 詳細ボタンをクリック
                        print(f"      → 詳細ページを開きます...")
                        driver.execute_script("arguments[0].click();", detail_button)
                        wait_and_accept_alert()
                        
                        # 新しいタブが開かれたか確認
                        new_tab_opened = len(driver.window_handles) > original_window_handles_count
                        
                        if new_tab_opened:
                            # 新しいタブに切り替え（明示的に待機）
                            try:
                                WebDriverWait(driver, 5).until(lambda d: len(d.window_handles) > original_window_handles_count)
                            except:
                                pass
                            new_window_handles = driver.window_handles
                            new_tab_handle = [h for h in new_window_handles if h != original_window_handle][0]
                            driver.switch_to.window(new_tab_handle)
                            print(f"      → 新しいタブに切り替えました")
                            # 詳細ページが読み込まれるまで待機
                            try:
                                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".contents_box, table")))
                            except:
                                pass
                        else:
                            # 同じタブで開かれた場合は、詳細ページが読み込まれるまで待機
                            try:
                                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".contents_box, table")))
                            except:
                                pass
                        
                        # 詳細情報を抽出
                        detail_info = extract_property_details()
                        print(f"      ✓ 詳細情報を抽出しました")
                        
                        # 一覧画面に戻る
                        print(f"      → 一覧画面に戻ります...")
                        if new_tab_opened:
                            # 新しいタブが開かれていた場合は、タブを閉じて元のタブに戻る
                            driver.close()
                            driver.switch_to.window(original_window_handle)
                            print(f"      → タブを閉じて元のタブに戻りました")
                        else:
                            # 同じタブで開かれた場合は戻るボタンで戻る
                            driver.back()
                        
                        # 一覧画面が読み込まれるまで待機（最適化：より具体的な要素を待つ）
                        try:
                            # 物件カードまたはテーブルが表示されるまで待機（タイムアウトを短縮）
                            WebDriverWait(driver, 5).until(
                                lambda d: len(find_property_cards()) > 0 or len(d.find_elements(By.ID, "tbl")) > 0
                            )
                            # 追加の待機は不要（要素が表示されていればOK）
                        except:
                            # タイムアウトした場合は少し待って再試行
                            try:
                                time.sleep(0.5)
                                cards = find_property_cards()
                                if len(cards) > 0:
                                    pass  # 成功
                                elif driver.find_elements(By.ID, "tbl"):
                                    pass  # 成功
                                else:
                                    print(f"      ⚠️ 一覧画面の読み込みがタイムアウトしました")
                            except:
                                pass
                        
                    except Exception as e:
                        print(f"      ⚠️ 詳細ページ処理エラー: {e}")
                        import traceback
                        traceback.print_exc()
                        # エラーが発生した場合も一覧に戻る
                        try:
                            # 現在のタブハンドルを確認
                            current_handles = driver.window_handles
                            if original_window_handle in current_handles:
                                # 元のタブが存在する場合は戻る
                                if driver.current_window_handle != original_window_handle:
                                    # 新しいタブが開かれていた場合は閉じる
                                    if len(current_handles) > 1:
                                        driver.close()
                                    driver.switch_to.window(original_window_handle)
                                else:
                                    driver.back()
                            else:
                                # 元のタブが見つからない場合は、最初のタブに戻る
                                if len(current_handles) > 0:
                                    driver.switch_to.window(current_handles[0])
                            time.sleep(2)
                        except Exception as e2:
                            print(f"      ⚠️ エラー回復処理も失敗: {e2}")
                            pass
                else:
                    print(f"      ℹ️ 「詳細」ボタンが見つかりませんでした")
                
                # 一覧情報と詳細情報を結合
                # 一覧情報を優先し、詳細情報で補完
                combined_info = {**list_info, **detail_info}
                
                # ヘッダーに合わせてデータを並べる（CSV用）
                row_data = []
                for key in headers:
                    value = combined_info.get(key, '')
                    row_data.append(value)
                
                all_data.append(row_data)
                
                # JSON形式で物件データを構築（UI用）
                property_id = combined_info.get('物件番号', '') or combined_info.get('管理番号', '') or f"property_{len(all_properties)+1}"
                # 物件IDを生成（ハッシュ化して一意性を確保）
                if property_id:
                    property_id_hash = hashlib.md5(property_id.encode('utf-8')).hexdigest()[:8]
                    property_id = f"{property_id}_{property_id_hash}"
                else:
                    property_id = f"property_{len(all_properties)+1}_{int(time.time())}"
                
                # 画像パスを取得
                image_dir = combined_info.get('画像保存パス', '')
                image_paths = []
                if image_dir and os.path.exists(image_dir):
                    # 画像ファイルを取得
                    for file in sorted(os.listdir(image_dir)):
                        if file.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                            # 相対パスで保存（results/YYYYMMDD/images/...）
                            rel_path = os.path.relpath(os.path.join(image_dir, file), os.getcwd()).replace('\\', '/')
                            image_paths.append(rel_path)
                
                # 検索用タグを生成
                tags = []
                if combined_info.get('所在地', ''):
                    # 所在地から都道府県、市区町村を抽出
                    address = combined_info.get('所在地', '')
                    if '都' in address:
                        tags.append('東京都')
                    elif '府' in address:
                        tags.append('大阪府' if '大阪' in address else '京都府')
                    elif '県' in address:
                        # 最初の県名を抽出
                        for prefecture in ['神奈川', '埼玉', '千葉', '愛知', '兵庫', '福岡']:
                            if prefecture in address:
                                tags.append(f'{prefecture}県')
                                break
                
                # 間取りからタグを生成
                if combined_info.get('間取り', ''):
                    layout = combined_info.get('間取り', '')
                    if '1' in layout or 'ワン' in layout:
                        tags.append('1R/1K')
                    elif '2' in layout or 'ツー' in layout:
                        tags.append('2LDK/2DK')
                    elif '3' in layout or 'スリー' in layout:
                        tags.append('3LDK/3DK')
                    elif '4' in layout or 'フォー' in layout:
                        tags.append('4LDK以上')
                
                # 築年月からタグを生成
                if combined_info.get('築年月', ''):
                    chiku = combined_info.get('築年月', '')
                    if '新築' in chiku:
                        tags.append('新築')
                    elif '202' in chiku:
                        tags.append('築浅')
                    elif '201' in chiku or '200' in chiku:
                        tags.append('築10年以内')
                    else:
                        tags.append('築10年以上')
                
                # 物件データオブジェクトを作成
                property_obj = {
                    'id': property_id,
                    'property_number': combined_info.get('物件番号', ''),
                    'management_number': combined_info.get('管理番号', ''),
                    'name': combined_info.get('物件名', ''),
                    'address': combined_info.get('所在地', ''),
                    'rent': combined_info.get('賃料', ''),
                    'management_fee': combined_info.get('管理費等', ''),
                    'layout': combined_info.get('間取り', ''),
                    'area': combined_info.get('専有面積', ''),
                    'built_date': combined_info.get('築年月', ''),
                    'structure': combined_info.get('建物構造', ''),
                    'floor': combined_info.get('階建/階', ''),
                    'access': combined_info.get('交通', ''),
                    'reikin': combined_info.get('礼金', ''),
                    'shikikin': combined_info.get('敷金', ''),
                    'equipment': combined_info.get('設備', ''),
                    'special_notes': combined_info.get('特記事項', ''),
                    'remarks': combined_info.get('備考', ''),
                    'surroundings': combined_info.get('周辺環境', ''),
                    'images': image_paths,
                    'image_count': len(image_paths),
                    'tags': tags,
                    'extracted_at': datetime.now().isoformat(),
                    'search_keywords': [
                        combined_info.get('物件名', ''),
                        combined_info.get('所在地', ''),
                        combined_info.get('間取り', ''),
                        combined_info.get('交通', ''),
                    ]
                }
                
                all_properties.append(property_obj)
                
                # 1件ごとにファイルを更新（最適化：ログ出力を削減、保存処理を軽量化）
                save_data_to_files()
                # ログ出力は10件ごと（パフォーマンス向上）
                if len(all_properties) % 10 == 0 or len(all_properties) == 1:
                    print(f"      ✓ データを保存しました（{len(all_properties)}件）")
            
            # 100件に制限されていることを確認
            actual_count = min(len(property_cards), 100)
            print(f"  ✓ {actual_count}件のデータを取得しました（制限: 100件）")
        
        # 中断チェック
        if interrupted:
            print("\n⚠️ 中断フラグが設定されました。処理を終了します...")
            break
        
        # 次のページがあるか確認
        next_btn = None
        try:
            # 方法1: title='次へ'のリンクを探す
            next_btn = driver.find_element(By.CSS_SELECTOR, "a[title='次へ']")
        except:
            try:
                # 方法2: テキストに「次へ」を含むリンクを探す
                next_btn = driver.find_element(By.XPATH, "//a[contains(text(), '次へ')]")
            except:
                try:
                    # 方法3: ページネーションの「次へ」ボタンを探す
                    next_btn = driver.find_element(By.XPATH, "//a[contains(@href, 'next') or contains(@href, '次')]")
                except:
                    pass
        
        if next_btn:
            # ボタンが無効化されているか確認
            try:
                btn_class = next_btn.get_attribute("class") or ""
                btn_style = next_btn.get_attribute("style") or ""
                if "disabled" in btn_class or "display:none" in btn_style or not next_btn.is_enabled():
                    print("  ℹ️ 最後のページに到達しました")
                    break
            except:
                pass
            
            # 次へボタンをクリック
            try:
                driver.execute_script("arguments[0].click();", next_btn)
                wait_and_accept_alert()
                time.sleep(3)
                page_num += 1
                print(f"  → {page_num}ページ目に移動しました")
            except Exception as e:
                print(f"  ⚠️ 次へボタンのクリックに失敗: {e}")
                print("  ℹ️ 最後のページに到達しました")
                break
        else:
            # 次へボタンが見つからない = 最後のページ
            print("  ℹ️ 最後のページに到達しました")
            break
    
    # ---------------------------------------------------------
    # 8. 最終データを保存（CSVとJSON）
    # ---------------------------------------------------------
    # 既に1件ごとに保存されているが、最終的な保存も実行
    if all_data or all_properties:
        print(f"💾 最終データを保存中...")
        save_data_to_files()
        print(f"✅ 最終データを保存しました")
    
    print("✅ 全ての処理が完了しました。")
    print(f"現在のURL: {driver.current_url}")
    
    try:
        input(">> Enterキーを押すとブラウザを閉じます...")
    except (EOFError, KeyboardInterrupt):
        print(">> ブラウザを閉じます...")

except KeyboardInterrupt:
    print("\n\n⚠️ キーボード割り込み（Ctrl+C）を受信しました。安全に終了します...")
    interrupted = True
    save_data_to_files()
    print("✅ データを保存しました")
    if csv_filename:
        print(f"   CSV: {csv_filename}")
    if json_filename:
        print(f"   JSON: {json_filename}")

except Exception as e:
    import traceback
    print(f"❌ エラー発生: {e}")
    print(f"エラータイプ: {type(e).__name__}")
    print("詳細なトレースバック:")
    traceback.print_exc()
    
    # エラー時もデータを保存
    try:
        save_data_to_files()
        print("✅ エラー発生時もデータを保存しました")
    except:
        pass
    
    # 現在の状態を確認
    try:
        print(f"現在のURL: {driver.current_url}")
        print(f"ページタイトル: {driver.title}")
    except:
        pass
    
    # もしURL直打ちでエラーになる場合は、メッセージを表示
    if "画面遷移エラー" in str(e):
        print("💡 ヒント: URL直打ちで弾かれた可能性があります。その場合は元の『ボタンクリック方式』に戻す必要があります。")

finally:
    # 最終的なデータ保存
    try:
        if all_data or all_properties:
            save_data_to_files()
    except:
        pass
    
    try:
        driver.quit()
    except:
        pass
