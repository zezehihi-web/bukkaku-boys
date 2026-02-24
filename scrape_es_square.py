"""
scrape_es_square.py
===================
いい生活Squareから物件詳細を取得し、既存UI互換のJSON形式で保存する。
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import base64
import requests
from PIL import Image, ImageStat, ImageFilter

from dotenv import load_dotenv

try:
    import pytesseract
    PYTESSERACT_AVAILABLE = True
except Exception:
    pytesseract = None
    PYTESSERACT_AVAILABLE = False

from playwright.sync_api import Page, sync_playwright
try:
    import boto3
    from botocore.client import Config
except Exception:
    boto3 = None
    Config = None

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()

OCR_TEXT_IMAGE_THRESHOLD = int(os.getenv("ES_SQUARE_OCR_TEXT_IMAGE_THRESHOLD", "50"))
RENT_OCR_MIN_YEN = int(os.getenv("ES_SQUARE_RENT_OCR_MIN_YEN", "10000") or "10000")
RENT_OCR_MAX_YEN = int(os.getenv("ES_SQUARE_RENT_OCR_MAX_YEN", "3000000") or "3000000")
RENT_OCR_PRIMARY_LABEL_INDEX = max(1, int(os.getenv("ES_SQUARE_RENT_OCR_PRIMARY_LABEL_INDEX", "3") or "3"))
RENT_OCR_PRIMARY_ONLY = os.getenv("ES_SQUARE_RENT_OCR_PRIMARY_ONLY", "1").lower() in ("1", "true", "yes", "on")
ENABLE_RENT_OCR_FALLBACK = os.getenv("ES_SQUARE_ENABLE_RENT_OCR_FALLBACK", "0").lower() in ("1", "true", "yes", "on")

OUTPUT_DIR = "output"
DATA_DIR = os.path.join(OUTPUT_DIR, "data")
IMAGES_DIR = os.path.join(OUTPUT_DIR, "images")
INDEX_FILE = os.path.join(OUTPUT_DIR, "es_square_index.json")
LEGACY_SHARED_INDEX = os.path.join(OUTPUT_DIR, "properties_index.json")

LOGIN_URL = "https://rent.es-square.net/login"
EMAIL = os.getenv("ES_SQUARE_EMAIL") or os.getenv("ITANJI_EMAIL", "")
PASSWORD = os.getenv("ES_SQUARE_PASSWORD") or os.getenv("ITANJI_PASSWORD", "")

TARGET_MUNICIPALITIES = 15
MAX_PROPERTIES = int(os.getenv("ES_SQUARE_MAX_PROPERTIES", "0") or "0")
HEADLESS = os.getenv("ES_SQUARE_HEADLESS", "").lower() in ("1", "true", "yes", "on")
COMPRESS_IMAGES = os.getenv("ES_SQUARE_COMPRESS_IMAGES", "1").lower() in ("1", "true", "yes", "on")
COMPRESS_MAX_SIDE = int(os.getenv("ES_SQUARE_COMPRESS_MAX_SIDE", "1200") or "1200")
COMPRESS_QUALITY = int(os.getenv("ES_SQUARE_COMPRESS_QUALITY", "78") or "78")
COMPRESS_WORKERS = int(os.getenv("ES_SQUARE_COMPRESS_WORKERS", "8") or "8")
SKIP_EXISTING = os.getenv("ES_SQUARE_SKIP_EXISTING", "1").lower() in ("1", "true", "yes", "on")
CLEANUP_ENDED = os.getenv(
    "ES_SQUARE_CLEANUP_ENDED",
    "1" if MAX_PROPERTIES == 0 else "0",
).lower() in ("1", "true", "yes", "on")
USE_BLOCK_CLEANUP = os.getenv("ES_SQUARE_USE_BLOCK_CLEANUP", "1").lower() in ("1", "true", "yes", "on")
KEEP_ENDED_LOCAL_ARCHIVE = os.getenv("KEEP_ENDED_LOCAL_ARCHIVE", "0").lower() in ("1", "true", "yes", "on")
SAVE_INTERVAL = max(1, int(os.getenv("ES_SQUARE_SAVE_INTERVAL", "1") or "1"))
SYNC_DELETE_ENABLED = os.getenv("ES_SQUARE_SYNC_DELETE_ENABLED", "0").lower() in ("1", "true", "yes", "on")

R2_UPLOAD_ENABLED = os.getenv("R2_UPLOAD_ENABLED", "").lower() in ("1", "true", "yes", "on")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "heyamatch-properties")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else ""

SCRAPED_URLS_FILE = os.path.join(OUTPUT_DIR, "scraped_urls.json")
APPLIED_URLS_FILE = os.path.join(OUTPUT_DIR, "applied_urls.json")

_r2_client = None
all_found_urls: set[str] = set()
applied_urls: set[str] = set()
interrupt_flag = False

# マイソク判定用ワーカー
_image_cleanup_executor = ThreadPoolExecutor(max_workers=4)

# R2バックグラウンドアップロード用
_upload_executor = ThreadPoolExecutor(max_workers=2)
_upload_futures: list = []


def compress_image_file(filepath: str) -> None:
    """
    画像をスマホ表示用にリサイズ・圧縮する（R2アップロード前に実行）。
    - 長辺がCOMPRESS_MAX_SIDE(デフォルト1200px)を超える場合はリサイズ
    - JPEG/WebP品質をCOMPRESS_QUALITY(デフォルト78)に設定
    - スマホ(2x-3xディスプレイ)で荒く見えない品質を維持しつつ容量削減
    """
    try:
        with Image.open(filepath) as img:
            # RGBA/P → RGB 変換（JPEG保存に必要）
            if img.mode in ("RGBA", "P", "LA"):
                img = img.convert("RGB")

            w, h = img.size
            # 長辺がmax_sideを超える場合のみリサイズ
            if max(w, h) > COMPRESS_MAX_SIDE:
                ratio = COMPRESS_MAX_SIDE / max(w, h)
                new_w = int(w * ratio)
                new_h = int(h * ratio)
                img = img.resize((new_w, new_h), Image.LANCZOS)

            # 形式に応じて保存
            ext = os.path.splitext(filepath)[1].lower()
            if ext in (".jpg", ".jpeg"):
                img.save(filepath, "JPEG", quality=COMPRESS_QUALITY, optimize=True)
            elif ext == ".webp":
                img.save(filepath, "WEBP", quality=COMPRESS_QUALITY)
            elif ext == ".png":
                img.save(filepath, "PNG", optimize=True)
            else:
                img.save(filepath, quality=COMPRESS_QUALITY, optimize=True)
    except Exception:
        pass  # 圧縮失敗時は元画像をそのまま使用


def upload_files_to_r2_background(local_paths: list[str]) -> None:
    """R2アップロードをバックグラウンドで実行（次の物件取得をブロックしない）"""
    if not local_paths or not is_r2_ready() or interrupt_flag:
        return
    try:
        paths_copy = list(local_paths)  # 参照が変わっても安全なようにコピー
        future = _upload_executor.submit(upload_files_to_r2, paths_copy)
        _upload_futures.append(future)
    except RuntimeError:
        # executor already shut down
        pass


def wait_all_uploads() -> None:
    """全バックグラウンドアップロードの完了を待つ"""
    global _upload_futures
    # 完了済みを除外
    pending = [f for f in _upload_futures if not f.done()]
    if not pending:
        _upload_futures = []
        return
    print(f"[R2] バックグラウンドアップロード {len(pending)}件の完了を待機中...")
    for f in pending:
        try:
            f.result(timeout=180)
        except Exception as e:
            if "shutdown" not in str(e).lower():
                print(f"[R2] バックグラウンドアップロードエラー: {e}")
    _upload_futures = []
    print("[R2] 全バックグラウンドアップロード完了")


def shutdown_upload_executor():
    """アップロードキューを安全にシャットダウン"""
    try:
        _upload_executor.shutdown(wait=False, cancel_futures=True)
    except TypeError:
        # Python < 3.9 では cancel_futures 未サポート
        _upload_executor.shutdown(wait=False)

def is_maisoku_image(filepath: str) -> bool:
    """
    画像がマイソク（図面チラシ）かどうかを判定する。
    
    判定基準（改訂版）:
    1. 【形状】縦長なら「間取り図」として保護（削除しない）。
    2. 【エッジ密度】横長の場合、マイソクは「文字」「地図」「表」が多く、エッジ密度が極端に高い。
       写真は「空」「壁」「床」など平坦な領域が多く、エッジ密度は中程度。
    3. 【ヒストグラム】マイソクは「白（紙）」と「黒（文字）」に画素が集中する。
    """
    try:
        with Image.open(filepath) as img:
            width, height = img.size
            if width == 0 or height == 0:
                return False
            
            # --- 1. 形状チェック ---
            # 縦長(Height > Width)は「間取り図」として保護
            if height > width:
                return False
            
            # --- 2. 色数チェック (イラスト/図面 vs 写真/スキャン) ---
            # 間取り図（ご提示のようなきれいな画像）は、色数が比較的少ない。
            # マイソクは写真が含まれたり、スキャンノイズで色数が非常に多くなる。
            
            # 処理高速化のため縮小してから色数カウント
            small = img.resize((128, 128)) 
            # getcolorsは色がmaxcolorsを超えるとNoneを返す
            # 閾値を4096色とする（写真は通常これを超える）
            colors = small.getcolors(maxcolors=4096)
            
            if colors:
                # 色数が制限内 = イラストや単純な図面の可能性が高い -> 保護
                return False

            # ここからは「横長」かつ「色数が多い（写真や複雑なチラシ）」画像
            # マイソク(文字だらけ) vs 写真(風景) の勝負
            
            # グレースケール変換
            gray = img.convert("L")
            
            # --- 3. エッジ密度チェック ---
            # FIND_EDGESフィルタで輪郭抽出
            edges = gray.filter(ImageFilter.FIND_EDGES)
            
            # エッジ画像の平均輝度を「エッジ密度スコア」とする
            edge_stat = ImageStat.Stat(edges)
            edge_density = edge_stat.mean[0]
            
            # --- 4. 白画素の割合チェック ---
            # マイソクは背景が白いことが多い
            hist = gray.histogram()
            # 輝度230以上を白とみなす
            white_pixels = sum(hist[230:])
            total_pixels = width * height
            white_ratio = white_pixels / total_pixels
            
            # print(f"[Debug] {os.path.basename(filepath)}: Edge={edge_density:.1f}, White={white_ratio:.2f}")
            
            # 判定基準（閾値を調整）
            # 文字・地図・表がびっしりのマイソクはエッジ密度が非常に高い
            if edge_density > 80: 
                return True
            
            # エッジがそこそこあり、かつ背景がかなり白い場合もマイソク
            if edge_density > 50 and white_ratio > 0.6:
                return True
                
            return False

    except Exception:
        pass
    return False

def background_image_check(filepath: str, property_id: str):
    """保存された画像をチェックし、マイソクなら削除する"""
    if not os.path.exists(filepath):
        return
    
    if is_maisoku_image(filepath):
        try:
            print(f"[Auto-Clean] マイソク除外: {os.path.basename(filepath)}")
            os.remove(filepath)
            # サムネイルなどの整合性は崩れるが、ファイル実体を消すことを優先
        except Exception:
            pass


JP = {
    "name": "\u7269\u4ef6\u540d",
    "building_name": "\u5efa\u7269\u540d",
    "room": "\u90e8\u5c4b\u756a\u53f7",
    "address": "\u6240\u5728\u5730",
    "property_address": "\u7269\u4ef6\u6240\u5728\u5730",
    "access": "\u4ea4\u901a\u6a5f\u95a2",
    "traffic": "\u4ea4\u901a",
    "rent": "\u8cc3\u6599",
    "mgmt": "\u7ba1\u7406\u8cbb",
    "mgmt_all": "\u7ba1\u7406\u8cbb/\u5171\u76ca\u8cbb/\u96d1\u8cbb",
    "common_fee": "\u5171\u76ca\u8cbb",
    "deposit": "\u6577\u91d1",
    "key_money": "\u793c\u91d1",
    "key_money_all": "\u793c\u91d1/\u6a29\u5229\u91d1",
    "layout": "\u9593\u53d6\u308a",
    "layout_detail": "\u9593\u53d6\u308a\u8a73\u7d30",
    "area": "\u5c02\u6709\u9762\u7a4d",
    "floor": "\u6240\u5728\u968e",
    "structure": "\u5efa\u7269\u69cb\u9020",
    "built_date": "\u7bc9\u5e74\u6708",
    "direction": "\u90e8\u5c4b\u5411\u304d",
    "available": "\u5165\u5c45\u6642\u671f",
    "period": "\u671f\u9593",
    "renewal": "\u66f4\u65b0\u6599",
    "insurance": "\u640d\u5bb3\u4fdd\u967a",
    "parking": "\u99d0\u8eca\u5834",
    "ad": "AD",
    "ad_jp": "\u5e83\u544a\u8cbb",
    "tx": "\u53d6\u5f15\u614b\u69d8",
    "contract_type": "\u5951\u7d04\u5f62\u614b",
    "guarantee": "\u4fdd\u8a3c\u4f1a\u793e",
    "remarks": "\u5099\u8003",
    "conditions": "\u5165\u5c45\u6761\u4ef6",
    "viewing_start": "\u5185\u898b\u958b\u59cb\u65e5",
    "viewing_notes": "\u5185\u898b\u6642\u6ce8\u610f\u4e8b\u9805",
    "message": "\u30e1\u30c3\u30bb\u30fc\u30b8",
}


def setup_dirs() -> None:
    for d in (OUTPUT_DIR, DATA_DIR, IMAGES_DIR):
        os.makedirs(d, exist_ok=True)

def migrate_from_shared_index():
    """旧共有 properties_index.json から es_square データを es_square_index.json に移行"""
    if os.path.exists(INDEX_FILE):
        return  # 既に専用ファイルがある
    if not os.path.exists(LEGACY_SHARED_INDEX):
        return
    try:
        with open(LEGACY_SHARED_INDEX, "r", encoding="utf-8") as f:
            all_data = json.load(f)
        if not isinstance(all_data, list):
            return
        my_data = [p for p in all_data if p.get("source") == "es_square"]
        if my_data:
            with open(INDEX_FILE, "w", encoding="utf-8") as f:
                json.dump(my_data, f, ensure_ascii=False, indent=2)
            print(f"[migrate] 旧共有インデックスから {len(my_data)}件 を es_square_index.json に移行しました")
    except Exception as e:
        print(f"[migrate] マイグレーション失敗: {e}")


def signal_handler(sig, frame):
    global interrupt_flag
    interrupt_flag = True
    print("\n[中断] 停止要求を受け付けました。途中保存して終了します...")
    # バックグラウンドR2アップロードを即座にキャンセル
    shutdown_upload_executor()


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\u3000", " ")).strip()


def normalize_listing_title_key(value: str) -> str:
    s = normalize_text(value).lower()
    # 空白/記号揺れを寄せる
    s = re.sub(r"[()（）\\[\\]{}【】<>＜＞「」『』]", " ", s)
    s = re.sub(r"[-‐‑–—―ー_/\\|,，.。:：;；]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def make_block_key(municipalities: list[str]) -> str:
    vals = [normalize_text(x) for x in municipalities if normalize_text(x)]
    vals.sort()
    return " | ".join(vals)


def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", value).strip()
    return cleaned[:120] or "property"


def split_title(title: str) -> tuple[str, str]:
    s = normalize_text(title)
    if not s:
        return "\u7269\u4ef6\u540d\u4e0d\u660e", ""
    m = re.search(r"(.+?)\s+([A-Za-z]?\d+[A-Za-z\-0-9]*)$", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return s, ""


def parse_money_to_yen(raw: str) -> str:
    s = normalize_text(raw)
    if not s:
        return ""
    # OCRで混在しやすい全角数字/記号を正規化
    s = s.translate(str.maketrans("０１２３４５６７８９，．", "0123456789,."))
    s = s.replace(" ", "").replace("\u3000", "")
    s = s.replace("O", "0").replace("o", "0")
    if any(x in s for x in ("\u306a\u3057", "\u7121", "\u4e0d\u8981")):
        return "0"
    m_man = re.search(r"([\d.]+)\s*\u4e07\u5186?", s)
    if m_man:
        return str(int(float(m_man.group(1)) * 10000))
    m_yen = re.search(r"([\d,]+)\s*\u5186", s)
    if m_yen:
        return m_yen.group(1).replace(",", "")
    # OCR向け: 数字のみ（109,000 など）を優先
    m_num_token = re.search(r"\b([\d,]{4,12})\b", s)
    if m_num_token:
        return m_num_token.group(1).replace(",", "")
    m_num = re.search(r"([\d,]+)", s)
    if m_num:
        return m_num.group(1).replace(",", "")
    return ""


def extract_rent_from_listing_item(item_locator, fallback_text: str = "") -> str:
    """Extract rent from list row near the property title/card."""
    try:
        raw = item_locator.evaluate(
            """
            (el) => {
              const row =
                el.closest('[data-testclass="bukkenListItem"]') ||
                el.closest('tr, li, [role="row"], .MuiPaper-root, .MuiListItem-root, .MuiBox-root') ||
                el;

              const isVisible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                const r = node.getBoundingClientRect();
                if (r.width <= 0 || r.height <= 0) return false;
                return true;
              };

              const isTimestamp = (s) => /^\d{13}$/.test(s);
              const moneyRe = /^\d{1,3}(?:,\d{3})+$/;
              const KANJI_CHIN = '\u8cc3';
              const KANJI_KAN = '\u7ba1';
              const WORD_CHINRYO = '\u8cc3\u6599';
              const WORD_YACHIN = '\u5bb6\u8cc3';
              const WORD_EN = '\u5186';

              // 0) Desktop list row (ES Square current UI):
              //    fee container css-57ym5z first child is rent, second is management fee.
              const feeBlock = row.querySelector('div.css-57ym5z');
              if (feeBlock) {
                const firstCol = feeBlock.querySelector(':scope > div:first-child');
                if (firstCol) {
                  const rentNodes = Array.from(firstCol.querySelectorAll('span.css-smu62q, span, div, p'))
                    .filter((n) => isVisible(n));
                  for (const n of rentNodes) {
                    const t = (n.textContent || '').trim();
                    if (!t || isTimestamp(t)) continue;
                    if (moneyRe.test(t)) return t;
                  }
                }
              }

              // 1) Prefer value near '?' label in fee block.
              const labels = Array.from(row.querySelectorAll('div,span,p')).filter(
                (n) => isVisible(n) && (n.textContent || '').trim() === KANJI_CHIN
              );
              for (const label of labels) {
                const roots = [label.parentElement, label.parentElement?.parentElement, row].filter(Boolean);
                for (const root of roots) {
                  const nodes = Array.from(root.querySelectorAll('span,div,p')).filter(isVisible);
                  for (const n of nodes) {
                    const t = (n.textContent || '').trim();
                    if (!t || isTimestamp(t)) continue;
                    if (moneyRe.test(t)) return t;
                  }
                }
              }

              // 2) Fee block fallback ('?' and '?' both present).
              const feeBlocks = Array.from(row.querySelectorAll('div')).filter((d) => {
                if (!isVisible(d)) return false;
                const t = (d.innerText || '').replace(/\s+/g, '');
                return t.includes(KANJI_CHIN) && t.includes(KANJI_KAN) && /\d{1,3}(?:,\d{3})/.test(t);
              });
              for (const fb of feeBlocks) {
                const t = (fb.innerText || '').replace(/\u3000/g, ' ');
                const m = t.match(new RegExp(KANJI_CHIN + "\\s*([0-9,]{3,})\\s*" + WORD_EN));
                if (m) return m[1];
              }

              // 3) Last fallback from row text.
              const text = (row.innerText || '').replace(/\u3000/g, ' ');
              let m = text.match(new RegExp(KANJI_CHIN + "\\s*([0-9,]{3,})\\s*" + WORD_EN));
              if (m) return m[1];
              m = text.match(new RegExp(WORD_CHINRYO + "\\s*[:\\uFF1A]?\\s*([0-9,]{3,})\\s*" + WORD_EN + "?"));
              if (m) return m[1];
              m = text.match(new RegExp(WORD_YACHIN + "\\s*[:\\uFF1A]?\\s*([0-9,]{3,})\\s*" + WORD_EN + "?"));
              if (m) return m[1];
              return '';
            }
            """
        ) or ""
        rent = parse_money_to_yen(raw)
        if rent:
            return rent
    except Exception:
        pass

    txt = normalize_text(fallback_text)
    m = re.search(r"(?:\u8cc3|\u8cc3\u6599|\u5bb6\u8cc3)\s*[:\uff1a]?\s*([0-9]{1,3}(?:,[0-9]{3})+)\s*\u5186?", txt)
    if m:
        return parse_money_to_yen(m.group(1))
    return ""

def _find_tesseract_exe() -> str:
    exe = shutil.which("tesseract")
    if exe:
        return exe
    win_default = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    if os.path.exists(win_default):
        return win_default
    return ""


def _ocr_image_text(img: Image.Image) -> str:
    if PYTESSERACT_AVAILABLE and pytesseract is not None:
        try:
            return pytesseract.image_to_string(img, lang="jpn+eng")
        except Exception:
            pass

    tesseract_exe = _find_tesseract_exe()
    if not tesseract_exe:
        return ""
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            img.save(tmp_path, format="PNG")
            proc = subprocess.run(
                [tesseract_exe, tmp_path, "stdout", "-l", "jpn+eng", "--psm", "6"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            return proc.stdout.decode("utf-8", errors="ignore")
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
    except Exception:
        return ""


def _image_text_length(filepath: str) -> int:
    try:
        with Image.open(filepath) as img:
            text = _ocr_image_text(img)
        if not text:
            return 0
        return len(re.sub(r"\s+", "", text))
    except Exception:
        return 0


def _is_boshu_zumen_text(text: str) -> bool:
    s = normalize_text(text)
    if not s:
        return False

    # 強トリガー: 1つでもあれば削除
    strong_words = [
        "敷金", "礼金", "家賃", "賃料", "管理費", "共益費",
        "株式会社", "有限会社", "手数料", "備考", "保険", "入居", "保証会社", "鍵交換",
    ]
    if any(w in s for w in strong_words):
        return True

    # 要望対応: 上記語の構成文字を1文字でも検知したら削除寄り
    # ノイズ耐性のため最低文字数だけ軽く見る
    trigger_chars = set("敷礼家賃料管理費共益")
    if len(s) >= 8 and any(ch in s for ch in trigger_chars):
        return True

    # 募集図面で出やすく、間取り図では出にくい語
    candidate_words = [
        "取引態様", "現況優先", "図面と現況", "保証会社", "火災保険",
        "仲介手数料", "広告料", "契約期間", "入居申込", "更新料",
        "管理会社", "借家人賠償", "短期解約", "解約予告", "違約金",
        "鍵交換", "24時間", "損害保険", "保証委託料", "初回保証料",
    ]
    hit = sum(1 for w in candidate_words if w in s)
    if hit >= 2:
        return True

    # 7文字以上の文字列があれば除外対象
    long_tokens = re.findall(r"[A-Za-z0-9一-龥ぁ-んァ-ヶー]{7,}", s)
    if long_tokens:
        return True

    return False


def try_rent_from_ocr(page: Page) -> str:
    """賃料が画像（data:image/ の img）で表示されている場合、その画像を取得してOCRで家賃を抽出する。
    まず「賃料」ラベル直後のセル内の img[src^="data:image/"] の base64 を取得し、デコードしてOCR。"""
    if not ENABLE_RENT_OCR_FALLBACK:
        return ""
    tesseract_exe = _find_tesseract_exe()
    if not PYTESSERACT_AVAILABLE and not tesseract_exe:
        print("[OCR-rent] スキップ: pytesseract/tesseract が見つかりません")
        return ""
    import io

    def _normalize_ocr_rent_text(text: str) -> str:
        s = normalize_text(text)
        s = s.translate(str.maketrans("０１２３４５６７８９，．", "0123456789,."))
        # 空白は保持して「100,000 5」のようなOCR崩れを判定に使う
        s = re.sub(r"[ \u3000]+", " ", s).strip()
        s = s.replace("O", "0").replace("o", "0")
        return s

    def _pick_plausible_rent(text: str) -> str:
        s = _normalize_ocr_rent_text(text)
        if not s:
            return ""

        # 1) 10.0万円 / 10万円
        for m in re.finditer(r"(\d+(?:\.\d+)?)\s*万", s):
            try:
                val = int(float(m.group(1)) * 10000)
                if RENT_OCR_MIN_YEN <= val <= RENT_OCR_MAX_YEN:
                    return str(val)
            except Exception:
                continue

        # 2) 100,000円 / 100,000 / 100.000
        # 要望対応: 「100,000 5」のように空白の後ろが円の誤読でも、前半を家賃として採用
        for m in re.finditer(r"(\d{1,3}(?:[,.]\d{3})+)\s*円?", s):
            try:
                val = int(m.group(1).replace(",", "").replace(".", ""))
                if RENT_OCR_MIN_YEN <= val <= RENT_OCR_MAX_YEN:
                    return str(val)
            except Exception:
                continue

        # 3) 100000円 / 100000（非カンマ）
        for m in re.finditer(r"(?<!\d)(\d{4,7})\s*円?(?!\d)", s):
            try:
                val = int(m.group(1))
                if RENT_OCR_MIN_YEN <= val <= RENT_OCR_MAX_YEN:
                    return str(val)
            except Exception:
                continue

        return ""

    def _ocr_rent_from_image(img: Image.Image) -> str:
        # Small rent image (e.g. 72x21) requires upscale + binarization.
        gray = img.convert("L")
        up = gray.resize((gray.width * 3, gray.height * 3), Image.LANCZOS)
        sharp = up.filter(ImageFilter.SHARPEN)
        bw = sharp.point(lambda p: 255 if p > 170 else 0)
        texts = []
        # 速度優先で up/bw を先に試し、ダメなら元画像も試す
        for candidate in (up, bw, img):
            txt = _ocr_image_text(candidate)
            if not txt:
                continue
            texts.append(txt)
            rent = _pick_plausible_rent(txt)
            if rent:
                return rent
        if not texts:
            return ""
        return _pick_plausible_rent(" ".join(texts))

    try:
        print(
            f"[OCR-rent] 開始 (pytesseract={PYTESSERACT_AVAILABLE}, "
            f"tesseract={'yes' if tesseract_exe else 'no'})"
        )
        page.wait_for_timeout(200)

        # Method A: near label ('賃料'/'家賃')
        # Method B: small data:image fallback in active dialog
        candidates = page.evaluate(
            """
            () => {
              const uniq = (arr) => [...new Set((arr || []).filter(Boolean))];
              const roots = [
                ...Array.from(document.querySelectorAll('[role="dialog"]')),
                ...Array.from(document.querySelectorAll('.MuiDialog-root, .MuiDrawer-root, .MuiModal-root')),
              ].filter(el => el && el.offsetParent !== null);
              const root = roots.length ? roots[roots.length - 1] : document.body;

              const labelHits = [];
              const labels = root.querySelectorAll('b, dt, th, span, div, p, label');
              for (const el of labels) {
                const t = (el.textContent || '').replace(/\\s+/g, '').trim();
                if (!(t.includes('賃料') || t.includes('家賃'))) continue;
                const item = el.closest('.MuiGrid-item') || el.closest('tr') || el.closest('div');
                if (!item) continue;
                const targets = [
                  item.nextElementSibling,
                  item.parentElement && item.parentElement.nextElementSibling,
                  item,
                  item.parentElement,
                ].filter(Boolean);
                for (const target of targets) {
                  const imgs = target.querySelectorAll ? target.querySelectorAll('img[src^="data:image/"]') : [];
                  for (const img of imgs) {
                    const src = (img.getAttribute('src') || '').trim();
                    if (src.indexOf('base64,') !== -1) labelHits.push(src);
                  }
                }
              }

              const smallHits = [];
              const imgs = root.querySelectorAll('img[src^="data:image/"]');
              for (const img of imgs) {
                const src = (img.getAttribute('src') || '').trim();
                if (src.indexOf('base64,') === -1) continue;
                const box = img.getBoundingClientRect();
                if (box.width >= 40 && box.width <= 420 && box.height >= 10 && box.height <= 70) {
                  smallHits.push(src);
                }
              }
              return { labelHits: uniq(labelHits), smallHits: uniq(smallHits) };
            }
            """
        )
        if not isinstance(candidates, dict):
            candidates = {}

        for method, urls in (
            ("A:ラベル隣接", candidates.get("labelHits") or []),
            ("B:小画像フォールバック", candidates.get("smallHits") or []),
        ):
            if not urls:
                continue

            indexed_urls = list(enumerate(urls, start=1))
            fallback_indexed: list[tuple[int, str]] = []
            if method == "A:ラベル隣接" and len(urls) >= RENT_OCR_PRIMARY_LABEL_INDEX:
                primary_idx = RENT_OCR_PRIMARY_LABEL_INDEX - 1
                primary = urls[primary_idx]
                if RENT_OCR_PRIMARY_ONLY:
                    indexed_urls = [(primary_idx + 1, primary)]
                    fallback_indexed = [(idx, u) for idx, u in enumerate(urls, start=1) if idx != primary_idx + 1]
                    print(f"[OCR-rent] A:ラベル隣接 固定候補#{RENT_OCR_PRIMARY_LABEL_INDEX}のみ使用")
                else:
                    indexed_urls = [(primary_idx + 1, primary)] + [
                        (idx, u) for idx, u in enumerate(urls, start=1) if idx != primary_idx + 1
                    ]
                    print(f"[OCR-rent] A:ラベル隣接 固定候補#{RENT_OCR_PRIMARY_LABEL_INDEX}を優先")

            print(f"[OCR-rent] {method} 候補: {len(indexed_urls)}件")
            best_rent = ""
            best_idx = -1
            for cand_idx, data_url in indexed_urls[:8]:
                if "base64," not in data_url:
                    continue
                try:
                    b64 = data_url.split("base64,", 1)[1]
                    raw = base64.b64decode(b64)
                    img = Image.open(io.BytesIO(raw))
                    rent = _ocr_rent_from_image(img)
                    if rent:
                        if (not best_rent) or (int(rent) > int(best_rent)):
                            best_rent = rent
                            best_idx = cand_idx
                except Exception:
                    continue

            if (not best_rent) and fallback_indexed:
                print(f"[OCR-rent] A:固定候補失敗のため残り候補を再試行: {len(fallback_indexed)}件")
                for cand_idx, data_url in fallback_indexed[:8]:
                    if "base64," not in data_url:
                        continue
                    try:
                        b64 = data_url.split("base64,", 1)[1]
                        raw = base64.b64decode(b64)
                        img = Image.open(io.BytesIO(raw))
                        rent = _ocr_rent_from_image(img)
                        if rent:
                            if (not best_rent) or (int(rent) > int(best_rent)):
                                best_rent = rent
                                best_idx = cand_idx
                    except Exception:
                        continue

            if best_rent:
                print(f"[OCR-rent] 成功 ({method}#{best_idx}): {best_rent}円")
                return best_rent

        # Method C: clip around label and OCR
        clip = page.evaluate(
            """
            () => {
              const roots = [
                ...Array.from(document.querySelectorAll('[role="dialog"]')),
                ...Array.from(document.querySelectorAll('.MuiDialog-root, .MuiDrawer-root, .MuiModal-root')),
              ].filter(el => el && el.offsetParent !== null);
              const root = roots.length ? roots[roots.length - 1] : document.body;
              const labels = root.querySelectorAll('b, dt, th, span, div, p, label');
              for (const el of labels) {
                const t = (el.textContent || '').replace(/\\s+/g, '').trim();
                if (!(t.includes('賃料') || t.includes('家賃'))) continue;
                const item = el.closest('.MuiGrid-item') || el.closest('tr') || el.parentElement;
                if (!item) continue;
                const cell = item.nextElementSibling || (item.parentElement && item.parentElement.querySelector('td:last-child'));
                const target = cell || item;
                const img = target.querySelector ? target.querySelector('img') : null;
                const box = (img || target).getBoundingClientRect();
                if (box.width < 10 || box.height < 5) continue;
                return {
                  x: Math.max(0, box.x - 4),
                  y: Math.max(0, box.y - 4),
                  width: Math.min(box.width + 8, 500),
                  height: Math.min(box.height + 8, 120)
                };
              }
              return null;
            }
            """
        )
        if not clip or clip.get("width", 0) < 10:
            print("[OCR-rent] 取得失敗: 家賃画像要素が見つかりません")
            return ""
        screenshot = page.screenshot(clip=clip)
        img = Image.open(io.BytesIO(screenshot))
        rent = _ocr_rent_from_image(img)
        if rent:
            print(f"[OCR-rent] 成功 (C:クリップ): {rent}円")
            return rent

        sample = normalize_text(_ocr_image_text(img))[:80]
        print(f"[OCR-rent] 画像は取得済みだが家賃として解釈できず。OCR結果(抜粋): {sample}")
        return ""
    except Exception as e:
        print(f"[OCR-rent] 例外: {type(e).__name__}: {e}")
        return ""


def normalize_url(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        return parsed.path.rstrip("/")
    except Exception:
        return url


def is_r2_ready() -> bool:
    if not R2_UPLOAD_ENABLED:
        return False
    if boto3 is None or Config is None:
        print("[R2] boto3未インストールのためアップロードをスキップします")
        return False
    missing = []
    if not R2_ACCOUNT_ID:
        missing.append("R2_ACCOUNT_ID")
    if not R2_ACCESS_KEY_ID:
        missing.append("R2_ACCESS_KEY_ID")
    if not R2_SECRET_ACCESS_KEY:
        missing.append("R2_SECRET_ACCESS_KEY")
    if missing:
        print(f"[R2] 環境変数が未設定のためアップロードをスキップします: {', '.join(missing)}")
        return False
    return True


def get_r2_client():
    global _r2_client
    if _r2_client is None:
        _r2_client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )
    return _r2_client


def get_content_type(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    if ext in (".jpg", ".jpeg"):
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    if ext == ".gif":
        return "image/gif"
    if ext == ".webp":
        return "image/webp"
    if ext == ".json":
        return "application/json"
    return "application/octet-stream"


def upload_files_to_r2(local_paths: list[str]) -> None:
    if not local_paths or not is_r2_ready():
        return
    client = get_r2_client()
    uploaded = 0
    errors = 0
    for rel_path in local_paths:
        rel_path = rel_path.replace("\\", "/")
        local_file = os.path.join(OUTPUT_DIR, rel_path)
        if not os.path.exists(local_file):
            continue
        content_type = get_content_type(rel_path)
        try:
            client.upload_file(
                local_file,
                R2_BUCKET_NAME,
                rel_path,
                ExtraArgs={"ContentType": content_type},
            )
            uploaded += 1
        except Exception as e:
            print(f"[R2] アップロード失敗: {rel_path} - {e}")
            errors += 1
    print(f"[R2] アップロード完了: {uploaded}件 (エラー {errors}件)")


def list_r2_keys(prefix: str) -> list[str]:
    if not is_r2_ready():
        return []
    keys: list[str] = []
    client = get_r2_client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if key:
                keys.append(key)
    return keys


def delete_r2_keys(keys: list[str]) -> None:
    if not keys or not is_r2_ready():
        return
    client = get_r2_client()
    deleted = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        try:
            client.delete_objects(
                Bucket=R2_BUCKET_NAME,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
            )
            deleted += len(chunk)
        except Exception as e:
            print(f"[R2] 削除失敗: {e}")
    print(f"[R2] 削除完了: {deleted}件")


def remove_local_image_dir(prop_id: str) -> int:
    if not prop_id:
        return 0
    local_dir = os.path.join(IMAGES_DIR, prop_id)
    if not os.path.exists(local_dir):
        return 0
    removed = 0
    for _, _, files in os.walk(local_dir):
        removed += len(files)
    shutil.rmtree(local_dir, ignore_errors=True)
    return removed


SOURCE_ID = "es_square"


def download_r2_index_raw() -> list[dict]:
    """R2上の properties_index.json を取得して全件リストで返す（他スクレイパーのデータも含む）"""
    if not is_r2_ready():
        return []
    try:
        client = get_r2_client()
        import tempfile
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tmp.close()
        client.download_file(R2_BUCKET_NAME, "properties_index.json", tmp.name)
        with open(tmp.name, "r", encoding="utf-8") as f:
            data = json.load(f)
        os.remove(tmp.name)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def download_index_from_r2() -> bool:
    """
    R2からproperties_index.jsonをダウンロードし、このスクレイパー(es_square)の物件のみ
    ローカルのINDEX_FILEに保存する。
    R2上のインデックスには他スクレイパー(itanji等)のデータも含まれるため分離が必要。
    """
    if not is_r2_ready():
        return False
    try:
        all_data = download_r2_index_raw()
        if not all_data:
            print("[R2] properties_index.json のダウンロード: ファイルが空または存在しない")
            return False
        # このスクレイパーの物件のみ抽出
        # sourceが未設定の物件は所属不明のためes_squareとして扱わない（安全側に倒す）
        my_data = [item for item in all_data if item.get("source") == SOURCE_ID]
        setup_dirs()
        with open(INDEX_FILE, "w", encoding="utf-8") as f:
            json.dump(my_data, f, ensure_ascii=False, indent=2)
        print(f"[R2] properties_index.json をR2からダウンロード (全体={len(all_data)}件, {SOURCE_ID}={len(my_data)}件)")
        return True
    except Exception as e:
        print(f"[R2] properties_index.json のダウンロード: {e}")
        return False


def upload_merged_index_to_r2() -> None:
    """
    ローカルのes_squareインデックスとR2上の他スクレイパーのデータをマージして
    properties_index.json をR2にアップロードする。
    これにより、他スクレイパーのデータを上書きしない。
    """
    if not is_r2_ready():
        return
    try:
        # 1. ローカルのes_squareインデックスを読み込み
        my_data: list[dict] = []
        if os.path.exists(INDEX_FILE):
            with open(INDEX_FILE, "r", encoding="utf-8") as f:
                my_data = json.load(f)

        # 2. R2上の他スクレイパーのデータを取得
        all_r2_data = download_r2_index_raw()
        other_data = [item for item in all_r2_data if item.get("source", SOURCE_ID) != SOURCE_ID]

        # 3. マージして重複排除
        def score_entry(p: dict) -> int:
            s = 0
            if isinstance(p.get('local_images'), list) and len(p['local_images']) > 0:
                s += 100 + len(p['local_images'])
            if p.get('thumbnail'):
                s += 50
            ic = p.get('image_count', 0)
            if isinstance(ic, str):
                ic = int(ic) if ic.isdigit() else 0
            s += ic
            if p.get('layout') and str(p['layout']).strip() and str(p['layout']).strip() != '-':
                s += 20
            if p.get('area') and str(p['area']).strip():
                s += 10
            return s

        combined = other_data + my_data
        best_by_id: dict[str, dict] = {}
        for prop in combined:
            pid = prop.get('id')
            if not pid:
                continue
            existing = best_by_id.get(pid)
            if not existing or score_entry(prop) > score_entry(existing):
                best_by_id[pid] = prop
        merged = list(best_by_id.values())
        print(f"[R2] 重複排除: {len(combined)}件 → {len(merged)}件")

        merged_path = os.path.join(OUTPUT_DIR, "_merged_index.json")
        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, separators=(',', ':'))

        client = get_r2_client()
        client.upload_file(
            merged_path,
            R2_BUCKET_NAME,
            "properties_index.json",
            ExtraArgs={"ContentType": "application/json"},
        )
        os.remove(merged_path)
        print(f"[R2] マージインデックスをアップロード (es_square={len(my_data)}件 + 他={len(other_data)}件 = {len(merged)}件)")
    except Exception as e:
        print(f"[R2] マージインデックスアップロード失敗: {e}")


def load_existing_properties() -> dict:
    """
    既存物件データを読み込む。
    R2が有効な場合は常にR2から最新のインデックスをダウンロードする（R2が情報源）。
    ローカルファイルはスクレイピング中の作業コピーとしてのみ使用。
    """
    result = {"by_url": {}, "by_title": {}, "all_ids": set()}
    # R2が有効なら常にR2から最新をダウンロード（ローカルは参照しない）
    if R2_UPLOAD_ENABLED:
        download_index_from_r2()
    if not os.path.exists(INDEX_FILE):
        return result
    try:
        with open(INDEX_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            item_id = item.get("id")
            if item_id:
                result["all_ids"].add(item_id)
            detail_url = item.get("detail_url")
            if detail_url:
                result["by_url"][detail_url] = item
            title_key = item.get("listing_title_key") or normalize_listing_title_key(item.get("title", ""))
            if title_key:
                result["by_title"][title_key] = item
    except Exception:
        pass
    return result


def save_scraping_results() -> None:
    with open(SCRAPED_URLS_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(all_found_urls), f, ensure_ascii=False, indent=2)
    with open(APPLIED_URLS_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(applied_urls), f, ensure_ascii=False, indent=2)
    print(f"[保存] 募集中URL: {len(all_found_urls)}件 -> {SCRAPED_URLS_FILE}")
    print(f"[保存] 申込ありURL: {len(applied_urls)}件 -> {APPLIED_URLS_FILE}")

    if R2_UPLOAD_ENABLED:
        upload_files_to_r2(["scraped_urls.json", "applied_urls.json"])


def cleanup_ended_properties(partial: bool = False) -> None:
    """
    募集終了物件をクリーンアップする。
    partial=True の場合（中断時）: 「申込あり」物件のみ削除（未スキャンページの誤削除を防止）
    partial=False の場合（完了時）: 「申込あり」＋「今回未検出」物件を削除
    """
    if not os.path.exists(INDEX_FILE):
        return
    try:
        with open(INDEX_FILE, "r", encoding="utf-8") as f:
            properties_index = json.load(f)
    except Exception:
        return

    scraped_normalized = {normalize_url(url) for url in all_found_urls}
    applied_normalized = {normalize_url(url) for url in applied_urls}
    active_properties = []
    ended_properties = []
    for prop in properties_index:
        prop_url = prop.get("detail_url", "")
        prop_norm = normalize_url(prop_url)
        reason = ""
        if prop_norm in applied_normalized:
            reason = "申込あり"
        elif not partial and prop_url and prop_norm not in scraped_normalized:
            reason = "今回のスクレイピングで未検出"
        if reason:
            ended_properties.append((prop, reason))
        else:
            active_properties.append(prop)

    if not ended_properties:
        print("[cleanup] 募集終了物件なし")
        return

    backup_dir = os.path.join(OUTPUT_DIR, "backup")
    ended_dir = os.path.join(OUTPUT_DIR, "ended_properties")
    os.makedirs(backup_dir, exist_ok=True)
    if KEEP_ENDED_LOCAL_ARCHIVE:
        os.makedirs(ended_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_FILE, os.path.join(backup_dir, f"properties_index.json.{timestamp}.bak"))

    deleted_local_images = 0
    deleted_detail_json = 0
    r2_delete_keys: set[str] = set()
    ended_details = []

    for prop, reason in ended_properties:
        prop_id = prop.get("id", "")
        if not prop_id:
            continue

        if KEEP_ENDED_LOCAL_ARCHIVE:
            ended_details.append({**prop, "ended_reason": reason, "ended_at": datetime.now().isoformat()})

        # --- ローカル削除（ディレクトリごと確実に削除）---
        deleted_local_images += remove_local_image_dir(prop_id)
        detail_json = os.path.join(DATA_DIR, f"{prop_id}.json")
        if os.path.exists(detail_json):
            try:
                if KEEP_ENDED_LOCAL_ARCHIVE:
                    with open(detail_json, "r", encoding="utf-8") as f:
                        d = json.load(f)
                    d["ended_reason"] = reason
                    d["ended_at"] = datetime.now().isoformat()
                    with open(os.path.join(ended_dir, f"{prop_id}.json"), "w", encoding="utf-8") as f:
                        json.dump(d, f, ensure_ascii=False, indent=2)
                os.remove(detail_json)
                deleted_detail_json += 1
            except Exception:
                pass

        # --- R2削除（prefix型で漏れなく収集）---
        r2_delete_keys.add(f"data/{prop_id}.json")
        for key in list_r2_keys(f"images/{prop_id}/"):
            r2_delete_keys.add(key)

    if KEEP_ENDED_LOCAL_ARCHIVE and ended_details:
        with open(os.path.join(ended_dir, f"ended_{timestamp}.json"), "w", encoding="utf-8") as f:
            json.dump(ended_details, f, ensure_ascii=False, indent=2)

    # ローカルインデックスを更新
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(active_properties, f, ensure_ascii=False, indent=2)

    # R2から一括削除
    if r2_delete_keys:
        delete_r2_keys(sorted(r2_delete_keys))

    # 更新後のインデックスをR2にマージアップロード（他スクレイパーのデータを保持）
    if R2_UPLOAD_ENABLED:
        upload_merged_index_to_r2()

    print(f"[cleanup] 募集終了: {len(ended_properties)}件, ローカル画像削除: {deleted_local_images}枚, JSON削除: {deleted_detail_json}件")


def cleanup_ended_properties_for_block(block_key: str, seen_urls: set[str], seen_title_keys: set[str]) -> None:
    """同一15市区町村ブロック内で前回存在し、今回未出現の物件を募集終了として削除する。"""
    if not block_key or not os.path.exists(INDEX_FILE):
        return
    try:
        with open(INDEX_FILE, "r", encoding="utf-8") as f:
            properties_index = json.load(f)
    except Exception:
        return

    active_properties = []
    ended_properties = []
    for prop in properties_index:
        prop_block = normalize_text(prop.get("search_block_key", ""))
        if prop_block != block_key:
            active_properties.append(prop)
            continue

        prop_norm = normalize_url(prop.get("detail_url", ""))
        prop_title_key = normalize_listing_title_key(prop.get("listing_title_key") or prop.get("title", ""))
        seen = False
        if prop_norm and prop_norm in seen_urls:
            seen = True
        if not seen and prop_title_key and prop_title_key in seen_title_keys:
            seen = True
        if seen:
            active_properties.append(prop)
        else:
            ended_properties.append((prop, "同一15市区町村ブロックで今回未出現"))

    if not ended_properties:
        print(f"[cleanup-block] no ended properties: {block_key}")
        return

    backup_dir = os.path.join(OUTPUT_DIR, "backup")
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_FILE, os.path.join(backup_dir, f"properties_index.json.block_{timestamp}.bak"))

    deleted_local_images = 0
    deleted_detail_json = 0
    r2_delete_keys: set[str] = set()
    for prop, _ in ended_properties:
        prop_id = prop.get("id", "")
        if not prop_id:
            continue
        deleted_local_images += remove_local_image_dir(prop_id)
        detail_json = os.path.join(DATA_DIR, f"{prop_id}.json")
        if os.path.exists(detail_json):
            try:
                os.remove(detail_json)
                deleted_detail_json += 1
            except Exception:
                pass
        r2_delete_keys.add(f"data/{prop_id}.json")
        for key in list_r2_keys(f"images/{prop_id}/"):
            r2_delete_keys.add(key)

    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(active_properties, f, ensure_ascii=False, indent=2)

    if r2_delete_keys:
        delete_r2_keys(sorted(r2_delete_keys))
    if R2_UPLOAD_ENABLED:
        upload_merged_index_to_r2()

    print(f"[cleanup-block] ended={len(ended_properties)} block={block_key} local_images={deleted_local_images} json={deleted_detail_json}")


def sync_local_and_r2() -> None:
    """
    ローカルとR2の最終同期。
    インデックスに存在しない孤立ファイル（残骸）を両方から検出・削除する。

    重要: R2バケットは複数スクレイパー(es_square, itanji)で共有されているため、
    R2からの削除は自スクレイパーのIDに一致するもののみに限定する。
    他スクレイパーのデータは絶対に削除しない。
    """
    if not SYNC_DELETE_ENABLED:
        print("[sync] 安全モード: ES_SQUARE_SYNC_DELETE_ENABLED=1 でないため削除処理をスキップ")
        return

    if not os.path.exists(INDEX_FILE):
        print("[sync] インデックスファイルなし - スキップ")
        return

    try:
        with open(INDEX_FILE, "r", encoding="utf-8") as f:
            index = json.load(f)
    except Exception:
        print("[sync] インデックス読み込みエラー - スキップ")
        return

    # このスクレイパーのアクティブID
    my_active_ids = {item.get("id") for item in index if item.get("id")}

    # R2上の全スクレイパーのアクティブIDも取得（他スクレイパーのデータを保護するため）
    all_r2_data = download_r2_index_raw()
    all_active_ids = {item.get("id") for item in all_r2_data if item.get("id")}
    other_active_ids = all_active_ids - my_active_ids

    orphan_local = 0
    orphan_r2_keys: list[str] = []

    # --- ローカル孤立ファイル削除 ---
    # ローカルはこのスクレイパー専用なので、my_active_idsだけで判定
    # data/ 配下の孤立JSON
    if os.path.exists(DATA_DIR):
        for f in os.listdir(DATA_DIR):
            if f.endswith(".json"):
                fid = f[:-5]
                if fid not in my_active_ids:
                    try:
                        os.remove(os.path.join(DATA_DIR, f))
                        orphan_local += 1
                    except Exception:
                        pass

    # images/ 配下の孤立ディレクトリ
    if os.path.exists(IMAGES_DIR):
        for d in os.listdir(IMAGES_DIR):
            dpath = os.path.join(IMAGES_DIR, d)
            if os.path.isdir(dpath) and d not in my_active_ids:
                shutil.rmtree(dpath, ignore_errors=True)
                orphan_local += 1

    if orphan_local:
        print(f"[sync] ローカル孤立ファイル削除: {orphan_local}件")

    # --- R2孤立ファイル削除（自スクレイパーの孤立のみ） ---
    if is_r2_ready():
        # data/ 配下
        for key in list_r2_keys("data/"):
            basename = key.split("/")[-1]
            if basename.endswith(".json"):
                fid = basename[:-5]
                # 自分のアクティブIDにも他スクレイパーのIDにも無い場合のみ削除
                if fid not in my_active_ids and fid not in other_active_ids:
                    orphan_r2_keys.append(key)

        # images/ 配下
        for key in list_r2_keys("images/"):
            parts = key.split("/")
            if len(parts) >= 2:
                fid = parts[1]
                if fid not in my_active_ids and fid not in other_active_ids:
                    orphan_r2_keys.append(key)

        if orphan_r2_keys:
            print(f"[sync] R2孤立ファイル削除: {len(orphan_r2_keys)}件 (他スクレイパーのデータは保護)")
            delete_r2_keys(orphan_r2_keys)
        else:
            print("[sync] R2孤立ファイルなし")

    # --- 最終レポート ---
    local_json_count = 0
    if os.path.exists(DATA_DIR):
        local_json_count = len([f for f in os.listdir(DATA_DIR) if f.endswith(".json")])
    local_img_dirs = 0
    if os.path.exists(IMAGES_DIR):
        local_img_dirs = len([d for d in os.listdir(IMAGES_DIR) if os.path.isdir(os.path.join(IMAGES_DIR, d))])

    r2_total = len(all_r2_data) if all_r2_data else 0
    print(f"[sync] 最終状態: {SOURCE_ID}インデックス={len(index)}件, R2全体={r2_total}件, ローカルJSON={local_json_count}件, ローカル画像Dir={local_img_dirs}件")
    if other_active_ids:
        print(f"[sync] 他スクレイパーのデータ: {len(other_active_ids)}件 (保護済み)")


def get_label_value_map_from_dialog(page: Page) -> dict[str, str]:
    script = """
    () => {
      const map = {};
      const setPair = (k, v, overwrite) => {
        if (!k) return;
        const key = String(k).replace(/\\s+/g, " ").trim();
        const val = String(v || "").replace(/\\s+/g, " ").trim();
        if (!key || !val) return;
        if (overwrite || !map[key] || (val.length > map[key].length)) map[key] = val;
      };

      // --- ルート要素の検出 ---
      const markers = /物件概要|設備詳細|情報公開日時|保証会社|入居時期/;
      const candidates = [
        ...Array.from(document.querySelectorAll('[role="dialog"]')),
        ...Array.from(document.querySelectorAll('[role="tabpanel"]')),
        ...Array.from(document.querySelectorAll('.MuiDialog-root, .MuiModal-root, .MuiDrawer-root')),
      ];
      let root = candidates.find((el) => markers.test((el.innerText || el.textContent || "")));
      if (!root) {
        const divs = Array.from(document.querySelectorAll("div"));
        root = divs.find((el) => markers.test((el.innerText || el.textContent || "")));
      }
      if (!root) return map;

      // ヘルパー: 値テキストをinnerTextで取得し「地図」テキスト等を除去
      const cleanValue = (el) => {
        if (!el) return "";
        // innerTextを使ってブロック要素間に改行を挿入（textContentは全て結合してしまう）
        let txt = (el.innerText || el.textContent || "").trim();
        return txt;
      };

      // ヘルパー: 住所フィールドから「地図」を除去し、住所と交通情報を分離
      const parseAddressField = (el) => {
        if (!el) return { address: "", stations: [] };
        const children = el.querySelectorAll("div, p, span, a");
        let addressParts = [];
        let stationLines = [];
        
        // まず直接テキストノードから住所を取得
        const directText = Array.from(el.childNodes)
          .filter(n => n.nodeType === 3) // テキストノード
          .map(n => n.textContent.trim())
          .filter(t => t && t !== "地図")
          .join(" ").trim();
        if (directText) addressParts.push(directText);

        // 子要素をスキャン
        children.forEach((child) => {
          const txt = (child.textContent || "").trim();
          if (!txt || txt === "地図") return;
          // 駅情報: 「〇〇駅」と「分」を含む行
          if (txt.includes("駅") && (txt.includes("分") || txt.includes("線"))) {
            // 個別の駅要素（ブロック要素）を抽出
            if (child.tagName === "DIV" || child.tagName === "P" || child.tagName === "SPAN") {
              // 複数駅が連結している可能性がある場合、改行で分割
              const innerLines = (child.innerText || txt).split("\\n").map(s => s.trim()).filter(Boolean);
              innerLines.forEach(line => {
                if (line.includes("駅") && !stationLines.includes(line)) {
                  stationLines.push(line);
                }
              });
            }
          }
        });

        // stationLinesが空の場合、innerText全体から駅情報を正規表現で抽出
        if (stationLines.length === 0) {
          const fullText = (el.innerText || el.textContent || "");
          // パターン: "〇〇線 〇〇駅 徒歩〇分" or "〇〇線〇〇駅徒歩〇分"
          const stationRegex = /([^\\n]*?(?:線|ライン)[^\\n]*?駅[^\\n]*?(?:徒歩|バス)?[^\\n]*?分[^\\n]*)/g;
          let m;
          while ((m = stationRegex.exec(fullText)) !== null) {
            const line = m[1].trim();
            if (line && !stationLines.includes(line)) stationLines.push(line);
          }
        }

        // 住所テキストのクリーンアップ
        let address = addressParts.join(" ").replace(/地図/g, "").trim();
        if (!address) {
          // フォールバック: innerText全体から住所行を取得（駅情報は除外）
          const lines = (el.innerText || "").split("\\n").map(s => s.trim()).filter(Boolean);
          address = lines.find(l => !l.includes("駅") && !l.includes("分") && l !== "地図" && l.length > 3) || "";
        }

        return { address, stations: stationLines };
      };

      // --- 方法1: Grid構造: <b>ラベル</b> + 隣のGridに値 ---
      const addressLabels = ["物件所在地", "所在地", "住所"];
      // 設備カテゴリラベル（方法6で専用処理するためスキップ）
      const facilitySkipLabels = [
        "キッチン", "バス・トイレ・洗面", "位置・フロア", "回線",
        "給湯", "空調・光熱", "室内仕様", "建物設備", "特徴・設備",
        "セキュリティ", "管理・防犯", "屋外設備", "ライフライン",
        "駐車場", "駐輪場", "バイク置き場", "区画設備"
      ];
      root.querySelectorAll("b").forEach((b) => {
        const key = (b.textContent || "").trim();
        if (!key) return;

        // 設備カテゴリは方法6で処理するためスキップ
        if (facilitySkipLabels.some(fl => key === fl || key.includes(fl))) return;

        const item = b.closest(".MuiGrid-item") || b.closest("div");
        if (!item) return;
        const next = item.nextElementSibling;
        if (!next) return;

        // 住所フィールドは特別処理（「地図」除去、駅情報抽出）
        if (addressLabels.some(label => key.includes(label))) {
          const parsed = parseAddressField(next);
          if (parsed.address) setPair(key, parsed.address);
          if (parsed.stations.length > 0) {
            setPair("交通", parsed.stations.join("\\n"));
          }
          return;
        }

        // innerTextを使用（ブロック要素間に改行挿入 → textContentとの違い）
        const val = cleanValue(next);
        setPair(key, val);
      });

      // --- 方法2: dt/dd構造 ---
      root.querySelectorAll("dt").forEach((dt) => {
        const dd = dt.nextElementSibling;
        if (dd && dd.tagName.toLowerCase() === "dd") setPair(dt.textContent, cleanValue(dd));
      });

      // --- 方法3: table構造 ---
      root.querySelectorAll("tr").forEach((tr) => {
        const th = tr.querySelector("th");
        const td = tr.querySelector("td");
        if (th && td) setPair(th.textContent, cleanValue(td));
      });

      // --- 方法4: コロン区切りテキスト（リーフ要素のみ対象） ---
      root.querySelectorAll("div, p, span").forEach((el) => {
        // リーフ要素チェック: 子にdiv/p/span/b等のブロック要素がなければリーフとみなす
        const hasBlockChild = el.querySelector("div, p, span, b, dt, dd, th, td, table, tr");
        if (hasBlockChild) return; // 親要素はスキップ（子要素の結合テキストで誤ったペアが生まれるため）
        
        const txt = (el.textContent || "").trim();
        if (!txt || txt.length > 200) return; // 長すぎるテキストはスキップ
        if (txt.includes("：")) {
          const i = txt.indexOf("：");
          const k = txt.slice(0, i).trim();
          const v = txt.slice(i + 1).trim();
          if (k.length <= 20 && v) setPair(k, v);
        } else if (txt.includes(":") && !txt.includes("http") && !txt.includes("//")) {
          const i = txt.indexOf(":");
          const k = txt.slice(0, i).trim();
          const v = txt.slice(i + 1).trim();
          if (k.length <= 20 && v) setPair(k, v);
        }
      });

      // --- 方法5: 間取り/面積パターン（常に実行、既存値を上書き） ---
      // "2LDK/53.90㎡" または "1K/17.82㎡" のようなラベルなし要素を検出
      const layoutAreaRegex = /^(\\d[SLDKR]+|ワンルーム|1R)\\s*[\\/／]\\s*([\\d.]+)\\s*[㎡m²]/;
      root.querySelectorAll("p, span, div").forEach((el) => {
        const txt = (el.textContent || "").trim();
        if (!txt || txt.length > 50) return;
        const m = txt.match(layoutAreaRegex);
        if (m) {
          setPair("間取り", m[1], true);
          setPair("専有面積", m[2] + "㎡", true);
        }
      });
      // 方法5b: ダイアログ外や別コンテナにある場合のため、document全体で MuiTypography-body1 の「1K/17.82㎡」形式を検索
      document.querySelectorAll("p.MuiTypography-body1, .MuiTypography-body1").forEach((el) => {
        const txt = (el.textContent || "").trim();
        if (!txt || txt.length > 30) return;
        const m = txt.match(layoutAreaRegex);
        if (m) {
          setPair("間取り", m[1], true);
          setPair("専有面積", m[2] + "㎡", true);
        }
      });

      // --- 方法6: 設備詳細の構造化抽出 ---
      // E-Seikatsuの設備セクションはカテゴリヘッダー + アイテムリストの構造
      const facilityCategories = [
        "キッチン", "バス・トイレ・洗面", "位置・フロア", "回線",
        "給湯", "空調・光熱", "室内仕様", "建物設備", "特徴・設備",
        "セキュリティ", "管理・防犯", "屋外設備", "ライフライン",
        "駐車場", "駐輪場", "バイク置き場", "区画設備"
      ];
      root.querySelectorAll("b").forEach((b) => {
        const cat = (b.textContent || "").trim();
        if (!facilityCategories.some(fc => cat.includes(fc))) return;
        const container = b.closest(".MuiGrid-item") || b.closest("div");
        if (!container) return;
        const next = container.nextElementSibling;
        if (!next) return;

        // まずリーフ要素（子にブロック要素がない）から個別アイテムを収集
        const leafItems = [];
        const leafEls = next.querySelectorAll("span, p, div, li");
        leafEls.forEach((el) => {
          // ブロック子要素がなければリーフ
          if (el.querySelector("span, p, div, li, b")) return;
          const txt = (el.textContent || "").trim();
          if (txt && txt !== "-" && txt !== "―" && txt.length > 1) {
            // カテゴリ名そのものは除外
            if (!facilityCategories.some(fc => txt === fc)) {
              leafItems.push(txt);
            }
          }
        });

        if (leafItems.length > 0) {
          setPair(cat, leafItems.join("，"), true);
        } else {
          // フォールバック: innerTextを改行で分割
          const items = (next.innerText || next.textContent || "")
            .split("\\n")
            .map(s => s.trim())
            .filter(s => s && s !== "-" && s !== "―" && s.length > 1
              && !facilityCategories.some(fc => s === fc));
          if (items.length > 0) {
            setPair(cat, items.join("，"), true);
          }
        }
      });

      return map;
    }
    """
    try:
        return page.evaluate(script) or {}
    except Exception:
        return {}


def pick_value(detail_map: dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in detail_map and detail_map[key]:
            return detail_map[key]
        for k, v in detail_map.items():
            if key in k and v:
                return v
    return ""


def close_any_popup(page: Page, wait_ms: int = 0) -> None:
    # 検索条件チップの削除ボタン（X）を誤って押さないよう、
    # ドロワーやダイアログ内の閉じるボタンのみを対象にする。
    # 注意: document.querySelectorAll() では :has-text() 等の Playwright 独自セレクタは使えない。
    # 有効な CSS セレクタのみを使う。
    script = """
    () => {
        // 1. aria-label="close" ボタン（ドロワー/ダイアログ内）
        const containers = document.querySelectorAll(
            '.MuiDrawer-root, .MuiDialog-root, [role="dialog"]'
        );
        for (const c of containers) {
            // close ボタン (aria-label)
            const closeBtn = c.querySelector('button[aria-label="close"]');
            if (closeBtn && closeBtn.offsetParent !== null) {
                closeBtn.click();
                return true;
            }
            // CloseIcon の SVG → 親 button
            const closeIcon = c.querySelector('svg[data-testid="CloseIcon"]');
            if (closeIcon && closeIcon.offsetParent !== null) {
                const btn = closeIcon.closest('button');
                if (btn) { btn.click(); return true; }
                closeIcon.click(); return true;
            }
            // "閉じる" テキストを含むボタン
            const buttons = c.querySelectorAll('button');
            for (const btn of buttons) {
                if (btn.offsetParent === null) continue;
                const txt = (btn.textContent || '').trim();
                if (txt === '閉じる' || txt.includes('閉じる')) {
                    btn.click();
                    return true;
                }
            }
        }
        return false;
    }
    """
    try:
        page.evaluate(script)
    except Exception:
        pass

def close_detail_popup(page: Page) -> None:
    # 詳細モーダルを閉じる。
    close_any_popup(page)
    # 念のためESCも送信（JSクリックが効かない場合への保険）
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    # 閉じたことを短時間で確認（最大500ms）。閉じなくても処理は続行する。
    try:
        page.wait_for_function(
            """
            () => {
                const txt = (document.body && document.body.innerText) || "";
                if (!txt.includes("物件概要")) return true;
                const drawers = document.querySelectorAll('.MuiDrawer-root[role="presentation"], [role="dialog"]');
                for (const d of drawers) {
                    const s = window.getComputedStyle(d);
                    if (s.display !== 'none' && s.visibility !== 'hidden' && d.innerText.includes("物件概要")) return false;
                }
                return true;
            }
            """,
            timeout=500,
            polling=50
        )
    except Exception:
        pass



def _split_lines(value: str, must_include: str = "", limit: int = 0) -> list[str]:
    items: list[str] = []
    for line in re.split(r"[\r\n]+", value or ""):
        s = normalize_text(line)
        if not s or s == "-" or s in items:
            continue
        if must_include and must_include not in s:
            continue
        items.append(s)
        if limit and len(items) >= limit:
            break
    return items


def _collect_facilities(detail_map: dict[str, str], body_text: str) -> list[str]:
    keys = [
        "区画設備", "ライフライン", "位置・フロア", "回線",
        "室内仕様", "建物設備", "特徴・設備", "管理・防犯",
        "セキュリティ", "キッチン", "バス・トイレ・洗面",
        "給湯", "空調・光熱", "屋外設備",
        "駐車場", "駐輪場", "バイク置き場",
    ]
    # カテゴリヘッダー名（設備名ではなくヘッダーのテキスト）
    category_headers = {
        "ライフライン", "位置・フロア", "回線", "室内仕様",
        "建物設備", "特徴・設備", "管理・防犯", "セキュリティ",
        "キッチン", "バス・トイレ・洗面", "給湯", "空調・光熱",
        "屋外設備", "区画設備", "駐車場", "駐輪場", "バイク置き場",
        "位置フロア", "特徴設備", "管理防犯",  # 「・」なし表記対応
    }
    # カテゴリ名を含む正規表現パターン（長い順にソート → 最長一致）
    _sorted_cats = sorted(category_headers, key=len, reverse=True)
    _cat_pattern = re.compile("|".join(re.escape(c) for c in _sorted_cats))

    # インフラ／ステータス系（表示不要）の除外パターン
    _exclude_re = re.compile(
        r"^(電気|ガス|上水道|下水道|水道)(：|:).{0,15}$"
        r"|^(有|無|なし|あり|-|ー|―|–|—)$"
        r"|^.{0,1}$"
        r"|^バイク置き場(：|:)"
        r"|^駐車場(：|:)"
        r"|^駐輪場(：|:)"
        r"|^面積(：|:)"
    )

    out: list[str] = []

    def _add(s: str) -> None:
        """正規化してリストに追加（重複・不正値を除外）"""
        s = normalize_text(s).strip()
        if not s or s in out:
            return
        if s in category_headers:
            return
        if _exclude_re.match(s):
            return
        if len(s) > 30:
            # 30文字超はまだ連結が残っている可能性 → さらに分割を試みる
            _split_and_add(s)
            return
        out.append(s)

    def _split_and_add(text: str) -> None:
        """カテゴリヘッダーが埋め込まれた連結文字列を分割して追加"""
        # カテゴリ名で分割（キャプチャグループで区切り文字も取得）
        segments = _cat_pattern.split(text)
        for seg in segments:
            seg = seg.strip()
            if not seg or seg in category_headers:
                continue
            if _exclude_re.match(seg):
                continue
            # さらにコロン区切りの「ラベル：値」を除去
            seg_clean = re.sub(r"^[^：:]{1,6}(：|:)\s*-?\s*", "", seg).strip()
            if seg_clean and len(seg_clean) > 1 and seg_clean not in out:
                if len(seg_clean) <= 30:
                    out.append(seg_clean)
            elif seg and len(seg) <= 30 and seg not in out:
                out.append(seg)

    for key in keys:
        raw = pick_value(detail_map, key)
        if not raw:
            continue
        # 全角/半角カンマ・改行で分割
        for token in re.split(r"[,、，／\n\r]+", raw):
            token = token.strip()
            if not token:
                continue

            # カテゴリヘッダーが埋め込まれているか？
            if _cat_pattern.search(token) and token not in category_headers:
                _split_and_add(token)
            else:
                _add(token)

    # body_textからキーワードマッチ（既にリストにあれば追加しない）
    for word in [
        "エレベーター", "防犯カメラ", "光ファイバー",
        "オートロック", "宅配ボックス", "インターホン",
        "浴室乾燥", "追い焚き風呂", "システムキッチン",
        "カウンターキッチン", "温水洗浄便座",
        "バス・トイレ別", "室内洗濯機置場",
        "独立洗面台", "フローリング", "エアコン",
        "TVインターホン", "モニター付きインターホン",
    ]:
        if word in body_text and word not in out:
            out.append(word)
    return out


def _guess_ext(url: str, content_type: str) -> str:
    ctype = (content_type or "").lower()
    if "png" in ctype:
        return ".png"
    if "webp" in ctype:
        return ".webp"
    if "gif" in ctype:
        return ".gif"
    path = urlparse(url or "").path.lower()
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        if path.endswith(ext):
            return ext
    return ".jpg"


def download_single_image(args):
    url, property_id, idx = args
    if not url:
        return None

    MIN_IMAGE_SIZE_BYTES = 3000  # 最低3KB以上の画像のみ保存

    # Handle data URIs (base64)
    if str(url).startswith("data:image/"):
        try:
            # data:image/png;base64,......
            header, encoded = str(url).split(",", 1)
            ext = ".jpg"
            if "png" in header: ext = ".png"
            elif "gif" in header: ext = ".gif"
            elif "webp" in header: ext = ".webp"

            data = base64.b64decode(encoded)

            # サイズチェック: 小さすぎる画像はサムネイルの可能性が高いのでスキップ
            if len(data) < MIN_IMAGE_SIZE_BYTES:
                print(f"  [Skip] 画像サイズが小さすぎます ({len(data)} bytes < {MIN_IMAGE_SIZE_BYTES}): {property_id}/{idx+1}")
                return None

            property_dir = os.path.join(IMAGES_DIR, property_id)
            os.makedirs(property_dir, exist_ok=True)
            filename = f"{str(idx + 1).zfill(2)}{ext}"
            filepath = os.path.join(property_dir, filename)

            with open(filepath, "wb") as f:
                f.write(data)

            # 画像圧縮（R2容量節約）
            if COMPRESS_IMAGES:
                compress_image_file(filepath)

            # 1枚目(01)は間取り図として保持したいため自動除外判定をかけない
            if idx != 0:
                _image_cleanup_executor.submit(background_image_check, filepath, property_id)

            return f"images/{property_id}/{filename}"
        except Exception:
            return None

    if str(url).startswith("data:"):
        return None
        
    if str(url).startswith("blob:"):
        # blob: URLはPython側からはダウンロードできないためスキップ
        # (ブラウザ側でbase64化しておく必要がある)
        return None

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=8)
        if response.status_code != 200 or not response.content:
            return None

        # サイズチェック: 小さすぎる画像はサムネイルの可能性が高いのでスキップ
        if len(response.content) < MIN_IMAGE_SIZE_BYTES:
            print(f"  [Skip] 画像サイズが小さすぎます ({len(response.content)} bytes): {property_id}/{idx+1}")
            return None

        ext = _guess_ext(url, response.headers.get("Content-Type", ""))
        property_dir = os.path.join(IMAGES_DIR, property_id)
        os.makedirs(property_dir, exist_ok=True)
        filename = f"{str(idx + 1).zfill(2)}{ext}"
        filepath = os.path.join(property_dir, filename)
        with open(filepath, "wb") as f:
            f.write(response.content)

        # 画像圧縮（R2容量節約）
        if COMPRESS_IMAGES:
            compress_image_file(filepath)

        return f"images/{property_id}/{filename}"
    except Exception:
        return None


def download_images(property_id: str, image_urls: list[str]) -> tuple[list[str], list[str]]:
    property_dir = os.path.join(IMAGES_DIR, property_id)

    # 既存チェックをスキップし、常に最新のURLリストで上書きダウンロードを試みる（不完全なダウンロードからの復帰のため）
    # if os.path.exists(property_dir):
    #     existing_files = [f for f in os.listdir(property_dir) if f.lower().endswith(("jpg", "jpeg", "png", "webp", "gif"))]
    #     if existing_files:
    #         existing = [f"images/{property_id}/{f}" for f in sorted(existing_files)]
    #         return existing, []

    if not image_urls:
        return [], []
    
    print(f"  [Debug] download_images: {len(image_urls)} urls provided")

    # itanji と同様に並列で保存
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(download_single_image, [(u, property_id, i) for i, u in enumerate(image_urls)]))
    local_paths = [r for r in results if r]

    # OCRで文字量を見て図面を除外（50文字以上）
    if not local_paths:
        return [], []

    ocr_available = PYTESSERACT_AVAILABLE or bool(_find_tesseract_exe())
    if not ocr_available:
        print("[OCR] disabled for image filter (pytesseract/tesseract not found)")
        return local_paths, local_paths

    removed_count = 0
    filtered_paths: list[str] = []
    for rel_path in local_paths:
        full_path = os.path.join(OUTPUT_DIR, rel_path)
        if not os.path.exists(full_path):
            continue
        # 1枚目(01.*)は必ず保持（OCR除外対象にしない）
        base = os.path.basename(rel_path).lower()
        if re.match(r"^01\.[a-z0-9]+$", base):
            filtered_paths.append(rel_path)
            continue
        text = ""
        text_len = 0
        try:
            with Image.open(full_path) as img:
                text = _ocr_image_text(img)
            text_len = len(re.sub(r"\s+", "", text)) if text else 0
        except Exception:
            text = ""
            text_len = 0

        if _is_boshu_zumen_text(text) or text_len >= OCR_TEXT_IMAGE_THRESHOLD:
            try:
                os.remove(full_path)
                removed_count += 1
            except Exception:
                filtered_paths.append(rel_path)
            continue
        filtered_paths.append(rel_path)

    if removed_count > 0:
        print(f"[OCR] removed layout-like images: {removed_count} (threshold={OCR_TEXT_IMAGE_THRESHOLD})")

    return filtered_paths, filtered_paths


def login(page: Page) -> bool:
    if not EMAIL or not PASSWORD:
        print("エラー: ES_SQUARE_EMAIL / ES_SQUARE_PASSWORD を .env に設定してください")
        return False

    print("[START] login")
    def find_in_any_frame(selectors: list[str]):
        for frame in page.context.pages[0].frames:
            for selector in selectors:
                try:
                    loc = frame.locator(selector)
                    if loc.count() > 0 and loc.first.is_visible():
                        return loc.first
                except Exception:
                    continue
        # fallback: current page only
        for selector in selectors:
            try:
                loc = page.locator(selector)
                if loc.count() > 0 and loc.first.is_visible():
                    return loc.first
            except Exception:
                continue
        return None

    def submit_auth_form() -> bool:
        email_input = find_in_any_frame(
            [
                "input#username",
                "input[name='username']",
                'input[type="email"]',
                'input[name*="email"]',
                'input[id*="email"]',
                'input[type="text"][autocomplete="email"]',
            ]
        )
        password_input = find_in_any_frame(
            [
                "input#password",
                "input[name='password']",
                'input[type="password"]',
                'input[name*="password"]',
                'input[id*="password"]',
            ]
        )
        if email_input is None or password_input is None:
            return False
        email_input.fill(EMAIL)
        password_input.fill(PASSWORD)
        # ユーザー要望により、パスワード入力後にEnterキー押下で送信する
        try:
            password_input.press("Enter")
        except Exception:
            # Enterで失敗した場合は従来のボタンクリックを試行
            submit = find_in_any_frame(
                [
                    'button:has-text("続ける")',
                    "button[name='action'][value='default']",
                    'button[type="submit"][name="action"][value="default"]',
                    'button[type="submit"]',
                    'input[type="submit"]',
                    'button:has-text("ログイン")',
                    'button:has-text("Sign in")',
                ]
            )
            if submit:
                submit.click()
            else:
                return False
        
        return True

    page.goto(LOGIN_URL, wait_until="load", timeout=90000)
    page.wait_for_timeout(1200)

    # すでにAuth0フォームが表示されている場合はそのまま送信
    if not submit_auth_form():
        login_button = None
        login_button_selectors = [
            'button:has-text("いい生活アカウントでログイン")',
            "button.css-rk6wt",
            "div.css-4rmlxi button",
            "button.MuiButton-contained",
        ]
        for selector in login_button_selectors:
            try:
                loc = page.locator(selector)
                if loc.count() > 0:
                    login_button = loc.first
                    break
            except Exception:
                continue
        if login_button is None:
            print("エラー: ログイン導線ボタンが見つかりません")
            page.screenshot(path=os.path.join(OUTPUT_DIR, "debug_es_no_login_button.png"))
            return False
        login_button.click()
        page.wait_for_timeout(1200)
        if not submit_auth_form():
            print("エラー: ログイン入力欄または送信ボタンが見つかりません")
            page.screenshot(path=os.path.join(OUTPUT_DIR, "debug_es_no_inputs.png"))
            return False

    def wait_logged_in(timeout_ms: int = 40000) -> bool:
        # 1) まずURL遷移待ち
        try:
            page.wait_for_url("**rent.es-square.net/**", timeout=timeout_ms)
        except Exception:
            pass
        # 2) ログイン後URLを直接叩いて確認
        if "rent.es-square.net" not in page.url:
            try:
                page.goto("https://rent.es-square.net/bukken/chintai/search?p=1&items_per_page=10", wait_until="load", timeout=timeout_ms)
            except Exception:
                pass
        return "rent.es-square.net" in page.url and "/bukken/" in page.url

    if not wait_logged_in():
        # Auth0画面に残っている場合は再入力・再送信を1回だけ試行
        submit_auth_form()
        if not wait_logged_in():
            print(f"エラー: ログイン後に遷移できませんでした ({page.url})")
            page.screenshot(path=os.path.join(OUTPUT_DIR, "debug_es_login_not_completed.png"))
            try:
                with open(os.path.join(OUTPUT_DIR, "debug_es_login_not_completed.html"), "w", encoding="utf-8") as f:
                    f.write(page.content())
            except Exception:
                pass
            return False

    close_any_popup(page)
    print(f"[OK] logged in: {page.url}")
    return True


def select_search_conditions(page: Page, skip_count: int = 0) -> tuple[int, list[str]]:
    print(f"[START] set conditions (skip={skip_count})")
    close_any_popup(page)
    SEARCH_PAGE_URL = "https://rent.es-square.net/bukken/chintai/search?p=1&items_per_page=10"

    def ensure_search_ready() -> None:
        # Auth0ログイン画面の誤検知を避けるため、URL + visible要素 + 文言で判定する
        is_auth_url = "auth.es-account.com" in page.url
        auth_form_visible = False
        try:
            auth_form_visible = page.locator('input[name="username"], input#username').first.is_visible()
        except Exception:
            auth_form_visible = False
        auth_text_present = False
        try:
            auth_text_present = page.locator("text=いい生活アカウントにログイン").count() > 0
        except Exception:
            auth_text_present = False

        if is_auth_url or (auth_form_visible and auth_text_present):
            print("[INFO] ログイン画面を検出したため再ログインします")
            if not login(page):
                raise RuntimeError("再ログインに失敗しました")
        try:
            # URLパラメータをリセットするために必ず指定URLへ遷移
            page.goto(SEARCH_PAGE_URL, wait_until="load", timeout=60000)
            page.wait_for_timeout(1200)
        except Exception:
            pass

    ensure_search_ready()

    def click_first(selectors: list[str], step_name: str, wait_ms: int = 50, required: bool = True) -> bool:
        for s in selectors:
            try:
                loc = page.locator(s)
                if loc.count() == 0:
                    continue
                for i in range(loc.count()):
                    target = loc.nth(i)
                    try:
                        if not target.is_visible():
                            continue
                    except Exception:
                        continue
                    try:
                        target.click(timeout=2500)
                    except Exception:
                        try:
                            target.click(timeout=2500, force=True)
                        except Exception:
                            try:
                                target.evaluate("(el) => el.click()")
                            except Exception:
                                continue
                    page.wait_for_timeout(wait_ms)
                    return True
            except Exception:
                continue
        if required:
            raise RuntimeError(f"{step_name} が見つかりません")
        return False

    # Left side menu can remain on a non-search tab; force area search tab first.
    tab_clicked = click_first(
        [
            'div[role="button"]:has-text("エリア/沿線検索")',
            'button:has-text("エリア/沿線検索")',
            'div:has-text("エリア/沿線検索")',
        ],
        "エリア/沿線検索タブ",
        required=False,
    )
    if not tab_clicked:
        print("[INFO] エリア/沿線検索タブは見つからないためスキップ（既に対象画面の想定）")

    area_selectors = [
        '[data-testid="tiikiSentakuSearch"] .css-zbqwv2',
        '[data-testid="tiikiSentakuSearch"]',
        '[data-testid="tiikiSentakuSearch"] button',
        '[data-testid="tiikiSentakuSearch"] div[role="button"]',
        'button:has-text("エリア・沿線を選択")',
        'div:has-text("エリア・沿線を選択")',
        'button:has-text("エリア/沿線を選択")',
        'div:has-text("エリア/沿線を選択")',
        'button:has-text("エリア沿線を選択")',
        'div:has-text("エリア沿線を選択")',
        'button:has-text("エリア")',
        'div[role="button"]:has-text("エリア")',
        'div:has-text("エリア")',
    ]
    area_clicked = click_first(area_selectors, "エリア選択ボタン", wait_ms=100, required=False)
    if not area_clicked:
        # 画面状態が崩れている場合に備えて検索ページへ再遷移して再試行
        ensure_search_ready()
        try:
            page.goto(SEARCH_PAGE_URL, wait_until="load", timeout=60000)
            page.wait_for_timeout(1200)
        except Exception:
            pass
        area_clicked = click_first(area_selectors, "エリア選択ボタン", wait_ms=700, required=False)
    # Hard fallback: click area selector root directly on updated UI
    if not area_clicked:
        try:
            root = page.locator('[data-testid="tiikiSentakuSearch"]').first
            if root.count() > 0:
                try:
                    root.click(timeout=1200, force=True)
                except Exception:
                    root.evaluate(
                        """
                        (el) => {
                          const target = el.querySelector('.css-zbqwv2') || el;
                          ['pointerdown','mousedown','mouseup','click'].forEach((t) =>
                            target.dispatchEvent(new MouseEvent(t, { bubbles: true }))
                          );
                          if (typeof target.click === 'function') target.click();
                        }
                        """
                    )
                page.wait_for_timeout(700)
                area_clicked = True
        except Exception:
            pass

    tokyo = page.locator("span.eds-checkbox__label").filter(has_text=re.compile(r"^\u6771\u4eac\u90fd\s*\("))
    if not area_clicked:
        # すでに市区町村チェックリストが開いていれば続行する
        try:
            tokyo.first.wait_for(state="visible", timeout=1200)
            area_clicked = True
            print("[INFO] エリア選択ボタン未検出だが東京都チェックが見えているため続行")
        except Exception:
            pass
    if not area_clicked:
        page.screenshot(path=os.path.join(OUTPUT_DIR, "debug_es_no_area_button.png"))
        with open(os.path.join(OUTPUT_DIR, "debug_es_no_area_button.html"), "w", encoding="utf-8") as f:
            f.write(page.content())
        raise RuntimeError("エリア選択ボタン が見つかりません")

    # 「エリア・沿線を選択」クリックが効かないケースがあるため、東京都出現まで再試行
    tokyo_visible = False
    for _ in range(4):
        try:
            tokyo.first.wait_for(state="visible", timeout=5000)
            tokyo_visible = True
            break
        except Exception:
            click_first(area_selectors, "エリア選択ボタン(再試行)", wait_ms=900, required=False)
            page.wait_for_timeout(500)
    if not tokyo_visible:
        page.screenshot(path=os.path.join(OUTPUT_DIR, "debug_es_no_tokyo_checkbox.png"))
        with open(os.path.join(OUTPUT_DIR, "debug_es_no_tokyo_checkbox.html"), "w", encoding="utf-8") as f:
            f.write(page.content())
        raise RuntimeError("東京都チェックが見つかりません")
    tokyo.first.click(force=True)
    page.wait_for_timeout(400)

    click_first(
        [
            'button:has-text("市区郡を選択")',
            'button:has-text("市区町村を選択")',
            'button.Button-module_eds-button--outlined__9xgJd:has-text("市区")',
            'div:has-text("市区郡を選択")',
            'div:has-text("市区町村を選択")',
        ],
        "市区郡選択ボタン",
        wait_ms=100,
    )

    labels = page.locator("span.eds-checkbox__label").filter(has_text=re.compile(r"(\u5e02|\u533a|\u753a|\u6751)\s*\("))
    try:
        labels.first.wait_for(state="visible", timeout=6000)
    except Exception:
        page.screenshot(path=os.path.join(OUTPUT_DIR, "debug_es_no_city_checkbox.png"))
        raise RuntimeError("市区郡チェックが表示されません")

    total = labels.count()
    print(f"[INFO] municipality candidates: {total}, skip: {skip_count}")
    
    # 有効な候補のインデックスをリストアップ
    valid_indices = []
    for i in range(total):
        text = normalize_text(labels.nth(i).inner_text())
        if not text:
            continue
        if re.match(r"^\u6771\u4eac\u90fd", text):
            continue
        if not re.search(r"(\u5e02|\u533a|\u753a|\u6751)", text):
            continue
        # 物件数が0の場合はクリックできない（無効化されている）ため除外
        if "(0)" in text or "（0）" in text:
            continue
        valid_indices.append(i)

    # スキップ分を除外して選択対象を決定
    target_indices = valid_indices[skip_count : skip_count + TARGET_MUNICIPALITIES]
    picked = 0
    selected_municipalities: list[str] = []
    
    for i in target_indices:
        try:
            selected_municipalities.append(normalize_text(labels.nth(i).inner_text()))
        except Exception:
            pass
        labels.nth(i).click()
        picked += 1
        page.wait_for_timeout(100)

    print(f"[OK] municipalities selected: {picked}")
    
    if picked == 0:
        return 0, []

    # -------------------------------------------------------------
    # ユーザー要望: 詳細条件を入力 -> 申込ありを除外 -> 画像1枚以上
    # -------------------------------------------------------------
    
    # 1. 「詳細条件を入力」ボタンをクリック
    # ユーザー提示クラス: eds-button Button-module_eds-button--primary__kax0- Button-module_eds-button--small__Knufy Button-module_eds-button--outlined__9xgJd
    click_first(
        [
            'button.eds-button.Button-module_eds-button--outlined__9xgJd:has-text("詳細条件を入力")',
            'button:has-text("詳細条件を入力")',
            'button:has-text("詳細条件")',
        ],
        "詳細条件を入力ボタン",
        wait_ms=1000,
    )

    # パネルが開くのを待つ (申込ありを除外 チェックボックスが見えるまで)
    try:
        page.locator('span.eds-checkbox__label').filter(has_text="申込ありを除外").first.wait_for(state="visible", timeout=5000)
    except Exception:
        print("[WARN] 詳細条件パネルの展開待ちでタイムアウトしました (詳細条件ボタンクリック失敗の可能性)")
        try:
            page.locator('button:has-text("詳細条件を入力")').click(timeout=2000)
            page.wait_for_timeout(1000)
        except Exception:
            pass

    # 2. 「申込ありを除外」にチェック
    #    <span class="eds-checkbox__label">申込ありを除外</span>
    try:
        exclude_applied = page.locator('span.eds-checkbox__label').filter(has_text="申込ありを除外")
        if exclude_applied.count() > 0:
            # 既にチェックが入っていないか確認(親のinputを見るなど)が必要だが、
            # 通常は初期状態では外れていると想定し、クリックする。
            # もしトグルなら状態確認が必要。ここでは単純にクリック。
            exclude_applied.first.click()
            page.wait_for_timeout(300)
    except Exception:
        print("[WARN] 「申込ありを除外」チェックボックスが見つかりません")

    # 3. 「内観あり」にチェック（画像枚数指定の代わり）
    #    <span class="eds-checkbox__label">内観あり</span>
    try:
        has_interior = page.locator('span.eds-checkbox__label').filter(has_text="内観あり")
        if has_interior.count() > 0:
            # 見える位置までスクロールしてからクリック
            try:
                has_interior.first.scroll_into_view_if_needed()
            except Exception:
                pass
            has_interior.first.click()
            page.wait_for_timeout(300)
        else:
            print("[WARN] 「内観あり」チェックボックスが見つかりません")
    except Exception as e:
        print(f"[WARN] 「内観あり」チェックエラー: {e}")

    # -------------------------------------------------------------
    
    # 詳細条件入力パネルが開いているため、ここで「検索」を押す必要がある
    # このパネル内の検索ボタンは、メイン画面の検索ボタンとは異なる可能性がある
    
    # パネル内の検索ボタンを探す
    # ユーザー提示クラス: eds-button Button-module_eds-button--primary__kax0- Button-module_eds-button--small__Knufy Button-module_eds-button--contained__QWPtF Button-module_eds-button--full-width__5YMD-
    
    # NOTE: パネル内のボタンは画面下部に固定されている場合や、スクロールが必要な場合がある。
    # Playwrightのclickは自動スクロールするが、見つからない場合は明示的に探す。
    
    search_btn_selectors = [
        'button.eds-button.Button-module_eds-button--contained__QWPtF.Button-module_eds-button--full-width__5YMD-:has-text("検索")',
        'button.Button-module_eds-button--full-width__5YMD-:has-text("検索")',
        'button:has-text("この条件で検索")',
        'button:has-text("検索する")',
        'button:has-text("検索")',
    ]
    
    # ダイアログ/ドロワー内を優先的に探す
    dialog_search_btn = None
    try:
        dialog = page.locator('[role="dialog"], .MuiDrawer-root, .MuiDialog-root').last
        if dialog.count() > 0:
            for sel in search_btn_selectors:
                btn = dialog.locator(sel)
                if btn.count() > 0 and btn.first.is_visible():
                    dialog_search_btn = btn.first
                    break
    except Exception:
        pass

    if dialog_search_btn:
        dialog_search_btn.click()
        # page.wait_for_timeout(500)
    else:
        click_first(
            search_btn_selectors,
            "検索ボタン(詳細条件)",
            wait_ms=100,
        )

    size_select = page.locator(
        'div[role="button"]:has-text("10件"), div[role="button"]:has-text("件"), .MuiSelect-select:has-text("件")'
    )
    if size_select.count() > 0:
        size_select.first.click()
        page.wait_for_timeout(300)
        option_100 = page.locator('li[data-value="100"], li[role="option"][data-value="100"], li:has-text("100件")')
        if option_100.count() > 0:
            option_100.first.click()
            page.wait_for_timeout(1000)

    close_any_popup(page)
    print("[OK] search done")
    return picked, selected_municipalities


def get_detail_image_urls(page: Page) -> list[str]:
    try:
        urls = page.evaluate(
            """
            () => {
              // 画像取得範囲を広げるため、特定のタブパネルではなくドロワー/ダイアログ全体を対象にする
              let root = document.querySelector('.MuiDrawer-root:last-of-type');
              if (!root) root = document.querySelector('[role="dialog"]:last-of-type');
              if (!root) root = document.querySelector('.MuiDialog-root:last-of-type');
              if (!root) {
                 // フォールバック: テキストで探す
                 const panelCandidates = Array.from(document.querySelectorAll('[role="tabpanel"]'));
                 root = panelCandidates.find((el) => /物件情報|設備情報|メッセージ/.test((el.textContent || "")));
              }
              if (!root) root = document;

              const out = new Set();
              const addUrl = (u) => {
                if (!u) return;
                const s = String(u).trim();
                if (!s || s.startsWith("data:")) return;
                try {
                  const abs = new URL(s, location.href).href;
                  if (/favicon|icon|logo|sprite|googleapis|miibo|mapscript/i.test(abs)) return;
                  if (!/(\.(jpg|jpeg|png|webp|gif)(\?|$)|property|room|photo|image)/i.test(abs)) return;
                  // 広告除外
                  if (/sfa_main_banner|es-service\.net\/onetop/i.test(abs)) return;
                  out.add(abs);
                } catch (_) {}
              };

              // imgタグ
              root.querySelectorAll("img").forEach((img) => {
                // サイズチェック (極端に小さいものは除外)
                if (img.width > 0 && img.width < 50 && img.height > 0 && img.height < 50) return;
                
                addUrl(img.getAttribute("src"));
                addUrl(img.getAttribute("data-src"));
                addUrl(img.getAttribute("data-original"));
                const srcset = img.getAttribute("srcset");
                if (srcset) {
                  const first = srcset.split(",")[0]?.trim().split(" ")[0];
                  addUrl(first);
                }
              });

              // 背景画像
              root.querySelectorAll("[style*='background-image']").forEach((el) => {
                const bg = getComputedStyle(el).backgroundImage || "";
                const m = bg.match(/url\((['"]?)(.*?)\1\)/i);
                if (m && m[2]) addUrl(m[2]);
              });

              return Array.from(out);
            }
            """
        )
        return urls or []
    except Exception:
        return []


def get_detail_image_urls_safe(page: Page) -> list[str]:
    """TargetClosedError安全なget_detail_image_urls呼び出し"""
    try:
        return get_detail_image_urls(page)
    except Exception:
        return []


def get_high_res_images_from_gallery(page: Page) -> list[str]:
    """
    ドロワー内のサムネイルをクリックしてギャラリーを開き、高画質画像を取得する。
    blob:URLの場合はbase64データに変換して返す。
    """
    if interrupt_flag:
        return get_detail_image_urls_safe(page)
    print("  [Gallery] starting gallery scrape...")
    
    # 1. サムネイルを探してクリック
    # ドロワー内の画像で、ある程度の大きさがあるものをサムネイルとみなす
    thumbnail = None
    try:
        # ドロワー要素を取得（ここでの画像のみを対象にする）
        drawer = page.locator('.MuiDrawer-root').last # 最後のドロワーが最前面
        if drawer.count() == 0:
            # ドロワーがない場合は全体から探す（広告画像などが混ざるリスクあり）
            # リスク軽減のため、特定のコンテナやrole="dialog"を探す
            container = page.locator('[role="dialog"], .MuiDialog-root').last
            if container.count() > 0:
                imgs = container.locator('img')
            else:
                # 最後の手段だが、一覧の画像を除外したい
                # 一覧は通常背面の要素にあるため、前面の要素を探すべきだが…
                return get_detail_image_urls(page)
        else:
            imgs = drawer.locator('img')
            
        count = imgs.count()
        for i in range(count):
            img = imgs.nth(i)
            if not img.is_visible():
                continue
            box = img.bounding_box()
            if not box:
                continue
            # アイコン除外: 40x40以上かつ縦横比が極端でないもの
            # さらに、広告バナーなどを除外するために、あまりに横長なものも除外検討
            if box['width'] > 60 and box['height'] > 60:
                # 広告画像（sfa_main_banner.png や www.es-service.net/onetop/...）を除外
                src = img.get_attribute("src") or ""
                alt = img.get_attribute("alt") or ""
                if "sfa_main_banner" in src or "es-service.net/onetop" in src:
                    continue
                if "広告" in alt or "banner" in src.lower():
                    continue

                thumbnail = img
                break
    except Exception:
        pass

    if not thumbnail:
        print("  [Gallery] no thumbnail found, fallback to normal scrape")
        return get_detail_image_urls(page)

    try:
        thumbnail.click(timeout=2000)
        page.wait_for_timeout(1000)
    except Exception:
        print("  [Gallery] failed to click thumbnail")
        return get_detail_image_urls(page)

    # 2. ギャラリーが開いたか確認（全画面オーバーレイなど）
    # シンプルに、クリック前より大きな画像が表示されているか、Dialogが増えたかなどで判定
    
    collected_urls = []
    max_images = 60  # 増やしておく
    
    # ギャラリー内の画像を順次取得
    # "次へ"ボタンを探してループする
    
    next_buttons = [
        'button[aria-label="next"]',
        'button[aria-label="Next"]',
        'button[aria-label="次の画像"]',
        'button[aria-label="Next image"]',
        '[data-testid="ArrowForwardIosIcon"]',
        '[data-testid="NavigateNextIcon"]',
        '.swiper-button-next',
        'button.image-gallery-right-nav',
        'div[class*="swiper-button-next"]',
        # 追加のセレクタ
        '[data-testid="keyboardArrowRight"]',
        'div:has([data-testid="keyboardArrowRight"])',
        'svg[data-testid="keyboardArrowRight"]',
        'div[aria-label="Next slide"]',
        'div[class*="arrow-next"]',
        'svg[data-testid="ArrowForwardIosIcon"]',
        'button[class*="next"]',
    ]

    seen_srcs = set()
    current_src = ""
    
    # ユーザー要望: 画像取得を確実にするため、上限まで回す
    # ただし、ループ検知やボタン消失時は終了
    
    for i in range(max_images):
        # 現在表示中の最大画像を取得
        current_img_data = None
        
        # 画像が変わるまで待つ (Smart Wait)
        # ロード時間を考慮して少し長めに待つ (最大1.5秒)
        for _wait in range(30): # 50ms * 30 = 1.5s max
            try:
                current_img_data = page.evaluate("""
                    async () => {
                        const MIN_IMAGE_SIZE = 300; // 最低300x300以上を要求
                        const MIN_DATA_URI_LENGTH = 5000; // data:imageは最低5KB以上

                        const pickLargestVisible = (imgs) => {
                            const candidates = imgs.filter((img) => {
                                const rect = img.getBoundingClientRect();
                                if (rect.width <= 120 || rect.height <= 120) return false;
                                const style = window.getComputedStyle(img);
                                if (style.opacity === '0' || style.visibility === 'hidden' || style.display === 'none') return false;
                                if (rect.right < 0 || rect.left > window.innerWidth || rect.bottom < 0 || rect.top > window.innerHeight) return false;
                                return true;
                            });
                            if (candidates.length === 0) return null;
                            return candidates.reduce((max, img) => {
                                const r1 = max.getBoundingClientRect();
                                const r2 = img.getBoundingClientRect();
                                return (r1.width * r1.height) > (r2.width * r2.height) ? max : img;
                            });
                        };

                        // 画像が高解像度で読み込まれるまで待機する関数
                        const waitForHighResImage = async (img, maxWait = 2000) => {
                            const startTime = Date.now();
                            while (Date.now() - startTime < maxWait) {
                                // 画像が完全に読み込まれ、かつ十分なサイズがあるかチェック
                                if (img.complete && img.naturalWidth >= MIN_IMAGE_SIZE && img.naturalHeight >= MIN_IMAGE_SIZE) {
                                    return true;
                                }
                                await new Promise(r => setTimeout(r, 100));
                            }
                            return false;
                        };

                        // アクティブスライドの画像を探す
                        const activeImgs = Array.from(
                            document.querySelectorAll('div.swiper-slide-active img, div[class*="swiper-slide-active"] img')
                        );
                        let target = pickLargestVisible(activeImgs);

                        // フォールバック: すべてのスライド内画像
                        if (!target) {
                            const allMainImgs = Array.from(
                                document.querySelectorAll('div.swiper-slide img, div[class*="swiper-slide"] img')
                            );
                            target = pickLargestVisible(allMainImgs);
                        }

                        if (!target) return null;

                        // 高解像度画像が読み込まれるまで待機
                        await waitForHighResImage(target);

                        let src = target.getAttribute('src') || target.getAttribute('data-src') || "";
                        if (!src) return null;

                        // data:image URIの場合、サイズをチェック
                        if (src.startsWith('data:image/')) {
                            if (src.length < MIN_DATA_URI_LENGTH) {
                                // サムネイルの可能性が高い、srcsetやdata-srcから高解像度版を探す
                                const srcset = target.getAttribute('srcset');
                                if (srcset) {
                                    const urls = srcset.split(',').map(s => s.trim().split(' ')[0]);
                                    const highRes = urls.find(u => !u.startsWith('data:') && u.length > 10);
                                    if (highRes) src = highRes;
                                }
                            }
                        }

                        if (src.startsWith('blob:')) {
                            try {
                                const response = await fetch(src);
                                const blob = await response.blob();
                                // blobサイズが小さすぎる場合はスキップ
                                if (blob.size < 5000) return null;
                                return await new Promise((resolve) => {
                                    const reader = new FileReader();
                                    reader.onloadend = () => resolve(reader.result);
                                    reader.onerror = () => resolve(null);
                                    reader.readAsDataURL(blob);
                                });
                            } catch (e) {
                                return null;
                            }
                        }

                        // 最終チェック: data:imageが小さすぎる場合はnull
                        if (src.startsWith('data:image/') && src.length < MIN_DATA_URI_LENGTH) {
                            return null;
                        }

                        return src;
                    }
                """)
                
                # 初回は待たなくていい。2回目以降は前回と違う画像になるまで待つ。
                if i == 0 and current_img_data:
                    break
                if current_img_data and current_img_data != current_src:
                    break
                
                page.wait_for_timeout(50) # ポーリング間隔
            except Exception as _gallery_err:
                if "TargetClosed" in type(_gallery_err).__name__ or "closed" in str(_gallery_err).lower():
                    print("  [Gallery] browser closed, returning collected images")
                    return collected_urls if collected_urls else get_detail_image_urls_safe(page)
                try:
                    page.wait_for_timeout(50)
                except Exception:
                    return collected_urls if collected_urls else []

        # 取得した画像を保存
        if current_img_data:
            if current_img_data in seen_srcs:
                # すでに取得済みの画像（直前も含む）なら終了
                # print(f"  [Gallery] end of images or loop detected. src={current_img_data[:30]}...")
                break
            else:
                if not current_img_data.startswith("data:image/svg"): # SVGアイコンを除外
                    collected_urls.append(current_img_data)
                    seen_srcs.add(current_img_data)
                    current_src = current_img_data
        else:
            print("  [Gallery] no image found")
            break
        
        # 中断チェック
        if interrupt_flag:
            break

        # 次へボタンをクリック
        clicked_next = False
        for selector in next_buttons:
            try:
                # 複数ある場合はvisibleなものを
                loc = page.locator(selector)
                cnt = loc.count()
                for k in range(cnt):
                    btn = loc.nth(k)
                    if btn.is_visible():
                        btn.click(timeout=100) # timeout短縮
                        clicked_next = True
                        page.wait_for_timeout(100)
                        break
                if clicked_next:
                    break
            except Exception as _nav_err:
                if "TargetClosed" in type(_nav_err).__name__ or "closed" in str(_nav_err).lower():
                    return collected_urls if collected_urls else []
                continue
        
        if not clicked_next:
            # ボタンが見つからない場合、キーボード操作(右矢印)を試す
            try:
                page.keyboard.press("ArrowRight")
                page.wait_for_timeout(100) # 短縮
                clicked_next = True 
            except Exception as _key_err:
                if "TargetClosed" in type(_key_err).__name__ or "closed" in str(_key_err).lower():
                    return collected_urls if collected_urls else []
        
        if not clicked_next:
            # それでもダメなら終了
            break # Loop終了

    # ギャラリーを閉じる (Escで閉じるのが確実)
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(200)
        # まだ閉じてなければボタンクリック試行
        close_any_popup(page)
    except Exception:
        pass
        
    # フォールバックとしてドロワー内の全画像も取得してマージする（ユーザー要望により廃止）
    # fallback_urls = get_detail_image_urls(page)
    # final_urls = sorted(list(set(collected_urls + fallback_urls)))
    
    print(f"  [Gallery] collected {len(collected_urls)} images")
    return collected_urls


def extract_property_from_open_dialog(
    page: Page,
    fallback_title: str,
    idx: int,
    known_url: str = "",
    listing_rent: str = "",
) -> dict:
    body_text = normalize_text(
        page.evaluate(
            """
            () => {
              const markers = /物件概要|設備詳細|情報公開日時|保証会社|入居時期/;
              const candidates = [
                ...Array.from(document.querySelectorAll('[role="dialog"]')),
                ...Array.from(document.querySelectorAll('[role="tabpanel"]')),
                ...Array.from(document.querySelectorAll('.MuiDialog-root, .MuiModal-root, .MuiDrawer-root')),
              ];
              let root = candidates.find((el) => markers.test((el.innerText || el.textContent || "")));
              if (!root) {
                const divs = Array.from(document.querySelectorAll("div"));
                root = divs.find((el) => markers.test((el.innerText || el.textContent || "")));
              }
              return (root && root.innerText) ? root.innerText : "";
            }
            """
        )
    )
    detail_map = get_label_value_map_from_dialog(page) or {}

    title = normalize_text(fallback_title) or normalize_text(
        pick_value(detail_map, JP["name"], JP["building_name"], JP["room"])
    ) or f"\u7269\u4ef6{idx + 1}"
    building_name, room_number = split_title(title)

    # --- 賃料 ---
    rent = parse_money_to_yen(listing_rent)
    if not rent:
        rent = parse_money_to_yen(pick_value(detail_map, JP["rent"]))
    if not rent:
        m_rent = re.search(r"賃料\s*([0-9,]+)\s*円", body_text)
        if m_rent:
            rent = m_rent.group(1).replace(",", "")
    if not rent:
        # 万円表記のフォールバック
        m_rent_man = re.search(r"賃料\s*([\d.]+)\s*万", body_text)
        if m_rent_man:
            rent = str(int(float(m_rent_man.group(1)) * 10000))
    if not rent and ENABLE_RENT_OCR_FALLBACK:
        # 家賃が画像で表示されている場合のOCRフォールバック（明示ON時のみ）
        rent = try_rent_from_ocr(page)
    if rent:
        print(f"[家賃] 取得OK: {rent}円")
    else:
        has_rent_in_map = bool(pick_value(detail_map, JP["rent"]))
        has_rent_text = bool(re.search(r"賃料|家賃", body_text))
        print(
            "[家賃] 取得NG: 家賃なし "
            f"(detail_map={'yes' if has_rent_in_map else 'no'}, "
            f"body_text={'yes' if has_rent_text else 'no'}, ocr=no_hit)"
        )
    management_fee = parse_money_to_yen(pick_value(detail_map, JP["mgmt_all"], JP["mgmt"], JP["common_fee"]))
    deposit = pick_value(detail_map, JP["deposit"])
    key_money_raw = pick_value(detail_map, JP["key_money_all"], JP["key_money"])
    # 「1ヶ月/-」のような余分な部分を除去
    key_money = re.split(r"[/／]", key_money_raw)[0].strip() if key_money_raw else ""

    # --- 間取り ---
    layout_src = pick_value(detail_map, JP["layout"], JP["layout_detail"])
    layout = ""
    # まずlayout_srcから正確なパターンを検索
    m_layout = re.search(r"(\d[SLDKR]+|ワンルーム|1R)", layout_src)
    if m_layout:
        layout = m_layout.group(1)
    # body_textフォールバック: "2LDK/53.90㎡" パターンからlayoutとareaを抽出
    if not layout:
        m_layout_area = re.search(r"(\d[SLDKR]+|ワンルーム|1R)\s*[\/／]\s*([\d.]+)\s*[㎡m²]", body_text)
        if m_layout_area:
            layout = m_layout_area.group(1)
    if not layout:
        m_layout_body = re.search(r"(\d[SLDKR]+|ワンルーム|1R)", body_text[:3000])
        if m_layout_body:
            layout = m_layout_body.group(1)

    # --- 専有面積 ---
    area_src = pick_value(detail_map, JP["area"], "\u9762\u7a4d")
    area = ""
    m_area_src = re.search(r"([\d.]+)", area_src)
    if m_area_src:
        area = m_area_src.group(1)
    if not area:
        m_area_combo = re.search(r"(?:\d[SLDKR]+|ワンルーム|1R)\s*[\/／]\s*([\d.]+)\s*[㎡m²]", body_text)
        if m_area_combo:
            area = m_area_combo.group(1)
        else:
            m_area = re.search(r"([\d.]+)\s*[㎡m²]", body_text)
            if m_area:
                area = m_area.group(1)

    # --- 駅情報 ---
    # 交通キーから取得（get_label_value_map_from_dialogで住所から分離済み）
    stations = _split_lines(pick_value(detail_map, JP["access"], JP["traffic"], "\u30a2\u30af\u30bb\u30b9", "\u4ea4\u901a"), must_include="\u99c5", limit=3)
    if not stations:
        # body_textフォールバック: 駅を含む行を抽出
        station_lines = [s for s in _split_lines(body_text) if ("\u99c5" in s and ("\u5f92\u6b69" in s or "\u5206" in s))]
        # 長い連結文字列から個別の駅情報を分割
        split_stations = []
        for sl in station_lines:
            # 「〇〇線」で始まる各駅情報を分割
            parts = re.split(r"(?=(?:JR|東京メトロ|都営|京成|京急|東急|小田急|西武|東武|京王|相鉄|りんかい|ゆりかもめ|つくば|日暮里|新京成|北総|芝山|成田|常磐|総武|中央|山手|埼京|湘南|横須賀|南武|横浜|武蔵野|青梅|五日市|八高|京浜東北|根岸|高崎|宇都宮|東海道|上野東京))", sl)
            for p in parts:
                p = p.strip()
                if p and "\u99c5" in p and p not in split_stations:
                    split_stations.append(p)
        stations = split_stations[:3] if split_stations else station_lines[:3]

    # --- 広告費 ---
    ad_fee = pick_value(detail_map, JP["ad"], JP["ad_jp"])
    if not ad_fee:
        m_ad = re.search(r"\bAD\b\s*([0-9]+(?:\.[0-9]+)?\s*[％%]?)", body_text, re.IGNORECASE)
        if m_ad:
            ad_fee = normalize_text(m_ad.group(1)).replace("％", "%")

    # ギャラリーを開いて高画質画像を取得
    image_urls = get_high_res_images_from_gallery(page)
    # image_urls = get_detail_image_urls(page) # 旧ロジック
    
    facilities = _collect_facilities(detail_map, body_text)

    now = datetime.now().isoformat()
    item_id = sanitize_filename(f"{building_name}_{room_number or idx + 1}")

    # --- 住所のクリーンアップ ---
    raw_address = pick_value(detail_map, JP["property_address"], JP["address"], "\u4f4f\u6240")
    # 「地図」テキスト除去
    clean_address = re.sub(r"\s*地図\s*", "", raw_address).strip()
    # 住所に駅情報が混入している場合の除去（「〇〇線」「〇〇駅」パターン以降を削除）
    m_addr_station = re.search(r"(.*?(?:丁目[\d\-]*|番地?[\d\-]*|号[\d\-]*|\d[\-\d]*))\s*(?:JR|東京メトロ|都営|京成|京急|東急|小田急|西武|東武|京王|相鉄|りんかい|ゆりかもめ|つくば|常磐|総武)", clean_address)
    if m_addr_station and len(m_addr_station.group(1)) > 5:
        clean_address = m_addr_station.group(1).strip()

    return {
        "title": title,
        "building_name": building_name,
        "room_number": room_number,
        "address": clean_address,
        "stations": stations,
        "rent": rent,
        "management_fee": management_fee,
        "deposit": deposit,
        "key_money": key_money,
        "renewal_fee": pick_value(detail_map, JP["renewal"], "\u66f4\u65b0\u6599"),
        "insurance": pick_value(detail_map, JP["insurance"], "\u706b\u707d\u4fdd\u967a", "\u4fdd\u967a"),
        "layout": layout or pick_value(detail_map, JP["layout"], JP["layout_detail"]),
        "area": area,
        "built_date": pick_value(detail_map, JP["built_date"], "\u7bc9\u5e74", "\u5efa\u7bc9\u5e74\u6708"),
        "floor": pick_value(detail_map, JP["floor"], "\u968e", "\u6240\u5728\u968e"),
        "structure": pick_value(detail_map, JP["structure"], "\u69cb\u9020"),
        "direction": pick_value(detail_map, JP["direction"], "\u5411\u304d", "\u65b9\u4f4d"),
        "available_date": pick_value(detail_map, JP["available"], "\u5165\u5c45\u53ef\u80fd\u65e5", "\u5165\u5c45\u53ef\u80fd\u6642\u671f"),
        "contract_period": pick_value(detail_map, JP["period"], "\u5951\u7d04\u671f\u9593", "\u8cc3\u8cb8\u501f\u5951\u7d04\u671f\u9593"),
        "parking": pick_value(detail_map, JP["parking"], "\u533a\u753b\u8a2d\u5099"),
        "facilities": facilities,
        "ad_fee": ad_fee,
        "transaction_type": pick_value(detail_map, JP["tx"], JP["contract_type"], "\u5143\u4ed8", "\u53d6\u5f15\u5f62\u614b"),
        "guarantee_company": pick_value(detail_map, JP["guarantee"], "\u4fdd\u8a3c"),
        "remarks": pick_value(detail_map, JP["remarks"]) or body_text[:500],
        "preferred_conditions": pick_value(detail_map, JP["conditions"], "\u5e0c\u671b\u6761\u4ef6"),
        "viewing_start_date": pick_value(detail_map, JP["viewing_start"]),
        "viewing_notes": pick_value(detail_map, JP["viewing_notes"], JP["message"]),
        "image_urls": image_urls,
        "image_count": len(image_urls),
        "detail_url": known_url if known_url else (page.url if "/search" not in page.url else ""),
        "raw_details": detail_map,
        "scraped_at": now,
        "id": item_id,
        "local_images": [],
        "updated_at": now,
        "original_scraped_at": now,
    }


def scrape_property_list(page: Page, existing_data: dict | None = None, block_key: str = "") -> tuple[list[dict], list[str], set[str], set[str]]:
    print("[START] collect list")
    properties: list[dict] = []
    new_upload_paths: list[str] = []
    pending_properties: list[dict] = []
    pending_upload_paths: list[str] = []
    used_ids: set[str] = set()
    existing_normalized_urls: set[str] = set()
    existing_title_keys: set[str] = set()
    seen_normalized_urls: set[str] = set()
    seen_title_keys: set[str] = set()
    if existing_data and existing_data.get("by_url"):
        # 検索ページURLなどが混入していると誤検知するため除外
        existing_normalized_urls = {
            normalize_url(u) for u in existing_data["by_url"].keys() 
            if u and "/search" not in u and "p=" not in u
        }
    if existing_data and existing_data.get("by_title"):
        existing_title_keys = set(existing_data["by_title"].keys())

    def flush_pending(reason: str) -> None:
        nonlocal pending_properties, pending_upload_paths
        if not pending_properties and not pending_upload_paths:
            return
        if pending_properties:
            print(f"[CHECKPOINT] save {len(pending_properties)}件 ({reason})")
            save_results(pending_properties)
            pending_properties = []
        if pending_upload_paths:
            # 画像のR2アップロードはバックグラウンドで実行（次の物件取得をブロックしない）
            upload_files_to_r2_background(pending_upload_paths)
            pending_upload_paths = []
        save_scraping_results()

    def wait_detail_open(timeout_ms: int = 3000) -> bool:
        try:
            page.wait_for_function(
                """
                () => {
                  const txt = (document.body && document.body.innerText) || "";
                  return txt.includes("物件概要") && txt.includes("設備詳細");
                }
                """,
                timeout=timeout_ms,
                polling=20
            )
            return True
        except Exception:
            return False

    def wait_detail_close(timeout_ms: int = 2000) -> bool:
        try:
            page.wait_for_function(
                """
                () => {
                  const txt = (document.body && document.body.innerText) || "";
                  return !txt.includes("物件概要") || !txt.includes("設備詳細");
                }
                """,
                timeout=timeout_ms,
                polling=20
            )
            return True
        except Exception:
            return False

    title_selectors = [
        "div[data-testclass='bukkenListItem'] p.MuiTypography-root.MuiTypography-body1.css-1bkh2wx",
        "div[data-testclass='bukkenListItem'] p.css-1bkh2wx",
        "div[data-testclass='bukkenListItem'] p.MuiTypography-root.MuiTypography-body1",
        "div[data-testclass='bukkenListItem'] a[href] p",
        # fallback (legacy DOM)
        "p.MuiTypography-root.MuiTypography-body1.css-1bkh2wx",
        "div.css-1affg1x",
        "div[class*='css-1affg1x']",
        "p.MuiTypography-root.MuiTypography-body1",
        "p[class*='MuiTypography-body1']",
        "a[href] p",
    ]

    title_locator = None
    selected_title_selector = ""
    for selector in title_selectors:
        loc = page.locator(selector)
        if loc.count() > 0:
            title_locator = loc
            selected_title_selector = selector
            break

    total_processed = 0

    while True:
        # リスト要素を再取得
        title_locator = None
        selected_title_selector = ""
        for selector in title_selectors:
            loc = page.locator(selector)
            if loc.count() > 0:
                title_locator = loc
                selected_title_selector = selector
                break
        
        if title_locator is None:
            # ページ読み込み遅延の可能性、少し待って再試行
            page.wait_for_timeout(2000)
            for selector in title_selectors:
                loc = page.locator(selector)
                if loc.count() > 0:
                    title_locator = loc
                    selected_title_selector = selector
                    break
            
        if title_locator is None:
            print("エラー: 物件タイトル要素が見つかりません (リスト終了の可能性)")
            break

        current_page_count = title_locator.count()
        print(f"[INFO] current page count: {current_page_count}")
        
        if current_page_count == 0:
            break

        # ページ内ループ
        for i in range(current_page_count):
            if interrupt_flag:
                break
            
            if MAX_PROPERTIES > 0 and total_processed >= MAX_PROPERTIES:
                print(f"[STOP] Max properties reached: {total_processed}")
                break

            # ... (中略: 物件クリック処理など) ...
            
            # 既存コードのループ中身をここに配置したいが、インデントが深くなるため
            # 構造を維持しつつ、i はページ内インデックスとして扱う
            
            # --- ここから既存のスクレイピングロジック ---
            current = page.locator(selected_title_selector)
            if current.count() <= i:
                continue

            title_text = normalize_text(current.nth(i).inner_text())
            listing_title_key = normalize_listing_title_key(title_text)
            if listing_title_key:
                seen_title_keys.add(listing_title_key)
            row_text = ""
            try:
                row_text = normalize_text(
                    current.nth(i).evaluate(
                        """
                        (el) => {
                          const row = el.closest('tr, li, [role="row"], .MuiPaper-root, .MuiListItem-root, .MuiBox-root');
                          return (row && row.innerText) || el.innerText || "";
                        }
                        """
                    )
                )
            except Exception:
                row_text = title_text

            listing_rent = extract_rent_from_listing_item(current.nth(i), row_text)
            if listing_rent:
                print(f"[家賃] 一覧取得OK: {listing_rent}円 ({title_text})")
            
            # 詳細URLを先に取得してスキップ判定
            # 祖先要素・親行も含めてリンクを探索する（title要素の子要素だけでは不十分）
            current_detail_url = ""
            current_detail_url_raw = ""
            try:
                current_detail_url_raw = current.nth(i).evaluate("""
                    (el) => {
                        // 1. 子要素内のリンク
                        const childLink = el.querySelector('a[href*="/bukken/"]');
                        if (childLink) return childLink.href;
                        // 2. 自身がリンク
                        if (el.tagName === 'A' && (el.href || '').includes('/bukken/')) return el.href;
                        // 3. 祖先のリンク
                        let node = el.parentElement;
                        while (node && node !== document.body) {
                            if (node.tagName === 'A' && (node.href || '').includes('/bukken/')) return node.href;
                            node = node.parentElement;
                        }
                        // 4. 最寄りのコンテナ行内のリンク
                        const row = el.closest('tr, li, [role="row"], .MuiPaper-root, .MuiListItem-root, .MuiBox-root, .MuiButtonBase-root');
                        if (row) {
                            const rowLink = row.querySelector('a[href*="/bukken/"]');
                            if (rowLink) return rowLink.href;
                        }
                        return '';
                    }
                """) or ""
                if current_detail_url_raw:
                    current_detail_url = normalize_url(current_detail_url_raw)
            except Exception:
                pass
            
            # 一覧で見つかったURLは常にall_found_urlsに記録（スキップ時も。削除判定に必須）
            if current_detail_url_raw:
                all_found_urls.add(current_detail_url_raw)
                seen_normalized_urls.add(normalize_url(current_detail_url_raw))

            # 一覧タイトルで既存判定できる場合は詳細を開かずにスキップ
            if SKIP_EXISTING and listing_title_key and listing_title_key in existing_title_keys:
                print(f"[SKIP-LIST] existing title: {title_text}")
                continue

            if SKIP_EXISTING and current_detail_url and current_detail_url in existing_normalized_urls:
                print(f"[SKIP-FAST] existing: {title_text}")
                continue

            clicked = False
            # 高速化: JSで即時クリック
            try:
                current.nth(i).evaluate("el => el.click()")
                clicked = True
            except Exception:
                # 失敗したら従来のクリック
                try:
                    current.nth(i).click(timeout=1000, force=True)
                    clicked = True
                except Exception:
                    pass

            if not clicked:
                try:
                    current.nth(i).evaluate(
                        """
                        (el) => {
                          const target = el.closest('[role="button"], .MuiButtonBase-root, a, li, tr, div');
                          if (target) target.click();
                        }
                        """
                    )
                    clicked = True
                except Exception:
                    pass

            if not clicked:
                continue

            # page.wait_for_timeout(500)
            if not wait_detail_open():
                continue

            # 詳細画面が開いた直後にURLチェックを行い、既存なら画像取得などをスキップ
            if SKIP_EXISTING:
                current_detail_url = normalize_url(page.url)
                # URLが /bukken/ を含んでいて、かつ登録済みの場合
                if "/bukken/" in current_detail_url and current_detail_url in existing_normalized_urls:
                    all_found_urls.add(page.url)  # 削除判定のため記録
                    seen_normalized_urls.add(current_detail_url)
                    print(f"[SKIP] existing (fast url check): {title_text} ({current_detail_url})")
                    close_detail_popup(page)
                    continue

            item = extract_property_from_open_dialog(
                page,
                title_text,
                total_processed,
                known_url=current_detail_url,
                listing_rent=listing_rent,
            )
            detail_url = item.get("detail_url", "")
            if detail_url:
                all_found_urls.add(detail_url)
                seen_normalized_urls.add(normalize_url(detail_url))
            if "申込あり" in row_text and detail_url:
                applied_urls.add(detail_url)

            if SKIP_EXISTING and detail_url and normalize_url(detail_url) in existing_normalized_urls:
                all_found_urls.add(detail_url)  # 削除判定のため記録
                print(f"[SKIP] existing: {item.get('title', detail_url)}")
                close_detail_popup(page)
                continue

            if not item.get("raw_details") and not item.get("address"):
                close_detail_popup(page)
                # page.wait_for_timeout(200)
                continue

            base_id = item["id"]
            suffix = 2
            while item["id"] in used_ids:
                item["id"] = f"{base_id}_{suffix}"
                suffix += 1

            local_images, newly_downloaded = download_images(item["id"], item.get("image_urls", []))
            if not local_images:
                # 画像なし処理
                img_loc = page.locator('[role="dialog"] img, [role="tabpanel"] img, .MuiDialog-root img, img')
                try:
                    img_count = min(img_loc.count(), 20)
                except Exception:
                    img_count = 0
                if img_count > 0:
                    prop_dir = os.path.join(IMAGES_DIR, item["id"])
                    os.makedirs(prop_dir, exist_ok=True)
                    shot_paths: list[str] = []
                    for j in range(img_count):
                        try:
                            box = img_loc.nth(j).bounding_box()
                            if not box or box.get("width", 0) < 40 or box.get("height", 0) < 40:
                                continue
                            filename = f"{str(len(shot_paths) + 1).zfill(2)}.jpg"
                            full = os.path.join(prop_dir, filename)
                            img_loc.nth(j).screenshot(path=full)
                            shot_paths.append(f"images/{item['id']}/{filename}")
                        except Exception:
                            continue
                    local_images = shot_paths

            # ユーザー要望: 画像がなくてもスキップしない
            # if not local_images:
            #     print(f"[SKIP] no images: {item.get('title', item['id'])}")
            #     close_detail_popup(page)
            #     page.wait_for_timeout(200)
            #     continue

            item["local_images"] = local_images
            item["image_count"] = len(local_images)
            item["listing_title_key"] = listing_title_key
            if block_key:
                item["search_block_key"] = block_key
            used_ids.add(item["id"])
            if newly_downloaded:
                pending_upload_paths.extend(newly_downloaded)
                new_upload_paths.extend(newly_downloaded)

            print(f"[NEW] scraped: {item['title']} (id={item['id']}, img={len(local_images)})")  # 明示的にログ出力

            properties.append(item)
            pending_properties.append(item)

            close_detail_popup(page)
            # page.wait_for_timeout(0)
            
            total_processed += 1

            if total_processed % 10 == 0:
                print(f"  ... scraped total {total_processed}")

            if len(pending_properties) >= SAVE_INTERVAL:
                flush_pending(f"interval total {total_processed}")
            
            # --- ここまでスクレイピングロジック ---

        if interrupt_flag:
            break
            
        if MAX_PROPERTIES > 0 and total_processed >= MAX_PROPERTIES:
            break

        # 次ページへ遷移
        print("[INFO] checking next page...")
        next_button = page.locator('button[aria-label="Go to next page"], button[aria-label="次へ"], button:has-text("次へ"), li[title="次へ"] button, button.MuiPaginationItem-page:has-text("次へ")').first
        
        has_next = False
        try:
            if next_button.count() > 0 and next_button.is_visible() and not next_button.is_disabled():
                print("[INFO] go to next page")
                next_button.click()
                page.wait_for_timeout(3000) # 遷移待ち
                has_next = True
        except Exception as e:
            print(f"[WARN] failed to go next page: {e}")
            
        if not has_next:
            print("[INFO] no next page found, finish list")
            break
            
    # end while True

    try:
        flush_pending("final")
    except Exception:
        pass
        
    return properties, new_upload_paths, seen_normalized_urls, seen_title_keys


def save_results(properties: list[dict]) -> None:
    if not properties:
        print("保存対象データなし")
        return

    setup_dirs()

    existing: list[dict] = []
    if os.path.exists(INDEX_FILE):
        try:
            with open(INDEX_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []

    # R2アップロード用のパスリスト
    upload_targets = []

    by_id = {item.get("id", ""): item for item in existing if item.get("id")}
    for p in properties:
        json_filename = f"{p['id']}.json"
        json_path = os.path.join(DATA_DIR, json_filename)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(p, f, ensure_ascii=False, indent=2)
        
        # JSONファイルの相対パスを追加
        upload_targets.append(f"data/{json_filename}")

        by_id[p["id"]] = {
            "id": p["id"],
            "source": "es_square",
            "title": p.get("title", ""),
            "rent": p.get("rent", ""),
            "management_fee": p.get("management_fee", ""),
            "deposit": p.get("deposit", ""),
            "key_money": p.get("key_money", ""),
            "address": p.get("address", ""),
            "stations": p.get("stations", []),
            "layout": p.get("layout", ""),
            "area": p.get("area", ""),
            "floor": p.get("floor", ""),
            "built_date": p.get("built_date", ""),
            "structure": p.get("structure", ""),
            "direction": p.get("direction", ""),
            "available_date": p.get("available_date", ""),
            "contract_period": p.get("contract_period", ""),
            "parking": p.get("parking", ""),
            "renewal_fee": p.get("renewal_fee", ""),
            "insurance": p.get("insurance", ""),
            "ad_fee": p.get("ad_fee", ""),
            "transaction_type": p.get("transaction_type", ""),
            "guarantee_company": p.get("guarantee_company", ""),
            "remarks": p.get("remarks", ""),
            "preferred_conditions": p.get("preferred_conditions", ""),
            "viewing_start_date": p.get("viewing_start_date", ""),
            "viewing_notes": p.get("viewing_notes", ""),
            "facilities": p.get("facilities", []),
            "building_name": p.get("building_name", ""),
            "room_number": p.get("room_number", ""),
            "local_images": p.get("local_images", []),
            "thumbnail": p["local_images"][0] if p.get("local_images") else None,
            "image_count": len(p.get("local_images", [])) if p.get("local_images") else len(p.get("image_urls", [])),
            "detail_url": p.get("detail_url", ""),
            "listing_title_key": p.get("listing_title_key", normalize_listing_title_key(p.get("title", ""))),
            "search_block_key": p.get("search_block_key", ""),
            "scraped_at": p.get("original_scraped_at", p.get("scraped_at", "")),
            "updated_at": p.get("updated_at", ""),
        }

    final_list = list(by_id.values())
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False, indent=2)
    
    print(f"[OK] saved: data={len(properties)} index={len(final_list)}")

    # R2へアップロード（個別JSONは直接、インデックスはマージしてアップロード）
    if R2_UPLOAD_ENABLED:
        print(f"[R2] JSONデータのアップロードを開始します ({len(upload_targets)}ファイル)")
        upload_files_to_r2(upload_targets)
        upload_merged_index_to_r2()


def compress_images_after_scrape() -> None:
    script_path = os.path.join(os.path.dirname(__file__), "optimize_existing_images.py")
    if not os.path.exists(script_path):
        print("[WARN] optimize_existing_images.py が見つからないため圧縮をスキップ")
        return

    cmd = [
        sys.executable,
        script_path,
        "--workers",
        str(COMPRESS_WORKERS),
        "--max-side",
        str(COMPRESS_MAX_SIDE),
        "--quality",
        str(COMPRESS_QUALITY),
    ]
    try:
        print("[START] image compression")
        subprocess.run(cmd, check=True, cwd=os.path.dirname(script_path))
        print("[OK] image compression done")
    except Exception as e:
        print(f"[WARN] image compression failed: {e}")


def main() -> None:
    global interrupt_flag
    signal.signal(signal.SIGINT, signal_handler)
    setup_dirs()
    migrate_from_shared_index()
    tesseract_exe = _find_tesseract_exe()
    if PYTESSERACT_AVAILABLE or tesseract_exe:
        print(
            f"[OCR] enabled (pytesseract={PYTESSERACT_AVAILABLE}, tesseract={'yes' if tesseract_exe else 'no'}, "
            f"rent_fallback={'on' if ENABLE_RENT_OCR_FALLBACK else 'off'})"
        )
    else:
        print("[OCR] disabled (pytesseract/tesseract not found)")
    existing_data = load_existing_properties()
    if SKIP_EXISTING:
        print(f"[準備] 既存物件URL: {len(existing_data.get('by_url', {}))}件（スキップ対象）")

    results: list[dict] = []
    completed = False
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        try:
            context = browser.new_context(locale="ja-JP", timezone_id="Asia/Tokyo")
            page = context.new_page()

            if not login(page):
                return

            offset = 0
            while True:
                if interrupt_flag:
                    break
                
                # 条件設定（offset個スキップして次のターゲットを選択）
                try:
                    picked, selected_municipalities = select_search_conditions(page, skip_count=offset)
                    if picked == 0:
                        print("[INFO] 全市区町村の検索が完了しました")
                        break
                    
                    # リスト取得＆詳細スクレイピング（全ページネーション含む）
                    block_key = make_block_key(selected_municipalities)
                    p_results, _, seen_urls, seen_title_keys = scrape_property_list(page, existing_data, block_key=block_key)
                    results.extend(p_results)
                    if USE_BLOCK_CLEANUP and CLEANUP_ENDED:
                        cleanup_ended_properties_for_block(block_key, seen_urls, seen_title_keys)
                    existing_data = load_existing_properties()
                    
                    # 今回選択した分だけオフセットを進める
                    offset += picked
                    
                except Exception as e:
                    print(f"[ERROR] main loop error: {e}")
                    import traceback
                    traceback.print_exc()
                    break

            completed = not interrupt_flag
            # 画像圧縮はダウンロード時にインラインで実行済みのため事後圧縮は不要
        except KeyboardInterrupt:
            interrupt_flag = True
            print("\n[中断] メイン処理で中断。保存済みデータで終了します。")
        finally:
            try:
                browser.close()
            except Exception:
                pass

    # バックグラウンドアップロードの完了を待機（全画像がR2に届いてからクリーンアップ）
    wait_all_uploads()

    save_scraping_results()
    if CLEANUP_ENDED:
        if completed:
            print("[cleanup] Full cleanup after completed scan (final consistency pass).")
            cleanup_ended_properties(partial=False)
        elif interrupt_flag:
            print("[cleanup] Interrupted run: applying partial cleanup (applied-only).")
            cleanup_ended_properties(partial=True)
        else:
            print("[cleanup] Partial fetch mode: cleanup skipped")

    if completed:
        print("[sync] ローカル/R2 最終同期チェックを実行します...")
        sync_local_and_r2()
    else:
        print("[sync] 中断/部分取得のため同期チェックをスキップします（正常完了時のみ実行）")
    print("[DONE]")


if __name__ == "__main__":
    main()
