"""SUUMOからエリアベース+キーワードで物件URLを収集"""
import json
import urllib.request
import urllib.parse
import re
import time
import sys
import os

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}

def fetch_suumo_page(url: str) -> str:
    req = urllib.request.Request(url, headers=HEADERS)
    resp = urllib.request.urlopen(req, timeout=15)
    return resp.read().decode('utf-8', errors='ignore')

def extract_property_urls(html: str) -> list[str]:
    """SUUMOの検索結果ページから物件詳細URLを抽出"""
    # /chintai/jnc_XXXXX/?bc=XXXXX パターン
    links = re.findall(r'href="(/chintai/jnc_\d+/\?bc=\d+)"', html)
    # 重複除去
    seen = set()
    unique = []
    for l in links:
        if l not in seen:
            seen.add(l)
            unique.append(f'https://suumo.jp{l}')
    return unique

def main():
    all_urls = []

    # 東京23区の各エリアからSUUMO物件を取得
    # sc コード: 新宿=13104, 渋谷=13113, 中野=13114, 杉並=13115, 豊島=13116
    # 板橋=13119, 練馬=13120, 世田谷=13112, 目黒=13110, 品川=13109
    # 港=13103, 千代田=13101, 文京=13105, 台東=13106, 墨田=13107
    # 江東=13108, 北=13117, 荒川=13118, 足立=13121, 葛飾=13122, 江戸川=13123
    # 大田=13111, 中央=13102
    # + 埼玉: さいたま市=11101-11110, 川口=11203, 所沢=11208, 川越=11201

    areas = [
        # (area_code, area_name) - 関東の主要エリア
        ('13104', '新宿区'),
        ('13113', '渋谷区'),
        ('13114', '中野区'),
        ('13115', '杉並区'),
        ('13116', '豊島区'),
        ('13112', '世田谷区'),
        ('13110', '目黒区'),
        ('13109', '品川区'),
        ('13103', '港区'),
        ('13105', '文京区'),
        ('13117', '北区'),
        ('13119', '板橋区'),
        ('13120', '練馬区'),
        ('13111', '大田区'),
        ('13106', '台東区'),
        ('11203', '川口市'),
        ('11101', 'さいたま市西区'),
    ]

    for sc_code, area_name in areas:
        print(f'\n=== {area_name} (sc_{sc_code}) ===')
        url = f'https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc={sc_code}&cb=0.0&ct=9999999&mb=0&mt=9999999&et=9999999&cn=9999999&pc=50&page=1'
        if sc_code.startswith('11'):
            url = url.replace('ta=13', 'ta=11')

        try:
            html = fetch_suumo_page(url)
            urls = extract_property_urls(html)
            print(f'  Found {len(urls)} property URLs')

            for u in urls[:5]:  # 各エリアから最大5件
                if u not in [x['url'] for x in all_urls]:
                    all_urls.append({'area': area_name, 'url': u})

            time.sleep(2)
        except Exception as e:
            print(f'  ERROR: {str(e)[:60]}')
            time.sleep(3)

        # 50件集まったら終了
        if len(all_urls) >= 50:
            break

    print(f'\n=== 合計 {len(all_urls)} 件のSUUMO URL収集完了 ===')

    # 保存
    with open('results/r2_suumo_urls.json', 'w', encoding='utf-8') as f:
        json.dump(all_urls, f, ensure_ascii=False, indent=2)

    print('保存: results/r2_suumo_urls.json')

    # URLリストをプレビュー
    for i, item in enumerate(all_urls[:10]):
        print(f'  {i+1}. [{item["area"]}] {item["url"][:80]}')
    if len(all_urls) > 10:
        print(f'  ... 他{len(all_urls)-10}件')


if __name__ == '__main__':
    main()
