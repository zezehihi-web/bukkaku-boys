"""SUUMO検索スクリプト - R2物件名からSUUMO URLを取得"""
import json
import urllib.request
import urllib.parse
import re
import time
import unicodedata
import sys
import os

# UTF-8出力を強制
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

def search_suumo(search_name: str) -> str | None:
    """SUUMOで物件名を検索し、詳細ページURLを返す"""
    encoded = urllib.parse.quote(search_name)
    url = f'https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&fw={encoded}&pc=50'

    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    })
    resp = urllib.request.urlopen(req, timeout=15)
    html = resp.read().decode('utf-8', errors='ignore')

    # 詳細ページリンクを探す（/chintai/xx/sc_xxxxx/ パターン）
    links = re.findall(r'href="(https://suumo\.jp/chintai/[a-z]+/sc_[^/"]+/[^"]*)"', html)
    if not links:
        links2 = re.findall(r'href="(/chintai/[a-z]+/sc_[^/"]+/[^"]*)"', html)
        links = [f'https://suumo.jp{l}' for l in links2]

    if links:
        # 重複除去して最初のリンクを返す
        seen = set()
        for l in links:
            if l not in seen:
                seen.add(l)
                return l

    return None


def main():
    # R2物件名を読み込み
    with open('results/r2_test_names.json', encoding='utf-8') as f:
        names = json.load(f)

    # フィルタ: 短い名前、戸建、住所形式を除外
    good_names = []
    for n in names:
        n_norm = unicodedata.normalize('NFKC', n).strip()
        n_norm = re.sub(r'\s+\d+[A-Z]?$', '', n_norm)  # 末尾の部屋番号除去
        n_norm = re.sub(r'\s*[-~～].*$', '', n_norm)      # サブタイトル除去
        n_norm = n_norm.strip()
        if len(n_norm) >= 4 and '戸建' not in n_norm and '丁目' not in n_norm and '号棟' not in n_norm:
            good_names.append(n_norm)

    print(f'検索対象: {len(good_names)}件')

    found = []
    errors = 0

    for i, name in enumerate(good_names):
        print(f'[{i+1}/{len(good_names)}] {name} ... ', end='', flush=True)

        try:
            url = search_suumo(name)
            if url:
                found.append({'name': name, 'url': url})
                print(f'HIT -> {url[:80]}')
            else:
                print('MISS')
        except Exception as e:
            print(f'ERR: {str(e)[:50]}')
            errors += 1
            time.sleep(3)

        time.sleep(1.5)  # レート制限

        # 50件見つけたら終了
        if len(found) >= 50:
            print(f'\n50件到達、検索終了')
            break

    print(f'\n結果: {len(found)}件HIT / {len(good_names)}件中 (エラー: {errors})')

    # 保存
    with open('results/r2_suumo_urls.json', 'w', encoding='utf-8') as f:
        json.dump(found, f, ensure_ascii=False, indent=2)

    print(f'保存: results/r2_suumo_urls.json')


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    main()
