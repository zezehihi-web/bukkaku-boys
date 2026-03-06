"""E2Eテスト - SUUMO URLをAPIに送信し、結果を集計"""
import json
import urllib.request
import time
import sys
import os

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

API_BASE = 'http://localhost:8000/api'


def submit_url(url: str) -> dict:
    """URLを送信して check_id を取得"""
    data = json.dumps({'url': url}).encode('utf-8')
    req = urllib.request.Request(
        f'{API_BASE}/check',
        data=data,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read().decode('utf-8'))


def get_status(check_id: int) -> dict:
    """check_id のステータスを取得"""
    req = urllib.request.Request(f'{API_BASE}/check/{check_id}')
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read().decode('utf-8'))


def main():
    # SUUMO URL読み込み
    with open('results/r2_suumo_urls.json', encoding='utf-8') as f:
        items = json.load(f)

    print(f'=== E2Eテスト: {len(items)}件のSUUMO URL ===\n')

    # Phase 1: 全URLを送信
    submissions = []
    for i, item in enumerate(items):
        url = item['url']
        area = item.get('area', '?')
        print(f'[{i+1}/{len(items)}] 送信中... [{area}] ', end='', flush=True)
        try:
            result = submit_url(url)
            check_id = result.get('id')
            print(f'ID={check_id} status={result.get("status")}')
            submissions.append({
                'check_id': check_id,
                'url': url,
                'area': area,
            })
        except Exception as e:
            print(f'ERROR: {str(e)[:80]}')
            submissions.append({
                'check_id': None,
                'url': url,
                'area': area,
                'error': str(e)[:100],
            })
        time.sleep(0.3)

    print(f'\n送信完了: {len([s for s in submissions if s["check_id"]])}件成功\n')

    # Phase 2: 結果をポーリング（最大5分）
    print('=== 処理待機中 (最大5分) ===')
    pending_ids = [s['check_id'] for s in submissions if s['check_id']]
    results = {}
    deadline = time.time() + 300  # 5分

    while pending_ids and time.time() < deadline:
        still_pending = []
        for cid in pending_ids:
            try:
                status = get_status(cid)
                s = status.get('status', 'unknown')
                if s in ('pending', 'processing'):
                    still_pending.append(cid)
                else:
                    results[cid] = status
            except Exception:
                still_pending.append(cid)

        completed = len(results)
        remaining = len(still_pending)
        elapsed = int(time.time() - (deadline - 300))
        print(f'  [{elapsed}s] 完了: {completed} / 待機中: {remaining}', flush=True)

        if not still_pending:
            break

        pending_ids = still_pending
        time.sleep(10)

    # タイムアウトしたものも結果取得
    for cid in pending_ids:
        try:
            results[cid] = get_status(cid)
        except Exception:
            pass

    # Phase 3: 結果集計
    print('\n=== 結果集計 ===\n')

    status_counts = {}
    platform_counts = {}
    detail_results = []

    for sub in submissions:
        cid = sub['check_id']
        if not cid or cid not in results:
            status_counts['submit_error'] = status_counts.get('submit_error', 0) + 1
            continue

        r = results[cid]
        s = r.get('status', 'unknown')
        status_counts[s] = status_counts.get(s, 0) + 1

        platform = r.get('platform', '') or '(なし)'
        if s == 'done':
            platform_counts[platform] = platform_counts.get(platform, 0) + 1

        detail_results.append({
            'id': cid,
            'area': sub['area'],
            'status': s,
            'property_name': r.get('property_name', ''),
            'room_number': r.get('room_number', ''),
            'platform': platform,
            'vacancy_status': r.get('vacancy_status', ''),
            'management_company': r.get('management_company', ''),
            'url': sub['url'],
        })

    print('--- ステータス別 ---')
    for s, c in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f'  {s}: {c}件')

    print(f'\n--- 確認完了物件 (done) の詳細 ---')
    done_items = [d for d in detail_results if d['status'] == 'done']
    for d in done_items:
        print(f"  {d['property_name']} {d['room_number']} [{d['area']}]")
        print(f"    管理会社: {d['management_company']}")
        print(f"    プラットフォーム: {d['platform']}")
        print(f"    空室状態: {d['vacancy_status']}")
        print()

    print(f'--- awaiting_platform の詳細 ---')
    awaiting = [d for d in detail_results if d['status'] == 'awaiting_platform']
    for d in awaiting[:10]:
        print(f"  {d['property_name']} {d['room_number']} [{d['area']}]")
        print(f"    管理会社: {d['management_company']}")
        print()
    if len(awaiting) > 10:
        print(f'  ... 他{len(awaiting)-10}件')

    # 保存
    with open('results/e2e_test_results.json', 'w', encoding='utf-8') as f:
        json.dump({
            'summary': status_counts,
            'platform_counts': platform_counts,
            'details': detail_results,
        }, f, ensure_ascii=False, indent=2)

    print(f'\n結果保存: results/e2e_test_results.json')


if __name__ == '__main__':
    main()
