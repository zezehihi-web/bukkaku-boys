"""Vercel本番 E2Eテスト - 20件のSUUMO URLをVercel APIに送信し結果を検証"""
import json
import time
import sys
import ssl
import urllib.request

sys.stdout.reconfigure(encoding="utf-8")

VERCEL_API = "https://akiya-tools.vercel.app/api/akishitsu"
SSL_CTX = ssl.create_default_context()

# r2_suumo_urls.json から20件を選択（異なるエリアから均等に）
TEST_URLS = [
    # 新宿区 (3件)
    {"area": "新宿区", "url": "https://suumo.jp/chintai/jnc_000103873102/?bc=100488538142"},
    {"area": "新宿区", "url": "https://suumo.jp/chintai/jnc_000105185316/?bc=100455451788"},
    {"area": "新宿区", "url": "https://suumo.jp/chintai/jnc_000104416540/?bc=100485686334"},
    # 中野区 (3件)
    {"area": "中野区", "url": "https://suumo.jp/chintai/jnc_000105390754/?bc=100493047116"},
    {"area": "中野区", "url": "https://suumo.jp/chintai/jnc_000104599260/?bc=100486890695"},
    {"area": "中野区", "url": "https://suumo.jp/chintai/jnc_000105406008/?bc=100493085793"},
    # 杉並区 (3件)
    {"area": "杉並区", "url": "https://suumo.jp/chintai/jnc_000105049111/?bc=100490333845"},
    {"area": "杉並区", "url": "https://suumo.jp/chintai/jnc_000105280906/?bc=100492117886"},
    {"area": "杉並区", "url": "https://suumo.jp/chintai/jnc_000105258510/?bc=100492182959"},
    # 練馬区 (2件)
    {"area": "練馬区", "url": "https://suumo.jp/chintai/jnc_000105219124/?bc=100491642282"},
    {"area": "練馬区", "url": "https://suumo.jp/chintai/jnc_000104937260/?bc=100489490820"},
    # 豊島区 (2件)
    {"area": "豊島区", "url": "https://suumo.jp/chintai/jnc_000105359116/?bc=100492628193"},
    {"area": "豊島区", "url": "https://suumo.jp/chintai/jnc_000105236078/?bc=100491790009"},
    # 板橋区 (2件)
    {"area": "板橋区", "url": "https://suumo.jp/chintai/jnc_000105349782/?bc=100492553927"},
    {"area": "板橋区", "url": "https://suumo.jp/chintai/jnc_000105247596/?bc=100491896498"},
    # 北区 (2件)
    {"area": "北区", "url": "https://suumo.jp/chintai/jnc_000105223654/?bc=100491721459"},
    {"area": "北区", "url": "https://suumo.jp/chintai/jnc_000105382802/?bc=100492921687"},
    # 世田谷区 (2件)
    {"area": "世田谷区", "url": "https://suumo.jp/chintai/jnc_000105382803/?bc=100492908706"},
    # 足立区 (1件)
    {"area": "足立区", "url": "https://suumo.jp/chintai/jnc_000105350420/?bc=100485938617"},
    # 文京区 (1件)
    {"area": "文京区", "url": "https://suumo.jp/chintai/jnc_000105405246/?bc=100493105474"},
]


def api_post(path: str, data: dict) -> dict:
    body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(
        f"{VERCEL_API}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def api_get(path: str) -> dict:
    req = urllib.request.Request(f"{VERCEL_API}{path}")
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    print(f"=== Vercel E2Eテスト: {len(TEST_URLS)}件 ===\n")

    # Phase 1: 送信
    submissions = []
    for i, item in enumerate(TEST_URLS):
        url = item["url"]
        area = item["area"]
        print(f"[{i+1:2d}/{len(TEST_URLS)}] 送信: [{area}] ", end="", flush=True)
        try:
            result = api_post("/check", {"url": url})
            cid = result.get("id")
            print(f"OK id={cid} status={result.get('status')}")
            submissions.append({"check_id": cid, "url": url, "area": area})
        except Exception as e:
            print(f"ERROR: {str(e)[:80]}")
            submissions.append({"check_id": None, "url": url, "area": area, "error": str(e)[:100]})
        time.sleep(0.5)

    ok_count = len([s for s in submissions if s["check_id"]])
    print(f"\n送信完了: {ok_count}/{len(TEST_URLS)}件成功\n")

    # Phase 2: ポーリング（最大10分）
    print("=== 処理待機 (最大10分, 15秒間隔) ===")
    pending_ids = {s["check_id"] for s in submissions if s["check_id"]}
    results = {}
    start_time = time.time()
    deadline = start_time + 600

    while pending_ids and time.time() < deadline:
        still_pending = set()
        for cid in pending_ids:
            try:
                status = api_get(f"/check/{cid}")
                s = status.get("status", "unknown")
                if s in ("pending", "running", "parsing", "matching", "checking"):
                    still_pending.add(cid)
                else:
                    results[cid] = status
            except Exception:
                still_pending.add(cid)

        completed = len(results)
        remaining = len(still_pending)
        elapsed = int(time.time() - start_time)
        print(f"  [{elapsed:3d}s] 完了: {completed} / 処理中: {remaining}", flush=True)

        if not still_pending:
            break

        pending_ids = still_pending
        time.sleep(15)

    # タイムアウト分も取得
    for cid in pending_ids:
        try:
            results[cid] = api_get(f"/check/{cid}")
        except Exception:
            pass

    # Phase 3: 結果集計
    print(f"\n{'='*60}")
    print(f"=== 結果集計 ({int(time.time()-start_time)}秒) ===")
    print(f"{'='*60}\n")

    status_counts = {}
    platform_counts = {}
    details = []

    for sub in submissions:
        cid = sub["check_id"]
        if not cid or cid not in results:
            status_counts["submit_error"] = status_counts.get("submit_error", 0) + 1
            continue

        r = results[cid]
        s = r.get("status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1

        platform = r.get("platform", "") or "(なし)"
        if s == "done":
            platform_counts[platform] = platform_counts.get(platform, 0) + 1

        details.append({
            "id": cid,
            "area": sub["area"],
            "status": s,
            "property_name": r.get("property_name", ""),
            "property_address": r.get("property_address", ""),
            "atbb_matched": r.get("atbb_matched", False),
            "atbb_company": r.get("atbb_company", ""),
            "platform": platform,
            "vacancy_result": r.get("vacancy_result", ""),
            "error_message": r.get("error_message", ""),
        })

    print("--- ステータス別 ---")
    for s, c in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {s}: {c}件")

    if platform_counts:
        print("\n--- プラットフォーム別 (done) ---")
        for p, c in sorted(platform_counts.items(), key=lambda x: -x[1]):
            print(f"  {p}: {c}件")

    # 各物件の詳細
    print(f"\n--- 全物件詳細 ---")
    for d in details:
        status_icon = {"done": "OK", "error": "NG", "not_found": "--", "awaiting_platform": "??"}.get(d["status"], "..")
        print(f"  [{status_icon}] #{d['id']} [{d['area']}] {d['property_name']}")
        if d["atbb_matched"]:
            print(f"      ATBB: {d['atbb_company'][:40]}")
        if d["platform"] and d["platform"] != "(なし)":
            print(f"      Platform: {d['platform']}")
        if d["vacancy_result"]:
            print(f"      Result: {d['vacancy_result']}")
        if d["error_message"]:
            print(f"      Error: {d['error_message'][:60]}")
        print()

    # KPI計算
    total = len(details)
    parsed = len([d for d in details if d["property_name"]])
    matched = len([d for d in details if d["atbb_matched"]])
    resolved = len([d for d in details if d["status"] == "done" and d["vacancy_result"] and "確認不可" not in d["vacancy_result"]])
    errors = len([d for d in details if d["status"] == "error"])

    print(f"--- KPI ---")
    print(f"  Parse成功率: {parsed}/{total} ({parsed/total*100:.0f}%)" if total else "")
    print(f"  ATBB照合率: {matched}/{total} ({matched/total*100:.0f}%)" if total else "")
    print(f"  自動解決率: {resolved}/{total} ({resolved/total*100:.0f}%)" if total else "")
    print(f"  エラー率:   {errors}/{total} ({errors/total*100:.0f}%)" if total else "")

    # 結果保存
    with open("results/vercel_e2e_results.json", "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "summary": status_counts,
            "platform_counts": platform_counts,
            "kpi": {
                "total": total,
                "parsed": parsed,
                "atbb_matched": matched,
                "auto_resolved": resolved,
                "errors": errors,
            },
            "details": details,
        }, f, ensure_ascii=False, indent=2)

    print(f"\n結果保存: results/vercel_e2e_results.json")


if __name__ == "__main__":
    main()
