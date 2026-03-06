"""
空確くん 100件総合テスト
SUUMO, CHINTAI, Yahoo!不動産, eheya から100件をテスト
"""
import asyncio
import aiohttp
import json
import time
from datetime import datetime

API_BASE = "http://localhost:8000/api"

# --- 100件のテストURL ---
TEST_URLS = [
    # === SUUMO 新宿区 (10件) ===
    "https://suumo.jp/chintai/jnc_000103392852/?bc=100478336058",
    "https://suumo.jp/chintai/jnc_000104928118/?bc=100489460445",
    "https://suumo.jp/chintai/jnc_000104249165/?bc=100488726415",
    "https://suumo.jp/chintai/jnc_000105405267/?bc=100493116233",
    "https://suumo.jp/chintai/jnc_000101395789/?bc=100475144362",
    "https://suumo.jp/chintai/jnc_000103398389/?bc=100487539647",
    "https://suumo.jp/chintai/jnc_000105012541/?bc=100493100050",
    "https://suumo.jp/chintai/jnc_000104655143/?bc=100490313558",
    "https://suumo.jp/chintai/jnc_000105357742/?bc=100492880523",
    "https://suumo.jp/chintai/jnc_000103129755/?bc=100493080297",

    # === SUUMO 渋谷区 (10件) ===
    "https://suumo.jp/chintai/jnc_000102572851/",
    "https://suumo.jp/chintai/jnc_000104132632/",
    "https://suumo.jp/chintai/jnc_000104442890/",
    "https://suumo.jp/chintai/jnc_000104725403/",
    "https://suumo.jp/chintai/jnc_000103474065/",
    "https://suumo.jp/chintai/jnc_000104438107/",
    "https://suumo.jp/chintai/jnc_000104491985/",
    "https://suumo.jp/chintai/jnc_000104689446/",
    "https://suumo.jp/chintai/jnc_000104426213/",
    "https://suumo.jp/chintai/jnc_000105096782/",

    # === SUUMO 港区 (10件) ===
    "https://suumo.jp/chintai/jnc_000103722886/?bc=100484913710",
    "https://suumo.jp/chintai/jnc_000102135662/?bc=100467243359",
    "https://suumo.jp/chintai/jnc_000104608362/?bc=100486958038",
    "https://suumo.jp/chintai/jnc_000105369711/?bc=100492822733",
    "https://suumo.jp/chintai/jnc_000104116839/?bc=100483510901",
    "https://suumo.jp/chintai/jnc_000102317704/?bc=100490403598",
    "https://suumo.jp/chintai/jnc_000105390124/?bc=100493191454",
    "https://suumo.jp/chintai/jnc_000104935409/?bc=100490093359",
    "https://suumo.jp/chintai/jnc_000104548512/?bc=100486957915",
    "https://suumo.jp/chintai/jnc_000104824264/?bc=100488811920",

    # === SUUMO 世田谷区 (10件) ===
    "https://suumo.jp/chintai/jnc_000104249646/?bc=100488503267",
    "https://suumo.jp/chintai/jnc_000104655645/?bc=100490396710",
    "https://suumo.jp/chintai/jnc_000104417158/?bc=100485500599",
    "https://suumo.jp/chintai/jnc_000103977248/?bc=100487356032",
    "https://suumo.jp/chintai/jnc_000105034954/?bc=100487814221",
    "https://suumo.jp/chintai/jnc_000104464832/?bc=100487472607",
    "https://suumo.jp/chintai/jnc_000103557955/?bc=100492868463",
    "https://suumo.jp/chintai/jnc_000105249296/?bc=100491850377",
    "https://suumo.jp/chintai/jnc_000104567818/?bc=100486644715",
    "https://suumo.jp/chintai/jnc_000105165408/?bc=100491231435",

    # === SUUMO 練馬区 (8件) ===
    "https://suumo.jp/chintai/jnc_000038283971/?bc=100493257050",
    "https://suumo.jp/chintai/jnc_000105258770/?bc=100491944316",
    "https://suumo.jp/chintai/jnc_000094557379/?bc=100493229446",
    "https://suumo.jp/chintai/jnc_000104064234/?bc=100375663413",
    "https://suumo.jp/chintai/jnc_000105123822/?bc=100490867161",
    "https://suumo.jp/chintai/jnc_000104929081/?bc=100489264687",
    "https://suumo.jp/chintai/jnc_000105337481/?bc=100492640261",
    "https://suumo.jp/chintai/jnc_000103654851/?bc=100479564374",

    # === SUUMO 大田区 (8件) ===
    "https://suumo.jp/chintai/jnc_000105337041/?bc=100492605761",
    "https://suumo.jp/chintai/jnc_000105291616/?bc=100492261597",
    "https://suumo.jp/chintai/jnc_000104295719/?bc=100484491584",
    "https://suumo.jp/chintai/jnc_000103223298/?bc=100489974170",
    "https://suumo.jp/chintai/jnc_000105390565/?bc=100493105409",
    "https://suumo.jp/chintai/jnc_000104276365/?bc=100484592057",
    "https://suumo.jp/chintai/jnc_000104725070/?bc=100488547075",
    "https://suumo.jp/chintai/jnc_000101466944/?bc=100463488886",

    # === SUUMO 板橋区 (8件) ===
    "https://suumo.jp/chintai/jnc_000104510439/?bc=100492959269",
    "https://suumo.jp/chintai/jnc_000105123744/?bc=100492221556",
    "https://suumo.jp/chintai/jnc_000105281221/?bc=100492772258",
    "https://suumo.jp/chintai/jnc_000101413004/?bc=100489288547",
    "https://suumo.jp/chintai/jnc_000105351204/?bc=100492861858",
    "https://suumo.jp/chintai/jnc_000104171017/?bc=100484149847",
    "https://suumo.jp/chintai/jnc_000104718030/?bc=100487599963",
    "https://suumo.jp/chintai/jnc_000105319052/?bc=100492472583",

    # === SUUMO 目黒区 (4件) ===
    "https://suumo.jp/chintai/jnc_000104399607/",
    "https://suumo.jp/chintai/jnc_000104399608/",
    "https://suumo.jp/chintai/jnc_000105081042/",
    "https://suumo.jp/chintai/jnc_000101574702/",

    # === SUUMO 豊島区 (4件) ===
    "https://suumo.jp/chintai/jnc_000104769190/?bc=100491767629",
    "https://suumo.jp/chintai/jnc_000104759770/?bc=100401812864",
    "https://suumo.jp/chintai/jnc_000091631801/?bc=100465427933",
    "https://suumo.jp/chintai/jnc_000105415357/?bc=100493256276",

    # === CHINTAI 新宿区 (13件) ===
    "https://www.chintai.net/detail/bk-0000006580000000007986050001/",
    "https://www.chintai.net/detail/bk-0000000740000000000423560008/",
    "https://www.chintai.net/detail/bk-C010105070000021380099910001/",
    "https://www.chintai.net/detail/bk-C010099971111630013007640001/",
    "https://www.chintai.net/detail/bk-C010086990000005114908800001/",
    "https://www.chintai.net/detail/bk-C010094380000013581190020001/",
    "https://www.chintai.net/detail/bk-C010105020000021370127030001/",
    "https://www.chintai.net/detail/bk-C010105020000021370126990001/",
    "https://www.chintai.net/detail/bk-C010105020000021370091520001/",
    "https://www.chintai.net/detail/bk-C010105020000021370022050001/",
    "https://www.chintai.net/detail/bk-C010091160000004602232950001/",
    "https://www.chintai.net/detail/bk-C010090810000004611600350001/",
    "https://www.chintai.net/detail/bk-C010100680000016310825330001/",

    # === Yahoo!不動産 (5件) ===
    "https://realestate.yahoo.co.jp/rent/detail/_000008736894735cd8b70ab4110f71d8bf0ca7145468/",
    "https://realestate.yahoo.co.jp/rent/detail/0000086524598721ce1782c8b6d8334d0a995c942dbe/",
    "https://realestate.yahoo.co.jp/rent/detail/000008780307424012218ebacb4dc1841b7c0d11947a/",
    "https://realestate.yahoo.co.jp/rent/detail/000008781517c7d4bc129dd4a652b52929a6593d5f97/",
    "https://realestate.yahoo.co.jp/rent/detail/_000008782788af8d1a27aca7f6f0380f6a4d0d4e695e/",

    # === eheya いい部屋ネット (10件) ===
    "https://www.eheya.net/detail/300001343017243000001/",
    "https://www.eheya.net/detail/300000803007098000003/",
    "https://www.eheya.net/detail/300001343017251000001/",
    "https://www.eheya.net/detail/300001013236171000001/",
    "https://www.eheya.net/detail/300001013233680000001/",
    "https://www.eheya.net/detail/300001343017229000001/",
    "https://www.eheya.net/detail/300001013235410000001/",
    "https://www.eheya.net/detail/300001343016779000001/",
    "https://www.eheya.net/detail/300001343017267000001/",
    "https://www.eheya.net/detail/300001343017269000001/",
]

assert len(TEST_URLS) == 100, f"Expected 100 URLs, got {len(TEST_URLS)}"


async def submit_check(session: aiohttp.ClientSession, url: str) -> dict | None:
    """Submit a single URL to the check API."""
    try:
        async with session.post(f"{API_BASE}/check", json={"url": url}, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                text = await resp.text()
                return {"error": f"HTTP {resp.status}: {text}", "submitted_url": url}
    except Exception as e:
        return {"error": str(e), "submitted_url": url}


async def poll_result(session: aiohttp.ClientSession, check_id: int, max_wait: int = 180) -> dict:
    """Poll for a check result until done or timeout."""
    start = time.time()
    while time.time() - start < max_wait:
        try:
            async with session.get(f"{API_BASE}/check/{check_id}", timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get("status", "")
                    if status in ("done", "error", "not_found", "awaiting_platform"):
                        return data
        except Exception:
            pass
        await asyncio.sleep(3)
    # Timeout - return last known state
    try:
        async with session.get(f"{API_BASE}/check/{check_id}", timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception:
        pass
    return {"id": check_id, "status": "timeout"}


async def run_batch(urls: list[str], batch_size: int = 5) -> list[dict]:
    """Run all URLs in batches to avoid overwhelming the system."""
    results = []
    total = len(urls)

    async with aiohttp.ClientSession() as session:
        for i in range(0, total, batch_size):
            batch = urls[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            print(f"\n[Batch {batch_num}/{total_batches}] Submitting {len(batch)} URLs...")

            # Submit batch
            submit_tasks = [submit_check(session, url) for url in batch]
            submissions = await asyncio.gather(*submit_tasks)

            # Collect check IDs
            check_ids = []
            for sub in submissions:
                if sub and "id" in sub and "error" not in sub:
                    check_ids.append(sub["id"])
                    print(f"  Submitted #{sub['id']}: {sub.get('portal_source', '?')}")
                else:
                    results.append(sub or {"error": "submit_failed"})
                    print(f"  FAILED: {sub}")

            # Wait a moment for processing
            await asyncio.sleep(2)

            # Poll for results
            if check_ids:
                poll_tasks = [poll_result(session, cid) for cid in check_ids]
                poll_results = await asyncio.gather(*poll_tasks)
                results.extend(poll_results)

                for r in poll_results:
                    status = r.get("status", "?")
                    name = r.get("property_name", "")[:30]
                    vacancy = r.get("vacancy_result", "")
                    platform = r.get("platform", "")
                    print(f"  #{r.get('id', '?')} [{status}] {name} | {platform} | {vacancy}")

            # Rate limit between batches
            if i + batch_size < total:
                print("  Waiting 5s before next batch...")
                await asyncio.sleep(5)

    return results


def analyze_results(results: list[dict]) -> dict:
    """Analyze test results and generate statistics."""
    stats = {
        "total": len(results),
        "by_status": {},
        "by_portal": {},
        "by_platform": {},
        "by_area": {},
        "vacancy_results": {},
        "parse_success": 0,
        "parse_fail": 0,
        "atbb_matched": 0,
        "atbb_not_matched": 0,
        "errors": [],
        "not_found_details": [],
        "success_details": [],
    }

    for r in results:
        if "error" in r and "status" not in r:
            stats["by_status"]["submit_error"] = stats["by_status"].get("submit_error", 0) + 1
            stats["errors"].append(r)
            continue

        status = r.get("status", "unknown")
        stats["by_status"][status] = stats["by_status"].get(status, 0) + 1

        portal = r.get("portal_source", "unknown")
        stats["by_portal"][portal] = stats["by_portal"].get(portal, 0) + 1

        # Parse success
        if r.get("property_name"):
            stats["parse_success"] += 1
        else:
            stats["parse_fail"] += 1

        # ATBB matching
        if r.get("atbb_matched"):
            stats["atbb_matched"] += 1
        else:
            stats["atbb_not_matched"] += 1

        # Platform
        platform = r.get("platform", "(なし)")
        if not platform:
            platform = "(なし)"
        stats["by_platform"][platform] = stats["by_platform"].get(platform, 0) + 1

        # Vacancy result
        vacancy = r.get("vacancy_result", "")
        if vacancy:
            stats["vacancy_results"][vacancy] = stats["vacancy_results"].get(vacancy, 0) + 1

        # Collect details for analysis
        if status == "not_found":
            stats["not_found_details"].append({
                "id": r.get("id"),
                "url": r.get("submitted_url", "")[:80],
                "property_name": r.get("property_name", ""),
                "portal": portal,
                "atbb_company": r.get("atbb_company", ""),
                "error": r.get("error_message", ""),
            })
        elif status == "done":
            stats["success_details"].append({
                "id": r.get("id"),
                "property_name": r.get("property_name", ""),
                "portal": portal,
                "platform": platform,
                "vacancy_result": r.get("vacancy_result", ""),
                "atbb_company": r.get("atbb_company", ""),
            })
        elif status == "error":
            stats["errors"].append({
                "id": r.get("id"),
                "url": r.get("submitted_url", "")[:80],
                "property_name": r.get("property_name", ""),
                "portal": portal,
                "error": r.get("error_message", ""),
            })

    return stats


def print_report(stats: dict):
    """Print comprehensive analysis report."""
    print("\n" + "=" * 80)
    print("空確くん 100件総合テスト結果レポート")
    print(f"実施日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    print(f"\n■ 総テスト件数: {stats['total']}")

    print("\n■ ステータス別結果:")
    for status, count in sorted(stats["by_status"].items(), key=lambda x: -x[1]):
        pct = count / stats["total"] * 100
        bar = "█" * int(pct / 2)
        print(f"  {status:25s}: {count:3d} ({pct:5.1f}%) {bar}")

    print("\n■ ポータル別結果:")
    for portal, count in sorted(stats["by_portal"].items(), key=lambda x: -x[1]):
        print(f"  {portal:15s}: {count:3d}件")

    print("\n■ プラットフォーム別結果:")
    for platform, count in sorted(stats["by_platform"].items(), key=lambda x: -x[1]):
        print(f"  {platform:25s}: {count:3d}件")

    print(f"\n■ パース成功率:")
    total_parse = stats["parse_success"] + stats["parse_fail"]
    if total_parse > 0:
        print(f"  成功: {stats['parse_success']}/{total_parse} ({stats['parse_success']/total_parse*100:.1f}%)")
        print(f"  失敗: {stats['parse_fail']}/{total_parse} ({stats['parse_fail']/total_parse*100:.1f}%)")

    print(f"\n■ ATBB照合率:")
    total_atbb = stats["atbb_matched"] + stats["atbb_not_matched"]
    if total_atbb > 0:
        print(f"  一致: {stats['atbb_matched']}/{total_atbb} ({stats['atbb_matched']/total_atbb*100:.1f}%)")
        print(f"  不一致: {stats['atbb_not_matched']}/{total_atbb} ({stats['atbb_not_matched']/total_atbb*100:.1f}%)")

    if stats["vacancy_results"]:
        print("\n■ 空室確認結果:")
        for result, count in sorted(stats["vacancy_results"].items(), key=lambda x: -x[1]):
            print(f"  {result:40s}: {count:3d}件")

    if stats["success_details"]:
        print(f"\n■ 成功した確認 ({len(stats['success_details'])}件):")
        for d in stats["success_details"][:20]:
            print(f"  #{d['id']} {d['property_name'][:25]:25s} | {d['platform']:20s} | {d['vacancy_result']}")

    if stats["not_found_details"]:
        print(f"\n■ 確認不可（not_found）の詳細 ({len(stats['not_found_details'])}件):")
        for d in stats["not_found_details"][:20]:
            print(f"  #{d['id']} {d['property_name'][:25]:25s} | ATBB企業: {d['atbb_company'][:20]}")

    if stats["errors"]:
        print(f"\n■ エラー詳細 ({len(stats['errors'])}件):")
        for d in stats["errors"][:10]:
            print(f"  #{d.get('id', '?')} {d.get('property_name', '')[:25]} | {d.get('error', '')[:60]}")

    # Key metrics
    print("\n" + "=" * 80)
    print("■ サービス品質指標")
    print("=" * 80)
    done = stats["by_status"].get("done", 0)
    not_found = stats["by_status"].get("not_found", 0)
    awaiting = stats["by_status"].get("awaiting_platform", 0)
    error = stats["by_status"].get("error", 0)
    timeout_count = stats["by_status"].get("timeout", 0)

    resolution_rate = done / stats["total"] * 100 if stats["total"] > 0 else 0
    usable_rate = (done + awaiting) / stats["total"] * 100 if stats["total"] > 0 else 0
    fail_rate = (not_found + error + timeout_count) / stats["total"] * 100 if stats["total"] > 0 else 0

    print(f"  自動解決率 (done):           {done:3d}/{stats['total']} = {resolution_rate:.1f}%")
    print(f"  対応可能率 (done+awaiting):   {done + awaiting:3d}/{stats['total']} = {usable_rate:.1f}%")
    print(f"  失敗率 (not_found+error):     {not_found + error + timeout_count:3d}/{stats['total']} = {fail_rate:.1f}%")


async def main():
    print(f"空確くん 100件総合テスト開始: {datetime.now()}")
    print(f"テスト件数: {len(TEST_URLS)}")

    # Check API connectivity
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_BASE}/checks", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    print("API接続: OK")
                else:
                    print(f"API接続エラー: HTTP {resp.status}")
                    return
        except Exception as e:
            print(f"API接続エラー: {e}")
            return

    results = await run_batch(TEST_URLS, batch_size=3)

    stats = analyze_results(results)
    print_report(stats)

    # Save full results
    output = {
        "test_date": datetime.now().isoformat(),
        "total_urls": len(TEST_URLS),
        "stats": {k: v for k, v in stats.items() if k not in ("not_found_details", "success_details", "errors")},
        "details": results,
    }
    with open("results/comprehensive_test_results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    print("\n結果をresults/comprehensive_test_results.jsonに保存しました")


if __name__ == "__main__":
    asyncio.run(main())
