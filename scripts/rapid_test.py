"""
空確くん 100件総合テスト v2 - 重複なし100物件
東京23区からユニーク建物(JNC)のSUUMO URLで空室確認
"""
import asyncio
import aiohttp
import json
import sys
import time
from datetime import datetime

API_BASE = "http://localhost:8000/api"

# --- 100件のテストURL（全てユニーク建物、東京23区から5件ずつ） ---
TEST_URLS = [
    # === 千代田区 ===
    "https://suumo.jp/chintai/jnc_000105431300/?bc=100493292164",
    "https://suumo.jp/chintai/jnc_000105423520/?bc=100493195029",
    "https://suumo.jp/chintai/jnc_000105405078/?bc=100493113509",
    "https://suumo.jp/chintai/jnc_000105381887/?bc=100492940622",
    "https://suumo.jp/chintai/jnc_000078681366/?bc=100492869748",
    # === 中央区 ===
    "https://suumo.jp/chintai/jnc_000104310170/?bc=100472643207",
    "https://suumo.jp/chintai/jnc_000102804970/?bc=100473547018",
    "https://suumo.jp/chintai/jnc_000104662161/?bc=100484884959",
    "https://suumo.jp/chintai/jnc_000102390665/?bc=100482191053",
    "https://suumo.jp/chintai/jnc_000104662159/?bc=100482190468",
    # === 港区 ===
    "https://suumo.jp/chintai/jnc_000103722886/?bc=100482503240",
    "https://suumo.jp/chintai/jnc_000103770529/?bc=100481414851",
    "https://suumo.jp/chintai/jnc_000102135662/?bc=100466803914",
    "https://suumo.jp/chintai/jnc_000103770530/?bc=100481491629",
    "https://suumo.jp/chintai/jnc_000104003301/?bc=100482502743",
    # === 新宿区 ===
    "https://suumo.jp/chintai/jnc_000103873102/?bc=100488538142",
    "https://suumo.jp/chintai/jnc_000105185316/?bc=100455451788",
    "https://suumo.jp/chintai/jnc_000104416540/?bc=100485686334",
    "https://suumo.jp/chintai/jnc_000105350420/?bc=100485938617",
    "https://suumo.jp/chintai/jnc_000105405246/?bc=100493105474",
    # === 文京区 ===
    "https://suumo.jp/chintai/jnc_000104608428/?bc=100487114184",
    "https://suumo.jp/chintai/jnc_000105064744/?bc=100490393774",
    "https://suumo.jp/chintai/jnc_000105272054/?bc=100492195770",
    "https://suumo.jp/chintai/jnc_000105242023/?bc=100492970068",
    "https://suumo.jp/chintai/jnc_000105272053/?bc=100492124861",
    # === 台東区 ===
    "https://suumo.jp/chintai/jnc_000105374460/?bc=100492870479",
    "https://suumo.jp/chintai/jnc_000105374461/?bc=100492858079",
    "https://suumo.jp/chintai/jnc_000105369765/?bc=100492823691",
    "https://suumo.jp/chintai/jnc_000105374462/?bc=100492870117",
    "https://suumo.jp/chintai/jnc_000105382185/?bc=100493171305",
    # === 墨田区 ===
    "https://suumo.jp/chintai/jnc_000105265239/?bc=100492270272",
    "https://suumo.jp/chintai/jnc_000105397765/?bc=100493157349",
    "https://suumo.jp/chintai/jnc_000104347780/?bc=100491286972",
    "https://suumo.jp/chintai/jnc_000105265254/?bc=100492696205",
    "https://suumo.jp/chintai/jnc_000105336941/?bc=100492863126",
    # === 江東区 ===
    "https://suumo.jp/chintai/jnc_000102997810/?bc=100474112471",
    "https://suumo.jp/chintai/jnc_000100119360/?bc=100463286032",
    "https://suumo.jp/chintai/jnc_000100119358/?bc=100457977577",
    "https://suumo.jp/chintai/jnc_000105397929/?bc=100493113486",
    "https://suumo.jp/chintai/jnc_000105397931/?bc=100493079745",
    # === 品川区 ===
    "https://suumo.jp/chintai/jnc_000105424114/?bc=100493290044",
    "https://suumo.jp/chintai/jnc_000105390393/?bc=100493063828",
    "https://suumo.jp/chintai/jnc_000105390394/?bc=100493005421",
    "https://suumo.jp/chintai/jnc_000102527510/?bc=100471167519",
    "https://suumo.jp/chintai/jnc_000103742898/?bc=100477057466",
    # === 目黒区 ===
    "https://suumo.jp/chintai/jnc_000105160533/?bc=100491136870",
    "https://suumo.jp/chintai/jnc_000072909041/?bc=100489785284",
    "https://suumo.jp/chintai/jnc_000104491794/?bc=100486328034",
    "https://suumo.jp/chintai/jnc_000104745154/?bc=100491865158",
    "https://suumo.jp/chintai/jnc_000104717713/?bc=100471015050",
    # === 大田区 ===
    "https://suumo.jp/chintai/jnc_000105337041/?bc=100492596710",
    "https://suumo.jp/chintai/jnc_000105291616/?bc=100492156492",
    "https://suumo.jp/chintai/jnc_000105291703/?bc=100492201018",
    "https://suumo.jp/chintai/jnc_000105337021/?bc=100492694791",
    "https://suumo.jp/chintai/jnc_000104295719/?bc=100484491584",
    # === 世田谷区 ===
    "https://suumo.jp/chintai/jnc_000104249646/?bc=100488503267",
    "https://suumo.jp/chintai/jnc_000104256434/?bc=100484211613",
    "https://suumo.jp/chintai/jnc_000104249645/?bc=100491956734",
    "https://suumo.jp/chintai/jnc_000104249647/?bc=100488503269",
    "https://suumo.jp/chintai/jnc_000104256433/?bc=100484211609",
    # === 渋谷区 ===
    "https://suumo.jp/chintai/jnc_000102572851/?bc=100475614148",
    "https://suumo.jp/chintai/jnc_000104132632/?bc=100483514009",
    "https://suumo.jp/chintai/jnc_000105405979/?bc=100342133318",
    "https://suumo.jp/chintai/jnc_000104417227/?bc=100450816605",
    "https://suumo.jp/chintai/jnc_000104317169/?bc=100485265872",
    # === 中野区 ===
    "https://suumo.jp/chintai/jnc_000105416053/?bc=100493177481",
    "https://suumo.jp/chintai/jnc_000105416054/?bc=100493170473",
    "https://suumo.jp/chintai/jnc_000104378970/?bc=100490463660",
    "https://suumo.jp/chintai/jnc_000104492006/?bc=100490531800",
    "https://suumo.jp/chintai/jnc_000105390754/?bc=100493077143",
    # === 杉並区 ===
    "https://suumo.jp/chintai/jnc_000105049111/?bc=100490333845",
    "https://suumo.jp/chintai/jnc_000105382802/?bc=100493133561",
    "https://suumo.jp/chintai/jnc_000105382803/?bc=100492908706",
    "https://suumo.jp/chintai/jnc_000105382804/?bc=100492913363",
    "https://suumo.jp/chintai/jnc_000105431757/?bc=100365378946",
    # === 豊島区 ===
    "https://suumo.jp/chintai/jnc_000105406158/?bc=100493103415",
    "https://suumo.jp/chintai/jnc_000105249532/?bc=100492695914",
    "https://suumo.jp/chintai/jnc_000105308991/?bc=100492765105",
    "https://suumo.jp/chintai/jnc_000105249504/?bc=100349339327",
    "https://suumo.jp/chintai/jnc_000031710539/?bc=100491919904",
    # === 北区 ===
    "https://suumo.jp/chintai/jnc_000105300960/?bc=100492412284",
    "https://suumo.jp/chintai/jnc_000103929545/?bc=100482027653",
    "https://suumo.jp/chintai/jnc_000105281056/?bc=100493254571",
    "https://suumo.jp/chintai/jnc_000105281057/?bc=100492131029",
    "https://suumo.jp/chintai/jnc_000103960685/?bc=100481833873",
    # === 荒川区 ===
    "https://suumo.jp/chintai/jnc_000105424666/?bc=100493247130",
    "https://suumo.jp/chintai/jnc_000105337398/?bc=100492550741",
    "https://suumo.jp/chintai/jnc_000105416420/?bc=100493176872",
    "https://suumo.jp/chintai/jnc_000105406253/?bc=100493089050",
    "https://suumo.jp/chintai/jnc_000105318986/?bc=100492551844",
    # === 板橋区 ===
    "https://suumo.jp/chintai/jnc_000104837006/?bc=100488827384",
    "https://suumo.jp/chintai/jnc_000104852138/?bc=100488658493",
    "https://suumo.jp/chintai/jnc_000103571279/?bc=100478608330",
    "https://suumo.jp/chintai/jnc_000105281129/?bc=100493089881",
    "https://suumo.jp/chintai/jnc_000105281127/?bc=100492916427",
    # === 練馬区 ===
    "https://suumo.jp/chintai/jnc_000038283971/?bc=100493229365",
    "https://suumo.jp/chintai/jnc_000105258770/?bc=100491944316",
    "https://suumo.jp/chintai/jnc_000094557379/?bc=100486212763",
    "https://suumo.jp/chintai/jnc_000092065559/?bc=100493088658",
    "https://suumo.jp/chintai/jnc_000104064234/?bc=100375663413",
]

print(f"Total URLs: {len(TEST_URLS)}", flush=True)


async def main():
    print(f"=== Test Start: {datetime.now()} ===", flush=True)
    check_ids = []

    # Phase 1: Submit all URLs rapidly
    print("\n--- Phase 1: Submitting all URLs ---", flush=True)
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(TEST_URLS):
            try:
                async with session.post(
                    f"{API_BASE}/check",
                    json={"url": url},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        check_ids.append(data["id"])
                        portal = data.get("portal_source", "?")
                        print(f"  [{i+1:3d}/100] Submitted #{data['id']} ({portal})", flush=True)
                    else:
                        print(f"  [{i+1:3d}/100] FAILED: HTTP {resp.status}", flush=True)
                        check_ids.append(None)
            except Exception as e:
                print(f"  [{i+1:3d}/100] ERROR: {e}", flush=True)
                check_ids.append(None)
            # Small delay to avoid overwhelming
            if (i + 1) % 10 == 0:
                await asyncio.sleep(2)
            else:
                await asyncio.sleep(0.3)

    valid_ids = [cid for cid in check_ids if cid is not None]
    print(f"\nSubmitted: {len(valid_ids)}/{len(TEST_URLS)}", flush=True)

    # Phase 2: Wait for processing
    print("\n--- Phase 2: Waiting for processing ---", flush=True)
    max_wait = 900  # 15 minutes (serialized checks take longer)
    start = time.time()
    while time.time() - start < max_wait:
        # Check how many are still pending
        pending = 0
        async with aiohttp.ClientSession() as session:
            for cid in valid_ids:
                try:
                    async with session.get(f"{API_BASE}/check/{cid}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data["status"] in ("pending", "parsing", "matching", "checking"):
                                pending += 1
                except Exception:
                    pass

        elapsed = int(time.time() - start)
        print(f"  {elapsed}s: {pending} still processing...", flush=True)
        if pending == 0:
            print("  All done!", flush=True)
            break
        await asyncio.sleep(15)

    # Phase 3: Collect all results
    print("\n--- Phase 3: Collecting results ---", flush=True)
    results = []
    async with aiohttp.ClientSession() as session:
        for cid in valid_ids:
            try:
                async with session.get(f"{API_BASE}/check/{cid}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        results.append(await resp.json())
            except Exception as e:
                results.append({"id": cid, "status": "fetch_error", "error_message": str(e)})

    # Phase 4: Analyze
    print(f"\n{'='*80}", flush=True)
    print(f"空確くん 100件総合テスト結果", flush=True)
    print(f"実施日時: {datetime.now()}", flush=True)
    print(f"{'='*80}", flush=True)

    status_counts = {}
    portal_counts = {}
    platform_counts = {}
    vacancy_counts = {}
    parse_ok = 0
    parse_fail = 0
    atbb_ok = 0
    atbb_fail = 0

    for r in results:
        st = r.get("status", "unknown")
        status_counts[st] = status_counts.get(st, 0) + 1

        portal = r.get("portal_source", "unknown")
        portal_counts[portal] = portal_counts.get(portal, 0) + 1

        plat = r.get("platform", "") or "(なし)"
        platform_counts[plat] = platform_counts.get(plat, 0) + 1

        vr = r.get("vacancy_result", "")
        if vr:
            vacancy_counts[vr] = vacancy_counts.get(vr, 0) + 1

        if r.get("property_name"):
            parse_ok += 1
        else:
            parse_fail += 1

        if r.get("atbb_matched"):
            atbb_ok += 1
        else:
            atbb_fail += 1

    total = len(results)
    print(f"\n■ 総件数: {total}", flush=True)

    print(f"\n■ ステータス別:", flush=True)
    for st, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
        pct = cnt / total * 100
        print(f"  {st:25s}: {cnt:3d} ({pct:5.1f}%)", flush=True)

    print(f"\n■ ポータル別:", flush=True)
    for p, cnt in sorted(portal_counts.items(), key=lambda x: -x[1]):
        # Breakdown by status for each portal
        portal_status = {}
        for r in results:
            if r.get("portal_source", "unknown") == p:
                s = r.get("status", "unknown")
                portal_status[s] = portal_status.get(s, 0) + 1
        status_str = ", ".join(f"{k}:{v}" for k, v in sorted(portal_status.items()))
        print(f"  {p:15s}: {cnt:3d}件 ({status_str})", flush=True)

    print(f"\n■ プラットフォーム別:", flush=True)
    for p, cnt in sorted(platform_counts.items(), key=lambda x: -x[1]):
        print(f"  {p:25s}: {cnt:3d}件", flush=True)

    print(f"\n■ パース成功率: {parse_ok}/{total} ({parse_ok/total*100:.1f}%)", flush=True)
    print(f"■ ATBB照合率:   {atbb_ok}/{total} ({atbb_ok/total*100:.1f}%)", flush=True)

    if vacancy_counts:
        print(f"\n■ 空室確認結果:", flush=True)
        for vr, cnt in sorted(vacancy_counts.items(), key=lambda x: -x[1]):
            print(f"  {vr:45s}: {cnt:3d}件", flush=True)

    # Detailed results
    done = [r for r in results if r.get("status") == "done"]
    not_found = [r for r in results if r.get("status") == "not_found"]
    errors = [r for r in results if r.get("status") == "error"]
    awaiting = [r for r in results if r.get("status") == "awaiting_platform"]

    if done:
        print(f"\n■ 成功した確認 ({len(done)}件):", flush=True)
        for r in done:
            print(f"  #{r['id']:3d} {r.get('property_name','')[:30]:30s} | {r.get('platform',''):20s} | {r.get('vacancy_result','')}", flush=True)

    if awaiting:
        print(f"\n■ プラットフォーム待ち ({len(awaiting)}件):", flush=True)
        for r in awaiting:
            print(f"  #{r['id']:3d} {r.get('property_name','')[:30]:30s} | ATBB企業: {r.get('atbb_company','')[:25]}", flush=True)

    if not_found:
        print(f"\n■ 確認不可 - not_found ({len(not_found)}件):", flush=True)
        for r in not_found:
            print(f"  #{r['id']:3d} {r.get('property_name','')[:30]:30s} | portal:{r.get('portal_source',''):10s} | ATBB企業: {r.get('atbb_company','')[:25]}", flush=True)

    if errors:
        print(f"\n■ エラー ({len(errors)}件):", flush=True)
        for r in errors:
            print(f"  #{r['id']:3d} {r.get('property_name','')[:30]:30s} | {r.get('error_message','')[:60]}", flush=True)

    # Key metrics
    done_cnt = status_counts.get("done", 0)
    await_cnt = status_counts.get("awaiting_platform", 0)
    nf_cnt = status_counts.get("not_found", 0)
    err_cnt = status_counts.get("error", 0)

    print(f"\n{'='*80}", flush=True)
    print(f"■ サービス品質KPI", flush=True)
    print(f"{'='*80}", flush=True)
    print(f"  自動解決率 (done):              {done_cnt:3d}/{total} = {done_cnt/total*100:.1f}%", flush=True)
    print(f"  対応可能率 (done+awaiting):      {done_cnt+await_cnt:3d}/{total} = {(done_cnt+await_cnt)/total*100:.1f}%", flush=True)
    print(f"  失敗率 (not_found+error):        {nf_cnt+err_cnt:3d}/{total} = {(nf_cnt+err_cnt)/total*100:.1f}%", flush=True)
    print(f"  パース成功率:                    {parse_ok:3d}/{total} = {parse_ok/total*100:.1f}%", flush=True)
    print(f"  ATBB照合率:                      {atbb_ok:3d}/{total} = {atbb_ok/total*100:.1f}%", flush=True)

    # Save results
    output = {
        "test_date": datetime.now().isoformat(),
        "total": total,
        "kpi": {
            "auto_resolution_rate": f"{done_cnt/total*100:.1f}%",
            "addressable_rate": f"{(done_cnt+await_cnt)/total*100:.1f}%",
            "failure_rate": f"{(nf_cnt+err_cnt)/total*100:.1f}%",
            "parse_success_rate": f"{parse_ok/total*100:.1f}%",
            "atbb_match_rate": f"{atbb_ok/total*100:.1f}%",
        },
        "status_counts": status_counts,
        "portal_counts": portal_counts,
        "platform_counts": platform_counts,
        "vacancy_counts": vacancy_counts,
        "results": results,
    }
    with open("results/comprehensive_test_v2.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n結果保存: results/comprehensive_test_v2.json", flush=True)

    # Save detailed per-property report
    report_lines = []
    report_lines.append(f"空確くん 100件テスト v2 全物件詳細レポート")
    report_lines.append(f"実施日時: {datetime.now()}")
    report_lines.append(f"=" * 100)
    report_lines.append("")

    for i, r in enumerate(results, 1):
        report_lines.append(f"--- [{i:3d}/100] ID#{r.get('id', '?')} ---")
        report_lines.append(f"  物件名:       {r.get('property_name', '(パース失敗)')}")
        report_lines.append(f"  住所:         {r.get('property_address', '')}")
        report_lines.append(f"  家賃:         {r.get('rent', '')}")
        report_lines.append(f"  面積:         {r.get('area', '')}")
        report_lines.append(f"  ポータル:     {r.get('portal_source', '')}")
        report_lines.append(f"  ATBB照合:     {'○' if r.get('atbb_matched') else '×'}")
        report_lines.append(f"  管理会社:     {r.get('atbb_company', '(不明)')}")
        report_lines.append(f"  プラットフォーム: {r.get('platform', '(なし)')}")
        report_lines.append(f"  ステータス:   {r.get('status', '')}")
        report_lines.append(f"  空室結果:     {r.get('vacancy_result', '')}")
        if r.get('error_message'):
            report_lines.append(f"  エラー:       {r.get('error_message', '')}")
        report_lines.append("")

    report_lines.append(f"=" * 100)
    report_lines.append(f"サマリー:")
    report_lines.append(f"  自動解決: {done_cnt}/{total} ({done_cnt/total*100:.1f}%)")
    report_lines.append(f"  電話確認待ち: {await_cnt}/{total}")
    report_lines.append(f"  未対応: {nf_cnt}/{total}")
    report_lines.append(f"  エラー: {err_cnt}/{total}")
    report_lines.append(f"  ATBB照合: {atbb_ok}/{total} ({atbb_ok/total*100:.1f}%)")

    with open("results/test_v2_detail.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"詳細レポート保存: results/test_v2_detail.txt", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
