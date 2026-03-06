"""
空確くん 500件テスト
1. SUUMOから東京23区の賃貸物件URLを~500件収集
2. APIに投入して空室確認
3. 結果を集計し、未解決管理会社をリスト化
"""
import asyncio
import aiohttp
import json
import re
import sys
import time
import os
from datetime import datetime
from collections import defaultdict

API_BASE = "http://localhost:8000/api"
RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")

# 東京23区のSUUMOエリアコード（ローマ字形式）
WARD_CODES = {
    "千代田区": "sc_chiyoda", "中央区": "sc_chuo", "港区": "sc_minato",
    "新宿区": "sc_shinjuku", "文京区": "sc_bunkyo", "台東区": "sc_taito",
    "墨田区": "sc_sumida", "江東区": "sc_koto", "品川区": "sc_shinagawa",
    "目黒区": "sc_meguro", "大田区": "sc_ota", "世田谷区": "sc_setagaya",
    "渋谷区": "sc_shibuya", "中野区": "sc_nakano", "杉並区": "sc_suginami",
    "豊島区": "sc_toshima", "北区": "sc_kita", "荒川区": "sc_arakawa",
    "板橋区": "sc_itabashi", "練馬区": "sc_nerima", "足立区": "sc_adachi",
    "葛飾区": "sc_katsushika", "江戸川区": "sc_edogawa",
}

# 各区から取得するURL数の目標
TARGET_PER_WARD = 22  # 23区 × 22 = 506件


async def collect_suumo_urls(session: aiohttp.ClientSession, ward_name: str, ward_code: str, count: int) -> list[str]:
    """SUUMOの一覧ページからJNC物件URLを収集"""
    urls = []
    seen_jnc = set()
    page = 1

    while len(urls) < count and page <= 5:
        list_url = f"https://suumo.jp/chintai/tokyo/{ward_code}/?page={page}"
        try:
            async with session.get(list_url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    break
                html = await resp.text()
        except Exception as e:
            print(f"  [WARN] {ward_name} page {page}: {e}")
            break

        # JNC URLを抽出 (建物ページ — bc=パラメータありなし両方)
        jnc_pattern = r'href="(/chintai/jnc_\d+/[^"]*)"'
        matches = re.findall(jnc_pattern, html)

        for path in matches:
            # JNC番号でユニーク化（同じ建物の別部屋を除外）
            jnc_match = re.search(r'jnc_(\d+)', path)
            if jnc_match:
                jnc_id = jnc_match.group(1)
                if jnc_id in seen_jnc:
                    continue
                seen_jnc.add(jnc_id)
                full_url = f"https://suumo.jp{path}"
                urls.append(full_url)
                if len(urls) >= count:
                    break

        page += 1
        await asyncio.sleep(0.5)  # SUUMO へのレート制限

    return urls[:count]


async def main():
    start_time = time.time()
    print(f"=== 500件テスト開始: {datetime.now()} ===", flush=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # ===== Phase 1: SUUMO URL収集 =====
    print("\n--- Phase 1: SUUMO URL収集 ---", flush=True)
    all_urls = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        for ward_name, ward_code in WARD_CODES.items():
            ward_urls = await collect_suumo_urls(session, ward_name, ward_code, TARGET_PER_WARD)
            all_urls.extend(ward_urls)
            print(f"  {ward_name}: {len(ward_urls)}件", flush=True)
            await asyncio.sleep(0.3)

    # 重複除去
    unique_urls = list(dict.fromkeys(all_urls))
    target = min(len(unique_urls), 500)
    test_urls = unique_urls[:target]
    print(f"\n収集完了: {len(test_urls)}件 (全{len(unique_urls)}件中)", flush=True)

    if len(test_urls) < 100:
        print("[ERROR] URL収集が少なすぎます。SUUMOの構造変更の可能性。", flush=True)
        return

    # ===== Phase 2: API投入 =====
    print(f"\n--- Phase 2: {len(test_urls)}件をAPIに投入 ---", flush=True)
    check_ids = []

    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(test_urls):
            try:
                async with session.post(
                    f"{API_BASE}/check",
                    json={"url": url},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        check_ids.append(data["id"])
                    else:
                        check_ids.append(None)
            except Exception as e:
                check_ids.append(None)

            if (i + 1) % 50 == 0:
                print(f"  投入: {i+1}/{len(test_urls)}", flush=True)
                await asyncio.sleep(3)
            elif (i + 1) % 10 == 0:
                await asyncio.sleep(1)
            else:
                await asyncio.sleep(0.2)

    valid_ids = [cid for cid in check_ids if cid is not None]
    print(f"\n投入完了: {len(valid_ids)}/{len(test_urls)}", flush=True)

    # ===== Phase 3: 処理待ち =====
    print(f"\n--- Phase 3: 処理待ち ---", flush=True)
    max_wait = 2400  # 40分
    start_wait = time.time()

    while time.time() - start_wait < max_wait:
        pending = 0
        async with aiohttp.ClientSession() as session:
            # サンプリングで確認（全件チェックは重いので50件ずつ）
            sample = valid_ids if len(valid_ids) <= 100 else valid_ids[::max(1, len(valid_ids)//100)]
            for cid in sample:
                try:
                    async with session.get(f"{API_BASE}/check/{cid}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data["status"] in ("pending", "parsing", "matching", "checking"):
                                pending += 1
                except Exception:
                    pass

        # サンプリング比から全体の未処理数を推定
        if len(valid_ids) > 100:
            est_pending = int(pending * len(valid_ids) / len(sample))
        else:
            est_pending = pending

        elapsed = int(time.time() - start_wait)
        print(f"  {elapsed}s: 推定 {est_pending} 件処理中...", flush=True)
        if est_pending == 0:
            print("  全件完了!", flush=True)
            break
        await asyncio.sleep(20)

    # ===== Phase 4: 結果収集 =====
    print(f"\n--- Phase 4: 結果収集 ---", flush=True)
    results = []
    async with aiohttp.ClientSession() as session:
        for i, cid in enumerate(valid_ids):
            try:
                async with session.get(f"{API_BASE}/check/{cid}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        results.append(await resp.json())
            except Exception as e:
                results.append({"id": cid, "status": "fetch_error"})

            if (i + 1) % 100 == 0:
                print(f"  収集: {i+1}/{len(valid_ids)}", flush=True)

    # ===== Phase 5: 分析 =====
    print(f"\n{'='*80}", flush=True)
    print(f"空確くん 500件テスト結果", flush=True)
    print(f"実施日時: {datetime.now()}", flush=True)
    print(f"所要時間: {int(time.time()-start_time)}秒", flush=True)
    print(f"{'='*80}", flush=True)

    total = len(results)
    status_counts = defaultdict(int)
    platform_counts = defaultdict(int)
    vacancy_counts = defaultdict(int)
    parse_ok = atbb_ok = 0

    # 管理会社別集計
    company_results = defaultdict(lambda: {"done": 0, "awaiting": 0, "not_found": 0, "total": 0, "phone": "", "properties": []})

    for r in results:
        st = r.get("status", "unknown")
        status_counts[st] += 1

        plat = r.get("platform", "") or "(なし)"
        platform_counts[plat] += 1

        vr = r.get("vacancy_result", "")
        if vr:
            vacancy_counts[vr] += 1

        if r.get("property_name"):
            parse_ok += 1
        if r.get("atbb_matched"):
            atbb_ok += 1

        company = r.get("atbb_company", "")
        if company:
            # 会社名と電話番号を分離
            parts = company.rsplit(" ", 1)
            co_name = parts[0] if parts else company
            co_phone = parts[1] if len(parts) > 1 else ""

            info = company_results[co_name]
            info["total"] += 1
            info["phone"] = co_phone
            info["properties"].append(r.get("property_name", "?"))

            if st == "done":
                info["done"] += 1
            elif st == "awaiting_platform":
                info["awaiting"] += 1
            else:
                info["not_found"] += 1

    # KPI表示
    done_cnt = status_counts.get("done", 0)
    await_cnt = status_counts.get("awaiting_platform", 0)
    nf_cnt = status_counts.get("not_found", 0)
    err_cnt = status_counts.get("error", 0)

    print(f"\n■ 総件数: {total}", flush=True)
    print(f"\n■ ステータス別:", flush=True)
    for st, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {st:25s}: {cnt:4d} ({cnt/total*100:5.1f}%)", flush=True)

    print(f"\n■ プラットフォーム別:", flush=True)
    for p, cnt in sorted(platform_counts.items(), key=lambda x: -x[1]):
        print(f"  {p:30s}: {cnt:4d}件", flush=True)

    print(f"\n■ 空室確認結果:", flush=True)
    for vr, cnt in sorted(vacancy_counts.items(), key=lambda x: -x[1]):
        print(f"  {vr:45s}: {cnt:4d}件", flush=True)

    print(f"\n■ KPI:", flush=True)
    print(f"  自動解決率:     {done_cnt:4d}/{total} = {done_cnt/total*100:.1f}%", flush=True)
    print(f"  対応可能率:     {done_cnt+await_cnt:4d}/{total} = {(done_cnt+await_cnt)/total*100:.1f}%", flush=True)
    print(f"  パース成功率:   {parse_ok:4d}/{total} = {parse_ok/total*100:.1f}%", flush=True)
    print(f"  ATBB照合率:     {atbb_ok:4d}/{total} = {atbb_ok/total*100:.1f}%", flush=True)

    # ===== 未解決管理会社リスト =====
    unresolved = {co: info for co, info in company_results.items() if info["awaiting"] > 0}
    # 件数順にソート
    sorted_unresolved = sorted(unresolved.items(), key=lambda x: -x[1]["awaiting"])

    unresolved_path = os.path.join(RESULTS_DIR, "unresolved_500.txt")
    with open(unresolved_path, "w", encoding="utf-8") as f:
        f.write(f"500件テスト 未解決管理会社リスト ({datetime.now():%Y-%m-%d %H:%M})\n")
        f.write(f"未解決管理会社: {len(sorted_unresolved)}社\n")
        f.write(f"未解決物件: {sum(info['awaiting'] for _, info in sorted_unresolved)}件\n")
        f.write(f"選択肢: itanji / es_square / goweb:KEY / bukkaku:KEY / es_b2b:KEY / realpro / ierabu_bb / dkpartners / 電話 / 不明\n")
        f.write("=" * 70 + "\n\n")

        for idx, (co_name, info) in enumerate(sorted_unresolved, 1):
            props = list(set(info["properties"]))[:3]
            props_str = ", ".join(p for p in props if p)
            f.write(f"{idx:3d}. {co_name} {info['phone']}\n")
            f.write(f"     未解決: {info['awaiting']}件 / 全{info['total']}件\n")
            f.write(f"     物件例: {props_str}\n")
            f.write(f"     → \n\n")

    # 解決済み管理会社リスト
    resolved = {co: info for co, info in company_results.items() if info["done"] > 0 and info["awaiting"] == 0}
    sorted_resolved = sorted(resolved.items(), key=lambda x: -x[1]["done"])

    print(f"\n■ 解決済み管理会社: {len(sorted_resolved)}社", flush=True)
    print(f"■ 未解決管理会社: {len(sorted_unresolved)}社", flush=True)
    print(f"  → 未解決リスト: {unresolved_path}", flush=True)

    # JSON結果保存
    json_path = os.path.join(RESULTS_DIR, "test_500_results.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "test_date": datetime.now().isoformat(),
            "total": total,
            "elapsed_seconds": int(time.time() - start_time),
            "kpi": {
                "auto_resolution_rate": f"{done_cnt/total*100:.1f}%",
                "addressable_rate": f"{(done_cnt+await_cnt)/total*100:.1f}%",
                "parse_success_rate": f"{parse_ok/total*100:.1f}%",
                "atbb_match_rate": f"{atbb_ok/total*100:.1f}%",
            },
            "status_counts": dict(status_counts),
            "platform_counts": dict(platform_counts),
            "vacancy_counts": dict(vacancy_counts),
            "unresolved_companies": len(sorted_unresolved),
            "resolved_companies": len(sorted_resolved),
            "results": results,
        }, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n結果JSON: {json_path}", flush=True)
    print(f"\n[{datetime.now():%H:%M:%S}] テスト完了", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
