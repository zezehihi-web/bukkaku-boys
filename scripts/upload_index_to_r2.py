#!/usr/bin/env python3
"""
手動でローカルのインデックスをR2にアップロードするユーティリティ。

スクレイパーを全実行せずに、既存のローカルインデックスファイルを
R2の itanji_index.json / es_square_index.json にアップロードする。

使い方:
    python scripts/upload_index_to_r2.py              # 両方アップロード
    python scripts/upload_index_to_r2.py itanji        # itanjiのみ
    python scripts/upload_index_to_r2.py es_square     # es_squareのみ
    python scripts/upload_index_to_r2.py --replace     # 置換モード（R2を完全上書き）
"""
import os
import sys

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()


def upload_itanji(replace_mode: bool = False):
    """itanjiのインデックスをR2にアップロード"""
    from scrape_itanji import upload_own_index_to_r2, INDEX_FILE, is_r2_ready
    if not is_r2_ready():
        print("[ERROR] R2の設定が不足しています。.env を確認してください。")
        return False
    if not os.path.exists(INDEX_FILE):
        print(f"[ERROR] ローカルインデックスが見つかりません: {INDEX_FILE}")
        return False
    import json
    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"[itanji] ローカルインデックス: {len(data)}件")
    if not data:
        print("[itanji] データが0件のためアップロードをスキップします。")
        return False
    upload_own_index_to_r2(replace_mode=replace_mode)
    return True


def upload_es_square(replace_mode: bool = False):
    """es_squareのインデックスをR2にアップロード"""
    from scrape_es_square import upload_own_index_to_r2, INDEX_FILE, is_r2_ready
    if not is_r2_ready():
        print("[ERROR] R2の設定が不足しています。.env を確認してください。")
        return False
    if not os.path.exists(INDEX_FILE):
        print(f"[ERROR] ローカルインデックスが見つかりません: {INDEX_FILE}")
        return False
    import json
    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"[es_square] ローカルインデックス: {len(data)}件")
    if not data:
        print("[es_square] データが0件のためアップロードをスキップします。")
        return False
    upload_own_index_to_r2(replace_mode=replace_mode)
    return True


def main():
    args = sys.argv[1:]
    replace_mode = "--replace" in args
    targets = [a for a in args if not a.startswith("--")]

    if replace_mode:
        print("[MODE] 置換モード: ローカルデータでR2を完全上書きします")
    else:
        print("[MODE] マージモード: R2既存データに新規分をマージします")

    if not targets:
        targets = ["itanji", "es_square"]

    for target in targets:
        if target == "itanji":
            upload_itanji(replace_mode=replace_mode)
        elif target == "es_square":
            upload_es_square(replace_mode=replace_mode)
        else:
            print(f"[WARN] 不明なターゲット: {target} (itanji / es_square を指定)")

    print("\n[完了]")


if __name__ == "__main__":
    main()
