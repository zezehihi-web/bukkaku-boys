# ミニPC 運用指示書

**対象**: ミニPC上で動作するAIエージェント
**最終更新**: 2026-03-06

---

## このPCの役割

このPCは「空確くん」のバックエンド処理専用マシンです。

ユーザーが https://akiya-tools.vercel.app/akishitsu で物件URLを入力すると、
その物件が本当に空室かどうかを、各不動産プラットフォームにログインして自動確認します。

Vercel（Webサーバー）はフロントエンドしか動かせないため、
Playwright（ブラウザ自動操作）が必要な空室確認処理は全てこのPCが担当します。

**このPCが止まると、ユーザーがURLを送信しても「処理中...」のまま永久に止まります。**

---

## 全体の流れ

```
ユーザーがSUUMO等のURLを入力
        |
        v
Vercel (akiya-tools.vercel.app)
  - URLをNeon PostgreSQLに保存 (status=pending)
  - ブラウザは2秒ごとにNeonをポーリングして結果を待つ
        |
        v
Neon PostgreSQL (クラウドDB)
  - akikaku_checks テーブルに行がINSERTされる
        ^
        | 5秒ごとにポーリング
        |
このPC (neon_listener.py)
  1. pending の行を検出 -> running に更新
  2. SUUMOページをスクレイピング -> 物件名・住所・賃料等を抽出
  3. ATBBデータベース(13万件)で管理会社を特定
  4. R2インデックス(3万件)で詳細URLを取得 -> 検索ステップをスキップ
  5. Playwrightでプラットフォーム(イタンジBB等)にアクセスし空室状態を確認
  6. 結果(「募集中」「申込あり」等)をNeonに書き戻し
  7. -> ユーザーのブラウザに結果が表示される
```

通信は常にこのPC -> クラウド（一方向）です。ポート開放やグローバルIP不要。

---

## 今回の変更点（2026-03-06）

以下の修正をコミット予定です。ミニPCで `git pull` して反映してください。

### SUUMOパーサー改善 (`backend/scrapers/suumo_parser.py`)

SUUMOがjnc_ページを/library/にリダイレクトするようになり、物件名が取れなくなっていました。

- **jnc_ -> bc_ URLリライト**: jnc_URLに `?bc=XXXX` があれば `/chintai/bc_XXXX/` に変換して部屋単位の情報を取得
- **library ページのフォールバック**: bc_コードが無い場合でも og:title から物件名を抽出
- **号室抽出**: bc_ページのh1やテーブルから号室番号を抽出し `物件名/号室` 形式で付加
- **ゴミ文字除去**: 先頭の中点・全角スペース等を除去（「・ レッドアイ」->「レッドアイ」）

効果: 物件名パース成功率 **55% -> 100%**

### ATBB照合エンジン改善 (`backend/services/property_matcher.py`)

物件名が空のとき住所+面積だけで別の区の物件にマッチする誤マッチが発生していました。

- **区レベルのゲート追加**: 足立区の物件が新宿区にマッチするような誤りを防止
- **物件名が空のとき築年数必須化**: 住所+面積だけではマッチさせない
- **フォールバックスコア閾値引き上げ**: 35 -> 40

効果: 誤マッチ **3件 -> 0件**

### neon_listener修正 (`backend/neon_listener.py`)

- **importパス修正**: `backend.playwright_loop` -> `backend.services.playwright_loop`（起動時エラー解消）
- **em dash除去**: Windows cp932エンコーディングエラー解消

### 起動スクリプト修正 (`scripts/run_neon_listener.sh`)

- `PYTHONUNBUFFERED=1` と `PYTHONIOENCODING=utf-8` を追加（ログが即時出力される）

---

## セットアップ手順（初回 or 更新時）

### 更新の場合（既に動いている場合）

```bash
# 1. neon_listener を停止（Ctrl+C）

# 2. 最新コードを取得
cd "C:/Users/yamag/空確くん"
git pull origin main

# 3. 再起動
bash scripts/run_neon_listener.sh
```

### 初回セットアップ（まだ一度も動かしていない場合）

```bash
# 1. リポジトリをクローン
cd "C:/Users/yamag"
git clone https://github.com/zezehihi-web/bukkaku-boys.git 空確くん

# 2. 必須パッケージのインストール
cd "C:/Users/yamag/空確くん"
pip install psycopg2-binary httpx lxml beautifulsoup4 aiosqlite boto3 python-dotenv
playwright install chromium

# 3. .env ファイルの配置
#    開発PCから C:\Users\yamag\空確くん\.env をコピー
#    ATBB・イタンジ・いい生活スクエア等の全認証情報が入っています

# 4. 起動
bash scripts/run_neon_listener.sh
```

### 起動成功の確認

以下のログが出ればOK:
```
[playwright_loop] スレッド起動完了
[neon_listener] 起動完了 - Neon DB をポーリング中 (5秒間隔)
[neon_listener] DATABASE_URL: ...neondb?sslmode=require
```

ジョブが来ると:
```
[neon_listener] ジョブ取得: id=1, status=pending, property=グランメゾン池袋
```

何もジョブがないときはログは出ません（正常）。

---

## このPCが常時やっていること

### neon_listener.py（メインプロセス）

- **5秒ごと**にNeon DBをポーリングし、`status=pending/matching/checking` の行を処理
- 1件ずつ順番に処理（排他ロック `FOR UPDATE SKIP LOCKED` で安全）
- 処理中にクラッシュしても、再起動時に `status=running` の行を `pending` に戻す（stale recovery）

### Playwright（ブラウザ自動操作）

- 専用スレッドで常駐。以下のプラットフォームにログインしてページ操作する:

| プラットフォーム | 種別 | 備考 |
|---|---|---|
| イタンジBB | シングルテナント | 常時ログイン維持 |
| いい生活スクエア | シングルテナント | 常時ログイン維持 |
| GoWeb | マルチテナント(8社) | オンデマンドログイン |
| 物確.com | マルチテナント(17社) | オンデマンドログイン |
| いい生活B2B | マルチテナント(22社) | オンデマンドログイン |
| いえらぶBB | シングルテナント | IPバン注意(15秒間隔) |
| リアルネットプロ | シングルテナント | オンデマンドログイン |

- `platform_lock` で各プラットフォームへの同時アクセスを防止
- IPバン防止のレートリミット（プラットフォームごとに5-15秒間隔）

### R2インデックス（Cloudflare R2）

- itanji/es_square/ierabu_bb の物件インデックス(合計3万件)をR2から取得
- 5分間キャッシュ
- ヒットすればプラットフォーム上での検索をスキップし、詳細URLに直接アクセス（高速）

### ATBBデータベース（SQLite）

- `backend/akikaku.db` に13万件のATBB物件データ
- R2から6時間ごとに最新版を自動同期（`r2_atbb_sync.py`）
  - ただしこれはFastAPIの `main.py` 経由で起動した場合のみ。neon_listener 単体では自動同期しない
  - ATBBデータの更新が必要な場合は、FastAPIサーバーも別ターミナルで起動するか、手動で `python -c "from backend.services.r2_atbb_sync import sync_now; import asyncio; asyncio.run(sync_now())"` を実行

---

## 日常運用

### 基本的に放置でOK

- neon_listenerはDB接続エラーがあっても10秒後に自動リトライ
- `run_neon_listener.sh` がプロセスクラッシュを検出して10秒後に自動再起動
- stale recoveryで処理中ジョブの自動復旧

### コード更新が入った場合

開発PCでコードを修正してGitHubにpushした後:

```bash
cd "C:/Users/yamag/空確くん"
git pull origin main
# Ctrl+C で neon_listener を停止 -> run_neon_listener.sh が自動再起動
# または手動で bash scripts/run_neon_listener.sh を再実行
```

### PCを再起動した場合

ターミナル（Git Bash）を開いて:

```bash
cd "C:/Users/yamag/空確くん"
bash scripts/run_neon_listener.sh
```

これだけ。環境変数はスクリプト内で自動設定されます。

---

## 既存のFastAPIサーバーとの共存

ローカルフロントエンド（localhost:3002）を使う場合は、別ターミナルで:

```bash
cd "C:/Users/yamag/空確くん"
python -m uvicorn backend.main:app --port 8000
```

- neon_listener と FastAPI は別プロセスだが同じ Playwright を共有
- `platform_lock` で排他制御されているので同時起動して問題なし
- FastAPIはATBBスケジューラーやR2同期も起動するため、フル機能が必要なときはこちらも起動

---

## トラブルシューティング

| 症状 | 原因 | 対処 |
|---|---|---|
| ターミナルにログが出ない | 正常（ジョブがない） | 放置OK |
| `DB接続エラー` | Neonの一時的な接続断 | 自動リトライ、放置OK |
| `TargetClosedError` | ブラウザプロセス異常終了 | Ctrl+C -> 自動再起動 |
| `ModuleNotFoundError` | パッケージ未インストール | `pip install <パッケージ名>` |
| ユーザーの画面が「処理中」で止まる | neon_listenerが動いていない | `bash scripts/run_neon_listener.sh` |
| `UnicodeEncodeError` | Pythonの出力エンコーディング | `run_neon_listener.sh` 使用で解決済み |

### Neon DBを直接確認したいとき

```bash
cd "C:/Users/yamag/空確くん" && python -c "
import psycopg2, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
conn = psycopg2.connect('postgresql://neondb_owner:npg_t8L6IUlWuKgF@ep-floral-bread-a1kdt3p1-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require')
cur = conn.cursor()
cur.execute('SELECT id, status, property_name, vacancy_result FROM akikaku_checks ORDER BY id DESC LIMIT 10')
for r in cur.fetchall():
    print(f'id={r[0]} status={r[1]} name={r[2]} result={r[3]}')
cur.close(); conn.close()
"
```

---

## 停止方法

- `Ctrl+C` で安全にシャットダウン（処理中ジョブは `error` に更新される）
- `run_neon_listener.sh` 使用時は `Ctrl+C` を2回押す（1回目でリスナー停止、2回目でラッパー停止）

---

## まとめ

このPCがやること:
1. **常に `neon_listener.py` を動かし続ける**
2. ユーザーがVercelに送信した空室確認リクエストを自動で処理する
3. コード更新が入ったら `git pull` して再起動する
4. それ以外は放置でOK

```bash
# 起動コマンド（これだけ覚えておけばOK）
cd "C:/Users/yamag/空確くん"
bash scripts/run_neon_listener.sh
```
