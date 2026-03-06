"""管理会社 → プラットフォーム認証情報マッピング

物確.com (bukkaku) / いい生活B2B (es_b2b) / GoWeb は
管理会社ごとにサブドメイン＋認証情報が異なるマルチテナント構成。

このモジュールは管理会社名 → (platform, credential_key) のマッピングを提供し、
credential_key から環境変数を動的にロードする。

環境変数の命名規則:
  {CREDENTIAL_KEY}_URL  → サブドメインURL (例: https://cic.bukkaku.jp)
  {CREDENTIAL_KEY}_ID   → ログインID
  {CREDENTIAL_KEY}_PASS → パスワード

例: CIC_URL, CIC_ID, CIC_PASS
"""

import json
import os
import re
import unicodedata


def _normalize(text: str) -> str:
    """全角英数→半角、小文字化、前後空白除去で名前揺れを吸収"""
    return unicodedata.normalize("NFKC", text).strip().lower()


# ── イタンジBB全社リスト自動フォールバック ──
# 41,308社のイタンジ登録企業リストを遅延ロードし、
# COMPANY_MAPに未登録でもイタンジBBに存在する管理会社を自動検出

_itanji_names: list[str] | None = None  # 遅延ロード済みフラグ
_itanji_token_index: dict[str, set[int]] | None = None


def _clean_company_name(norm: str) -> str:
    """正規化済み会社名から電話番号・法人格・支店名を除去"""
    # 電話番号除去（末尾の数字+ハイフン）
    clean = re.sub(r'[\d\-]+$', '', norm).strip()
    # 法人格除去
    clean = re.sub(r'^[\(（](?:株|有|同|合)[\)）]|^株式会社|^有限会社|^合同会社', '', clean).strip()
    clean = re.sub(r'株式会社$|[\(（](?:株|有|同)[\)）]$', '', clean).strip()
    # 全角スペース以降（支店名）除去
    clean = clean.split('　')[0].strip()
    # 半角スペース以降も除去
    clean = clean.split()[0] if clean else clean
    return clean


def _load_itanji_companies():
    """イタンジBB全社リストを遅延ロードしてbigramインデックスを構築"""
    global _itanji_names, _itanji_token_index

    itanji_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "results", "itanji_companies.json"
    )
    if not os.path.exists(itanji_path):
        _itanji_names = []
        _itanji_token_index = {}
        return

    with open(itanji_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 正規化して格納
    _itanji_names = sorted({_normalize(c["name"].strip()) for c in data["companies"]})

    # bigramトークンインデックス構築
    _itanji_token_index = {}
    for idx, name in enumerate(_itanji_names):
        for i in range(len(name) - 1):
            bigram = name[i:i+2]
            if bigram not in _itanji_token_index:
                _itanji_token_index[bigram] = set()
            _itanji_token_index[bigram].add(idx)


def _check_itanji_list(company_name: str) -> bool:
    """イタンジBB全社リストに部分一致するか（bigram高速検索）"""
    global _itanji_names, _itanji_token_index

    if _itanji_names is None:
        _load_itanji_companies()

    if not _itanji_names or not _itanji_token_index:
        return False

    norm = _normalize(company_name)
    clean = _clean_company_name(norm)

    if not clean or len(clean) < 2:
        return False

    # cleanのbigramでitanji候補を絞り込み
    bigrams = [clean[i:i+2] for i in range(len(clean) - 1)]
    if not bigrams:
        return False

    candidates = _itanji_token_index.get(bigrams[0], set()).copy()
    for bg in bigrams[1:]:
        candidates &= _itanji_token_index.get(bg, set())
        if not candidates:
            return False

    # 候補に対して実際の部分一致確認
    for idx in candidates:
        if clean in _itanji_names[idx]:
            return True

    return False


# ── 管理会社名（部分一致キー） → (platform_type, credential_key) ──
# 部分一致: ATBB管理会社名に含まれていればヒット
# 長いキーを先に検査するため、長い順に登録
COMPANY_MAP: list[tuple[str, str, str]] = [
    # (会社名の部分一致キー, platform_type, credential_key)

    # ====================================================================
    # GoWeb系 (goweb.work / 100kadou.net)
    # ====================================================================
    ("アンビション", "goweb", "AMBITION"),  # DXホールディングス/ルームピア含む
    ("宅都プロパティ", "goweb", "TAKUTO"),
    ("宅都ホールディングス", "goweb", "TAKUTO"),
    ("シマダハウス", "goweb", "SHIMADA_HOUSE"),
    ("島田ハウス", "goweb", "SHIMADA_HOUSE"),
    ("KACHIAL", "itanji", ""),  # GoWeb廃止 → イタンジBBで確認
    ("カチアル", "itanji", ""),
    ("エムズコミュニケーション", "goweb", "MS_COMM_WEB"),
    ("GOODリアルエステート", "goweb", "GOOD_REAL_ESTATE"),
    ("グッドリアルエステート", "goweb", "GOOD_REAL_ESTATE"),
    ("TAKUTO", "goweb", "TAKUTO"),
    ("高知ハウス", "goweb", "KOCHIHOUSE"),
    ("ブルーボックス", "goweb", "BLUEBOX"),
    ("JAアメニティーハウス", "goweb", "JA_AMENITY"),

    # ====================================================================
    # 物確.com (bukkaku.jp)
    # ====================================================================
    ("アドバンス・シティ・プランニング", "bukkaku", "ACP"),
    ("アドバンスシティプランニング", "bukkaku", "ACP"),
    ("セイビ・プロパティ・マネジメント", "bukkaku", "SEIBI_PM"),
    ("セイビプロパティマネジメント", "bukkaku", "SEIBI_PM"),
    ("サムティプロパティマネジメント", "itanji", ""),  # bukkaku廃止 → イタンジBBで確認
    ("サムティ・プロパティ", "itanji", ""),
    ("アーキテクトデベロッパー", "bukkaku", "MDI"),
    ("ジョイント・プロパティ", "bukkaku", "JOINT_PROPERTY"),
    ("ジョイントプロパティ", "bukkaku", "JOINT_PROPERTY"),
    ("トーセイ・コミュニティ", "bukkaku", "TOSEI_COM"),
    ("トーセイコミュニティ", "bukkaku", "TOSEI_COM"),
    ("環境ステーション", "ierabu_bb", "KANKYO_STATION"),
    ("ビレッジハウス", "bukkaku", "VILLAGE_HOUSE"),
    ("グッドワークス", "bukkaku", "GOODWORKS"),
    ("イントランス", "bukkaku", "INTRANCE"),
    ("デュアルタップ", "itanji", ""),  # bukkaku廃止 → イタンジBBで確認
    # ("ハウスメイト", "bukkaku", "HOUSEMATE"),  # URL未設定（.envにHOUSEMATE_URLなし）
    ("リアルワン", "itanji", ""),  # bukkaku廃止 → イタンジBBで確認
    ("GVレント", "itanji", ""),  # bukkaku廃止 → イタンジBBで確認
    ("グランヴァン", "itanji", ""),
    ("ダイニチ", "bukkaku", "DAINICHI"),
    ("アミックス", "bukkaku", "AMIX"),
    ("C・I・C", "bukkaku", "CIC"),
    ("シーアイシー", "bukkaku", "CIC"),
    ("CIC", "bukkaku", "CIC"),
    ("MDI", "bukkaku", "MDI"),

    # ====================================================================
    # いい生活B2B (es-b2b.com)
    # ====================================================================
    ("明和住販流通センター", "es_b2b", "MEIWA_JUHAN"),
    ("アオヤマ・メインランド", "es_b2b", "AOYAMA_MAINLAND"),
    ("青山メインランド", "es_b2b", "AOYAMA_MAINLAND"),
    ("青山メイン企画", "es_b2b", "AOYAMA_MAIN_KIKAKU"),
    ("シノケンファシリティーズ", "es_b2b", "SHINOKEN_ES"),
    ("シノケン・ファシリティーズ", "es_b2b", "SHINOKEN_ES"),
    ("トラストアドバイザーズ", "es_b2b", "TRUST_ADVISERS"),
    ("アルプス住宅サービス", "es_b2b", "ALPS_JUTAKU"),
    ("ロイヤルエンタープライズ", "es_b2b", "ROYAL_ENTERPRISE"),
    ("明豊プロパティーズ", "itanji", ""),  # es-b2b廃止 → イタンジBBで確認
    ("トーシンコミュニティー", "es_b2b", "TOSHIN"),
    ("ヒロ・コーポレーション", "es_b2b", "HIRO_CORP"),
    ("ヒロコーポレーション", "es_b2b", "HIRO_CORP"),
    ("TFDコーポレーション", "es_b2b", "TFD"),
    ("ステージプランナー", "es_b2b", "STAGE_PLANNER"),
    ("ポートホームズ", "es_b2b", "PORT_HOMES"),
    ("日神管財", "es_b2b", "NISSHIN_KANZAI"),
    ("髙松エステート", "es_b2b", "TAKAMATSU_ESTATE"),
    ("高松エステート", "es_b2b", "TAKAMATSU_ESTATE"),
    ("明和管財", "itanji", ""),  # es-b2b廃止 → イタンジBBで確認
    # ("中央ビル管理", "es_b2b", "CHUO_BIRU"),  # roomspot.es-b2b.com 403
    ("ベルテックス", "es_b2b", "VERTEX"),
    ("グッドコム", "es_b2b", "GOODCOM"),
    ("長栄", "es_b2b", "CHOEI"),
    ("JPMC", "es_b2b", "JPMC"),
    ("PDM", "es_b2b", "PDM"),

    # ====================================================================
    # イタンジBBで確認（元プラットフォーム廃止 or イタンジ移行済み）
    # ====================================================================
    ("LIXILリアルティ", "itanji", ""),
    ("アートアベニュー", "itanji", ""),
    ("Robot Home", "itanji", ""),
    ("ロボットホーム", "itanji", ""),
    ("シー・エフ・ネッツ", "itanji", ""),
    ("相互住宅", "itanji", ""),
    ("マルイホーム", "itanji", ""),
    ("ホーミングライフ", "itanji", ""),
    ("伊藤忠アーバンコミュニティ", "itanji", ""),
    ("大京穴吹不動産", "itanji", ""),
    ("東京建物不動産販売", "itanji", ""),
    ("木下の賃貸", "itanji", ""),
    ("リロの賃貸", "itanji", ""),
    ("モリモトクオリティ", "itanji", ""),
    ("長谷工ライブネット", "itanji", ""),
    ("三井ホームエステート", "itanji", ""),
    ("三井不動産レジデンシャルリース", "itanji", ""),
    ("三菱地所ハウスネット", "itanji", ""),
    ("三菱地所リアルエステート", "itanji", ""),
    ("アソシア・プロパティ", "itanji", ""),
    ("アソシアプロパティ", "itanji", ""),

    # ====================================================================
    # イタンジBBで確認（500件テスト #3,10,17,18,25,31,36 + ユーザー指定）
    # ====================================================================
    ("GV-Rent", "itanji", ""),           # #3 ＧＶ－Ｒｅｎｔ(株)
    ("フォアライフ", "itanji", ""),       # #10 (株)フォアライフ・シティ
    ("東急リバブル", "itanji", ""),       # #17,18 東急リバブル各営業所
    ("DRマネージメント", "itanji", ""),   # #25 (株)ＤＲマネージメント
    ("エステートトーワ", "itanji", ""),   # #31 (株)エステートトーワ
    ("ロイヤルホームサービス", "itanji", ""), # #36 (株)ロイヤルホームサービス

    # ====================================================================
    # イタンジBBで確認（テスト結果 + 全社リストから追加）
    # ====================================================================
    ("住友林業レジデンシャル", "itanji", ""),
    ("松崎商事", "itanji", ""),
    ("スターツピタットハウス", "itanji", ""),
    ("東急住宅リース", "itanji", ""),
    ("ユナイト", "itanji", ""),
    ("総合プロパティ", "itanji", ""),
    ("グローリア・ライフ・クリエイト", "itanji", ""),
    ("グローリアライフクリエイト", "itanji", ""),
    ("S-FITパートナーズ", "itanji", ""),
    ("ウィンズプロモーション", "itanji", ""),
    ("大和財託リーシング", "itanji", ""),
    ("Plan C", "itanji", ""),
    ("レーベントラスト", "itanji", ""),
    ("リライフ", "itanji", ""),
    ("リロケーション・ジャパン", "itanji", ""),
    ("リロケーションジャパン", "itanji", ""),
    ("リロケーション情報センター", "itanji", ""),
    ("ジェイレックス", "itanji", ""),
    ("横濱コーポレーション", "itanji", ""),
    ("新日本コンサルティング", "itanji", ""),
    ("フォーメンバーズ", "itanji", ""),
    ("マックインターナショナル", "itanji", ""),
    ("ミサワホーム不動産", "itanji", ""),
    ("コスモバンク", "itanji", ""),
    ("プロパティエージェント", "itanji", ""),
    ("ハウスメイトショップ", "itanji", ""),
    ("メイクスプラス", "itanji", ""),
    ("グローバルコミュニティ", "itanji", ""),
    ("日商ベックス", "itanji", ""),
    ("武蔵小杉駅前不動産", "itanji", ""),
    ("タウン管理サービス", "itanji", ""),
    ("バディ", "itanji", ""),
    ("パナソニックホームズ", "itanji", ""),
    ("ランドネット", "itanji", ""),
    ("エイペスト", "itanji", ""),
    ("クレオン", "itanji", ""),
    ("ケイアイスター", "itanji", ""),
    ("エストゥルース", "itanji", ""),
    ("レックス大興", "itanji", ""),
    ("MAI", "itanji", ""),
    ("ATC", "itanji", ""),
    ("AJプロパティ", "itanji", ""),

    # ====================================================================
    # イタンジBBで確認（500件テスト・イタンジ全社リスト照合で追加）
    # ====================================================================
    ("ワイキャピタルエージェンシー", "itanji", ""),  # #6
    ("ALDO", "itanji", ""),                          # #8
    ("ストライクホーム", "itanji", ""),               # #9
    ("プラン建設", "itanji", ""),                     # #12
    ("KIMITSUYA", "itanji", ""),                      # #14
    ("中央邸宅社", "itanji", ""),                     # #15
    ("タウンズ", "itanji", ""),                       # #20
    ("ファンハウス", "itanji", ""),                   # #21
    ("ボンズ・コーポレーション", "itanji", ""),       # #22
    ("ボンズコーポレーション", "itanji", ""),         # #22 (中点なし)
    ("Draws", "itanji", ""),                          # #23
    ("フレンド不動産", "itanji", ""),                 # #26
    ("都市建設", "itanji", ""),                       # #27
    ("エステート清水", "itanji", ""),                 # #29
    ("ニューライフオリジナル", "itanji", ""),         # #30
    ("インターフェイス", "itanji", ""),               # #33
    ("生和コーポレーション", "itanji", ""),           # #34
    ("イチイ", "itanji", ""),                         # #35

    # ====================================================================
    # イタンジBB実検索ヒット (Phase 3: 未対応上位200社テスト → 56社ヒット)
    # ====================================================================
    ("ベルエステート", "itanji", ""),
    ("寺村不動産", "itanji", ""),
    ("相模原土地開発", "itanji", ""),
    ("朗らか社", "itanji", ""),
    ("中央ビル管理", "itanji", ""),
    ("川津産業", "itanji", ""),
    ("ワールドインターナショナル", "itanji", ""),
    ("ノースエステート", "itanji", ""),
    ("NextSmile", "itanji", ""),
    ("協同コンサルト", "itanji", ""),
    ("平成総研", "itanji", ""),
    ("守屋住宅", "itanji", ""),
    ("房総ライフ", "itanji", ""),
    ("クイックホーム", "itanji", ""),
    ("湘南ミサワホーム", "itanji", ""),
    ("ホワイトホームズ", "itanji", ""),
    ("ケン・コーポレーション", "itanji", ""),
    ("ケンコーポレーション", "itanji", ""),
    ("アパルトマンエージェント", "itanji", ""),
    ("鈴屋商会", "itanji", ""),
    ("ユートク・ライフ", "itanji", ""),
    ("ユートクライフ", "itanji", ""),
    ("アーヴァンネット", "itanji", ""),
    ("だるま商事", "itanji", ""),
    ("SREホールディングス", "itanji", ""),
    ("マルヤ住宅", "itanji", ""),
    ("クルマ不動産", "itanji", ""),
    ("古郡ホーム", "itanji", ""),
    ("高品ハウジング", "itanji", ""),
    ("都市総合マネジメント", "itanji", ""),
    ("大成トラスト", "itanji", ""),
    ("三和エステート", "itanji", ""),
    ("工房創アセットリンク", "itanji", ""),
    ("国建エステート", "itanji", ""),
    ("房総住建", "itanji", ""),
    ("愛ホームズ", "itanji", ""),
    ("湘興ハウジング", "itanji", ""),
    ("ハウスウェル", "itanji", ""),
    ("JEY PROPERTY", "itanji", ""),
    ("ケイエスホーム", "itanji", ""),
    ("板倉地所", "itanji", ""),
    ("東京ハウスナビ", "itanji", ""),

    # ====================================================================
    # いい生活スクエアで確認
    # ====================================================================
    ("レオパレス", "es_square", ""),
    ("パシフィック・ディベロップメント", "es_square", ""),
    ("パシフィックディベロップメント", "es_square", ""),
    ("LENZ", "es_square", ""),
    ("ライブズナビ", "es_square", ""),
    ("フジミハウジング", "es_square", ""),  # Phase 3 拡張検索で発見

    # ====================================================================
    # いえらぶBB (ielove.jp)
    # ====================================================================
    ("シーラ", "ierabu_bb", ""),
    ("野村不動産パートナーズ", "ierabu_bb", "NOMURA_PARTNERS"),
    ("アムス・エステート", "ierabu_bb", ""),
    ("アムスエステート", "ierabu_bb", ""),
    ("武蔵コミュニティー", "ierabu_bb", ""),
    ("さくらスタイル", "ierabu_bb", ""),
    ("LPPプロパティマネジメント", "ierabu_bb", ""),
    # --- Phase 2 監査で発見 (未対応上位社 → ierabu_bb検索ヒット) ---
    ("西田コーポレーション", "ierabu_bb", ""),
    ("日本財託管理サービス", "ierabu_bb", ""),
    ("リノワークス", "ierabu_bb", ""),
    ("ウスイホーム", "ierabu_bb", ""),
    ("RoomConnect", "ierabu_bb", ""),
    ("オープンハウス・プロパティマネジメント", "ierabu_bb", ""),
    ("オープンハウスプロパティマネジメント", "ierabu_bb", ""),
    # --- 拡張検索 Phase 3 (200社テスト → ierabu_bb 49社ヒット) ---
    ("アパレオ", "ierabu_bb", ""),
    ("協栄不動産", "ierabu_bb", ""),
    ("ハウスフレンド", "ierabu_bb", ""),
    ("湘南営繕協会", "ierabu_bb", ""),
    ("北山ハウス産業", "ierabu_bb", ""),
    ("吉田不動産", "ierabu_bb", ""),
    ("伸和商事", "ierabu_bb", ""),
    ("埼玉住宅情報センター", "ierabu_bb", ""),
    ("リアルインベストメント", "ierabu_bb", ""),
    ("ロイヤルハウジング", "ierabu_bb", ""),
    ("京商プロパティー", "ierabu_bb", ""),
    ("千代田建設", "ierabu_bb", ""),
    ("アクタスプラン", "ierabu_bb", ""),
    ("岩瀬建設", "ierabu_bb", ""),
    ("クリエイト西武", "ierabu_bb", ""),
    ("シンコー流通サービス", "ierabu_bb", ""),
    ("ユーミーClass", "ierabu_bb", ""),
    ("ユーミーＣｌａｓｓ", "ierabu_bb", ""),
    ("タウンハウジング", "ierabu_bb", ""),  # 千葉/東京/神奈川 全支店カバー
    ("グランリファイン", "ierabu_bb", ""),
    ("三協住宅社", "ierabu_bb", ""),
    ("日鉄興和不動産コミュニティ", "ierabu_bb", ""),
    ("LIKEホーム", "ierabu_bb", ""),
    ("ＬＩＫＥホーム", "ierabu_bb", ""),
    ("山福不動産", "ierabu_bb", ""),
    ("アルプス建設", "ierabu_bb", ""),
    ("山口不動産", "ierabu_bb", ""),
    ("臼井不動産", "ierabu_bb", ""),
    ("松堀不動産", "ierabu_bb", ""),
    ("明星商事", "ierabu_bb", ""),
    ("スタートライングループ", "ierabu_bb", ""),
    ("横田ハウジング", "ierabu_bb", ""),
    ("スペース・プラン", "ierabu_bb", ""),
    ("スペースプラン", "ierabu_bb", ""),
    ("アーキテクトディベロッパー", "ierabu_bb", ""),
    ("AsuxiA", "ierabu_bb", ""),
    ("ＡｓｕｘｉＡ", "ierabu_bb", ""),
    ("ユーミーネット湘南", "ierabu_bb", ""),
    ("アルシュ・コーポレーション", "ierabu_bb", ""),
    ("アルシュコーポレーション", "ierabu_bb", ""),
    ("JAかながわ西湘不動産", "ierabu_bb", ""),
    ("ＪＡかながわ西湘不動産", "ierabu_bb", ""),
    ("パキラハウス", "ierabu_bb", ""),
    ("コスモジャパン", "ierabu_bb", ""),
    ("ハウスコム", "ierabu_bb", ""),
    ("セカンドブリュー", "ierabu_bb", ""),
    ("アイ・エヌ・ティー", "ierabu_bb", ""),
    ("アイエヌティー", "ierabu_bb", ""),
    ("ウエルホーム", "ierabu_bb", ""),
    ("喜正産業", "ierabu_bb", ""),
    ("石原興業", "ierabu_bb", ""),
    ("サニーハウス", "ierabu_bb", ""),
    ("KTプランニング", "ierabu_bb", ""),
    ("ＫＴプランニング", "ierabu_bb", ""),
    ("ニュータウン", "ierabu_bb", ""),  # 戸塚/横浜/鶴見/鷺沼 全支店カバー

    # ====================================================================
    # いい生活B2B追加
    # ====================================================================
    ("TFDエステート", "es_b2b", "TFD"),

    # ====================================================================
    # DKポータル（大東建託パートナーズ全営業所共通）
    # ====================================================================
    ("大東建託パートナーズ", "dkpartners", ""),

    # ====================================================================
    # GoWeb追加
    # ====================================================================
    ("前田", "goweb", "MS_COMM_WEB"),

    # ====================================================================
    # 未実装プラットフォーム（チェッカー未実装 → 電話フォールバック）
    # ====================================================================
    ("エイブル保証", "phone", ""),       # 自社サイト（チェッカーなし）
    ("MAXIV", "realpro", ""),            # リアルネットプロ
    ("マキシヴ", "realpro", ""),          # リアルネットプロ（カナ表記）
    ("エイブル", "realpro", ""),         # リアルネットプロ（東日本含む全支店）
    ("オリバー", "skips", ""),
    ("ASSETIA", "skips", ""),
    ("スカイコート賃貸センター", "kimaroom", ""),

    # ====================================================================
    # 電話確認が必要な管理会社
    # ====================================================================
    ("大和リビング", "phone", ""),
    ("ニチワ", "phone", ""),
    ("U-HOUSE", "phone", ""),
    ("リーヴライフ", "phone", ""),
    ("インフィニットルームワークス", "phone", ""),
    ("ミネルバ", "phone", ""),
    ("アインスホーム", "phone", ""),
    ("東都", "phone", ""),
    ("住友不動産", "phone", ""),
    ("バレッグス", "phone", ""),
    ("アクセス・リアルティー", "phone", ""),
    ("アクセスリアルティー", "phone", ""),
    ("住商レジデンシャル", "phone", ""),
    ("エムズ・ウエスト", "phone", ""),
    ("エムズウエスト", "phone", ""),
    ("アローズ", "phone", ""),
    ("日本住宅", "phone", ""),
    ("むぎばたけ", "phone", ""),
    ("i-Town", "phone", ""),
    ("TOMAN", "phone", ""),
    ("J.P.Returns", "phone", ""),
    ("ルームグリーン", "phone", ""),
    ("ユリカレント", "phone", ""),
    ("ライフアドバンス", "phone", ""),
    ("ニッテイライフ", "phone", ""),
    ("こゆき", "phone", ""),
    ("なごみ", "phone", ""),
    ("D2", "phone", ""),

    # 500件テスト: itanji/es_square/ierabu_bb全て未検出 → 電話確認
    ("京王不動産", "phone", ""),           # #1,5 (13+3件)
    ("小寺商店", "phone", ""),             # #4 (3件)
    ("六耀", "phone", ""),                 # #7 (3件)
    ("ポルンガ", "phone", ""),             # #13 (2件)
    ("内田物産", "phone", ""),             # #16 (2件)
    ("ドリームコネクション", "ierabu_bb", ""), # #19 いえらぶBBに40件登録あり
    ("まいら", "phone", ""),               # #24 (1件)
    ("愛三土地建物", "phone", ""),         # #28 (1件)
    ("栗原建設", "phone", ""),             # #32 (1件)
]


def lookup_credentials(company_name: str) -> tuple[str, str, str, str] | None:
    """管理会社名から認証情報を検索

    Args:
        company_name: ATBB管理会社名（例: "(株)CIC 03-1234-5678"）

    Returns:
        (platform, url, login_id, password) or None
    """
    if not company_name:
        return None

    normalized = _normalize(company_name)

    for key, platform, cred_key in COMPANY_MAP:
        if _normalize(key) in normalized:
            # シングルテナント（itanji/es_square）は環境変数チェック不要
            if not cred_key:
                return (platform, "", "", "")
            url = os.getenv(f"{cred_key}_URL", "")
            login_id = os.getenv(f"{cred_key}_ID", "")
            password = os.getenv(f"{cred_key}_PASS", "")
            if url and login_id and password:
                return (platform, url, login_id, password)
            # 環境変数未設定の場合はスキップ（他のマッチを試す）
            continue

    # フォールバック: イタンジBB全社リスト照合
    if _check_itanji_list(company_name):
        return ("itanji", "", "", "")

    return None


def get_platform_key(company_name: str) -> str | None:
    """管理会社名からプラットフォームキーを返す

    マルチテナント: 'bukkaku:CIC' / 'goweb:AMBITION' 形式の複合キー
    シングルテナント: 'itanji' / 'es_square' の単純キー

    Args:
        company_name: ATBB管理会社名

    Returns:
        'bukkaku:CIC' / 'es_b2b:TFD' / 'goweb:AMBITION' / 'itanji' / None
    """
    if not company_name:
        return None

    normalized = _normalize(company_name)

    for key, platform, cred_key in COMPANY_MAP:
        if _normalize(key) in normalized:
            # シングルテナント（itanji/es_square）は単純キーを返す
            if not cred_key:
                return platform
            # マルチテナント: 環境変数の有無に関わらずプラットフォームキーを返す
            # （認証情報の実在確認は lookup_credentials で行う）
            return f"{platform}:{cred_key}"

    # フォールバック: イタンジBB全社リスト照合
    if _check_itanji_list(company_name):
        return "itanji"

    return None


def parse_platform_key(platform_key: str) -> tuple[str, str]:
    """複合キーを分解: 'bukkaku:CIC' → ('bukkaku', 'CIC')

    単純キー 'itanji' の場合は ('itanji', '') を返す
    """
    if ":" in platform_key:
        platform, cred_key = platform_key.split(":", 1)
        return (platform, cred_key)
    return (platform_key, "")


def get_credentials(credential_key: str) -> tuple[str, str, str]:
    """credential_key から環境変数を読み込む

    Args:
        credential_key: 'CIC', 'TFD', 'AMBITION' など

    Returns:
        (url, login_id, password)

    Raises:
        ValueError: 環境変数が未設定の場合
    """
    url = os.getenv(f"{credential_key}_URL", "")
    login_id = os.getenv(f"{credential_key}_ID", "")
    password = os.getenv(f"{credential_key}_PASS", "")

    if not url or not login_id or not password:
        raise ValueError(
            f"{credential_key}_URL / {credential_key}_ID / {credential_key}_PASS "
            f"のいずれかが未設定です"
        )

    return (url, login_id, password)
