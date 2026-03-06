"""URL解析サービス - ポータルサイトURLを判別して適切なパーサーに振り分け"""

import re

from backend.scrapers.suumo_parser import parse_suumo_url
from backend.scrapers.homes_parser import parse_homes_url
from backend.scrapers.chintai_parser import parse_chintai_url
from backend.scrapers.yahoo_parser import parse_yahoo_url
from backend.scrapers.athome_parser import parse_athome_url
from backend.scrapers.eheya_parser import parse_eheya_url
from backend.scrapers.apamanshop_parser import parse_apamanshop_url
from backend.scrapers.able_parser import parse_able_url
from backend.scrapers.minimini_parser import parse_minimini_url
from backend.scrapers.pitat_parser import parse_pitat_url
from backend.scrapers.homemate_parser import parse_homemate_url
from backend.scrapers.canary_parser import parse_canary_url
from backend.scrapers.goodroom_parser import parse_goodroom_url

# ドメイン→パーサー関数のマッピング
_PORTAL_MAP = {
    "suumo.jp": parse_suumo_url,
    "homes.co.jp": parse_homes_url,
    "chintai.net": parse_chintai_url,
    "realestate.yahoo.co.jp": parse_yahoo_url,
    "athome.co.jp": parse_athome_url,
    "eheya.net": parse_eheya_url,
    "apamanshop.com": parse_apamanshop_url,
    "able.co.jp": parse_able_url,
    "minimini.jp": parse_minimini_url,
    "pitat.com": parse_pitat_url,
    "homemate.co.jp": parse_homemate_url,
    "canary-app.jp": parse_canary_url,
    "goodrooms.jp": parse_goodroom_url,
    # TODO: SUUMO集約サイト（データはSUUMOと同一だがHTML構造は独自）
    # sumaity.com (スマイティ), door.ac (Door賃貸), smocca.jp (スモッカ)
    # → 専用パーサー追加で対応予定
    # TODO: OHEYAGO (oheyago.jp) - 部屋ページが即404になるため実用性低
    # TODO: キャッシュバック賃貸 (cbchintai.com) - bot検出あり、Playwright必要
}


def detect_portal(url: str) -> str | None:
    """URLからポータルサイト名を検出

    Returns:
        ポータル名（例: "suumo", "homes", "chintai"）またはNone
    """
    url_lower = url.lower()
    for domain in _PORTAL_MAP:
        if domain in url_lower:
            # ドメインからポータル名を生成
            name = _domain_to_portal(domain)
            return name
    return None


def _domain_to_portal(domain: str) -> str:
    """ドメインからポータル名を生成"""
    _DOMAIN_ALIASES = {
        "realestate.yahoo.co.jp": "yahoo",
        "canary-app.jp": "canary",
        "goodrooms.jp": "goodroom",
    }
    if domain in _DOMAIN_ALIASES:
        return _DOMAIN_ALIASES[domain]
    return domain.split(".")[0]


async def parse_portal_url(url: str, portal_source: str = "") -> dict:
    """ポータルURLから物件情報を抽出

    Args:
        url: 物件詳細ページのURL
        portal_source: ポータル名（省略時はURLから自動判定）

    Returns:
        dict with keys: property_name, address, rent, area, layout, build_year
    """
    # portal_source指定がある場合、対応するドメインを検索
    if portal_source:
        for domain, parser_fn in _PORTAL_MAP.items():
            name = _domain_to_portal(domain)
            if name == portal_source:
                return await parser_fn(url)

    # URLから自動判定
    url_lower = url.lower()
    for domain, parser_fn in _PORTAL_MAP.items():
        if domain in url_lower:
            return await parser_fn(url)

    raise ValueError(f"対応していないURLです: {url}")
