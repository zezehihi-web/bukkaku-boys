"""URL解析サービス - SUUMO/HOMESのURLを判別して適切なパーサーに振り分け"""

from backend.scrapers.suumo_parser import parse_suumo_url
from backend.scrapers.homes_parser import parse_homes_url


async def parse_portal_url(url: str, portal_source: str) -> dict:
    """ポータルURLから物件情報を抽出

    Args:
        url: SUUMO/HOMESの物件URL
        portal_source: 'suumo' / 'homes'

    Returns:
        dict with keys: property_name, address, rent, area, layout
    """
    if portal_source == "suumo":
        return await parse_suumo_url(url)
    elif portal_source == "homes":
        return await parse_homes_url(url)
    else:
        # URL文字列から推定
        if "suumo.jp" in url:
            return await parse_suumo_url(url)
        elif "homes.co.jp" in url:
            return await parse_homes_url(url)

        raise ValueError(f"対応していないURLです: {url}")
