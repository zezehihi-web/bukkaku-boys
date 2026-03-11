"""管理者API認証ミドルウェア"""

import os
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer(auto_error=False)

ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "")


async def require_admin(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> str:
    """管理者APIキーを検証する依存関数"""
    if not ADMIN_API_KEY:
        # API Key未設定の場合は認証をスキップ（開発環境用）
        return "dev"
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="認証が必要です",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if credentials.credentials != ADMIN_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="APIキーが無効です",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials
