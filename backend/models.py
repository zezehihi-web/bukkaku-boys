"""Pydantic データモデル定義"""

from pydantic import BaseModel


# === リクエスト ===

class CheckRequest(BaseModel):
    """空室確認リクエスト"""
    url: str


class PlatformSelection(BaseModel):
    """プラットフォーム選択（ユーザー手動）"""
    platform: str  # 'itanji' / 'es_square'
    remember: bool = True  # この選択を記憶する


class KnowledgeEntry(BaseModel):
    """ナレッジDB登録・更新"""
    company_name: str
    company_phone: str = ""
    platform: str


class PhoneTaskUpdate(BaseModel):
    """電話確認タスク更新"""
    status: str = "completed"  # 'completed' / 'cancelled'
    note: str = ""


# === レスポンス ===

class CheckStatus(BaseModel):
    """空室確認ステータス"""
    id: int
    submitted_url: str
    portal_source: str
    property_name: str
    property_address: str
    property_rent: str
    property_area: str
    property_layout: str
    property_build_year: str = ""
    atbb_matched: bool
    atbb_company: str
    platform: str
    platform_auto: bool
    status: str
    vacancy_result: str
    error_message: str
    created_at: str
    completed_at: str | None


class CheckListItem(BaseModel):
    """確認結果一覧の1項目"""
    id: int
    property_name: str
    status: str
    vacancy_result: str
    portal_source: str
    created_at: str


class KnowledgeItem(BaseModel):
    """ナレッジDB項目"""
    id: int
    company_name: str
    company_phone: str
    platform: str
    requires_phone: bool = False
    use_count: int
    last_used_at: str


class PhoneTaskItem(BaseModel):
    """電話確認タスク項目"""
    id: int
    check_request_id: int | None
    company_name: str
    company_phone: str
    property_name: str
    property_address: str
    reason: str
    status: str
    note: str
    created_at: str
    completed_at: str | None
