"""電話確認タスクAPIルーター"""

from datetime import datetime

from fastapi import APIRouter, HTTPException

from backend.database import get_db
from backend.models import PhoneTaskItem, PhoneTaskUpdate
from backend.services.knowledge_service import mark_phone_required

router = APIRouter(tags=["phone_tasks"])


@router.get("/phone-tasks", response_model=list[PhoneTaskItem])
async def list_phone_tasks(status: str = ""):
    """電話確認タスク一覧"""
    db = await get_db()
    try:
        if status:
            rows = await db.execute(
                "SELECT * FROM phone_tasks WHERE status = ? ORDER BY id DESC",
                (status,),
            )
        else:
            rows = await db.execute("SELECT * FROM phone_tasks ORDER BY id DESC")
        records = await rows.fetchall()
    finally:
        await db.close()

    return [_row_to_task(r) for r in records]


@router.put("/phone-tasks/{task_id}")
async def update_phone_task(task_id: int, update: PhoneTaskUpdate):
    """電話確認タスクを完了/キャンセル"""
    db = await get_db()
    try:
        row = await db.execute("SELECT * FROM phone_tasks WHERE id = ?", (task_id,))
        record = await row.fetchone()
        if not record:
            raise HTTPException(status_code=404, detail="タスクが見つかりません")

        await db.execute(
            """UPDATE phone_tasks
               SET status = ?, note = ?, completed_at = ?
               WHERE id = ?""",
            (update.status, update.note, datetime.now().isoformat(), task_id),
        )
        await db.commit()
    finally:
        await db.close()

    # 「電話確認必要」として管理会社を学習
    if update.status == "completed" and record["company_name"]:
        await mark_phone_required(record["company_name"], record["company_phone"] or "")

    return {"status": "ok"}


@router.get("/phone-tasks/count")
async def phone_tasks_count():
    """未完了の電話確認タスク数"""
    db = await get_db()
    try:
        row = await db.execute(
            "SELECT COUNT(*) as cnt FROM phone_tasks WHERE status = 'pending'"
        )
        record = await row.fetchone()
    finally:
        await db.close()

    return {"count": record["cnt"] if record else 0}


def _row_to_task(row) -> PhoneTaskItem:
    return PhoneTaskItem(
        id=row["id"],
        check_request_id=row["check_request_id"],
        company_name=row["company_name"] or "",
        company_phone=row["company_phone"] or "",
        property_name=row["property_name"] or "",
        property_address=row["property_address"] or "",
        reason=row["reason"] or "",
        status=row["status"] or "pending",
        note=row["note"] or "",
        created_at=row["created_at"] or "",
        completed_at=row["completed_at"],
    )
