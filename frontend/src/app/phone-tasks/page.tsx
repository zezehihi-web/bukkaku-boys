"use client";

import { useState, useEffect } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type PhoneTask = {
  id: number;
  check_request_id: number | null;
  company_name: string;
  company_phone: string;
  property_name: string;
  property_address: string;
  reason: string;
  status: string;
  note: string;
  created_at: string;
  completed_at: string | null;
};

const STATUS_STYLES: Record<string, string> = {
  pending: "bg-yellow-100 text-yellow-800",
  completed: "bg-green-100 text-green-800",
  cancelled: "bg-gray-100 text-gray-500",
};

const STATUS_LABELS: Record<string, string> = {
  pending: "未対応",
  completed: "完了",
  cancelled: "キャンセル",
};

export default function PhoneTasksPage() {
  const [tasks, setTasks] = useState<PhoneTask[]>([]);
  const [filter, setFilter] = useState<string>("pending");
  const [editingId, setEditingId] = useState<number | null>(null);
  const [note, setNote] = useState("");
  const [updating, setUpdating] = useState(false);

  const fetchTasks = async () => {
    try {
      const params = filter ? `?status=${filter}` : "";
      const res = await fetch(`${API_BASE}/api/phone-tasks${params}`);
      if (res.ok) {
        setTasks(await res.json());
      }
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    fetchTasks();
    const interval = setInterval(fetchTasks, 10000);
    return () => clearInterval(interval);
  }, [filter]);

  const handleUpdate = async (taskId: number, status: string) => {
    setUpdating(true);
    try {
      const res = await fetch(`${API_BASE}/api/phone-tasks/${taskId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status, note }),
      });
      if (res.ok) {
        setEditingId(null);
        setNote("");
        fetchTasks();
      }
    } catch {
      // ignore
    }
    setUpdating(false);
  };

  const pendingCount = tasks.filter((t) => t.status === "pending").length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">
            電話確認タスク
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            ウェブで確認できなかった物件の電話確認リスト
          </p>
        </div>
        {pendingCount > 0 && (
          <span className="px-3 py-1.5 bg-yellow-100 text-yellow-800 rounded-full text-sm font-medium">
            {pendingCount}件 未対応
          </span>
        )}
      </div>

      {/* フィルター */}
      <div className="flex gap-2">
        {[
          ["pending", "未対応"],
          ["completed", "完了"],
          ["cancelled", "キャンセル"],
          ["", "全て"],
        ].map(([value, label]) => (
          <button
            key={value}
            onClick={() => setFilter(value)}
            className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              filter === value
                ? "bg-blue-600 text-white"
                : "bg-white border border-gray-300 text-gray-600 hover:bg-gray-50"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* タスクリスト */}
      {tasks.length === 0 ? (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8 text-center">
          <p className="text-gray-400">
            {filter === "pending"
              ? "未対応のタスクはありません"
              : "タスクがありません"}
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {tasks.map((task) => (
            <div
              key={task.id}
              className="bg-white rounded-xl shadow-sm border border-gray-200 p-5"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <span
                      className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                        STATUS_STYLES[task.status] || "bg-gray-100 text-gray-600"
                      }`}
                    >
                      {STATUS_LABELS[task.status] || task.status}
                    </span>
                    <span className="text-xs text-gray-400">
                      #{task.id}
                    </span>
                  </div>
                  <h3 className="text-sm font-semibold text-gray-900 truncate">
                    {task.property_name || "(物件名なし)"}
                  </h3>
                  {task.property_address && (
                    <p className="text-xs text-gray-500 mt-0.5">
                      {task.property_address}
                    </p>
                  )}
                </div>
                <div className="text-right shrink-0">
                  <p className="text-sm font-medium text-gray-900">
                    {task.company_name}
                  </p>
                  {task.company_phone && (
                    <a
                      href={`tel:${task.company_phone}`}
                      className="text-sm text-blue-600 hover:underline font-mono"
                    >
                      {task.company_phone}
                    </a>
                  )}
                </div>
              </div>

              {task.reason && (
                <p className="mt-2 text-xs text-gray-500 bg-gray-50 rounded px-2 py-1">
                  理由: {task.reason}
                </p>
              )}

              {task.note && task.status !== "pending" && (
                <p className="mt-2 text-xs text-gray-600 bg-blue-50 rounded px-2 py-1">
                  メモ: {task.note}
                </p>
              )}

              <div className="mt-3 flex items-center justify-between">
                <span className="text-xs text-gray-400">
                  {task.created_at}
                </span>

                {task.status === "pending" && (
                  <div className="flex gap-2">
                    {editingId === task.id ? (
                      <div className="flex items-center gap-2">
                        <input
                          type="text"
                          value={note}
                          onChange={(e) => setNote(e.target.value)}
                          placeholder="メモ（任意）"
                          className="px-2 py-1 border border-gray-300 rounded text-xs w-40"
                        />
                        <button
                          onClick={() => handleUpdate(task.id, "completed")}
                          disabled={updating}
                          className="px-3 py-1 bg-green-600 text-white rounded text-xs font-medium hover:bg-green-700 disabled:opacity-50"
                        >
                          完了
                        </button>
                        <button
                          onClick={() => handleUpdate(task.id, "cancelled")}
                          disabled={updating}
                          className="px-3 py-1 bg-gray-400 text-white rounded text-xs font-medium hover:bg-gray-500 disabled:opacity-50"
                        >
                          取消
                        </button>
                        <button
                          onClick={() => {
                            setEditingId(null);
                            setNote("");
                          }}
                          className="px-2 py-1 text-gray-400 text-xs hover:text-gray-600"
                        >
                          戻る
                        </button>
                      </div>
                    ) : (
                      <button
                        onClick={() => setEditingId(task.id)}
                        className="px-3 py-1.5 bg-blue-600 text-white rounded-lg text-xs font-medium hover:bg-blue-700"
                      >
                        対応する
                      </button>
                    )}
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
