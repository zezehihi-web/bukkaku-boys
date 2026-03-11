"use client";

import { useState } from "react";
import { API_BASE } from "../../lib/api";
import { usePolling } from "../../lib/usePolling";
import type { PhoneTask } from "../../lib/types";

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

export default function AdminPhoneTasksPage() {
  const [tasks, setTasks] = useState<PhoneTask[]>([]);
  const [filter, setFilter] = useState<string>("pending");
  const [editingId, setEditingId] = useState<number | null>(null);
  const [note, setNote] = useState("");
  const [updating, setUpdating] = useState(false);

  const fetchTasks = async () => {
    try {
      const params = filter ? `?status=${filter}` : "";
      const key = typeof window !== "undefined" ? localStorage.getItem("admin_api_key") : null;
      const headers: Record<string, string> = {};
      if (key) headers["Authorization"] = `Bearer ${key}`;

      const res = await fetch(`${API_BASE}/api/phone-tasks${params}`, { headers });
      if (res.ok) {
        setTasks(await res.json());
      }
    } catch {
      // ignore
    }
  };

  usePolling(fetchTasks, 10000);

  const handleUpdate = async (taskId: number, status: string) => {
    setUpdating(true);
    try {
      const key = typeof window !== "undefined" ? localStorage.getItem("admin_api_key") : null;
      const headers: Record<string, string> = { "Content-Type": "application/json" };
      if (key) headers["Authorization"] = `Bearer ${key}`;

      const res = await fetch(`${API_BASE}/api/phone-tasks/${taskId}`, {
        method: "PUT",
        headers,
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
          <h1 className="text-xl font-bold text-gray-900">電話確認タスク</h1>
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
      <div className="flex gap-2 flex-wrap">
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
          <div className="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mx-auto mb-3">
            <svg className="w-6 h-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z" />
            </svg>
          </div>
          <p className="text-gray-400 text-sm">
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
              className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 transition-shadow hover:shadow-md"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 mb-2">
                    <span
                      className={`text-xs px-2.5 py-1 rounded-full font-medium ${
                        STATUS_STYLES[task.status] || "bg-gray-100 text-gray-600"
                      }`}
                    >
                      {STATUS_LABELS[task.status] || task.status}
                    </span>
                    <span className="text-xs text-gray-400">#{task.id}</span>
                  </div>
                  <h3 className="text-sm font-semibold text-gray-900">
                    {task.property_name || "(物件名なし)"}
                  </h3>
                  {task.property_address && (
                    <p className="text-xs text-gray-500 mt-0.5">
                      {task.property_address}
                    </p>
                  )}
                  <p className="text-sm font-medium text-gray-700 mt-2">
                    {task.company_name}
                  </p>
                </div>

                {/* 電話CTA */}
                {task.company_phone && (
                  <a
                    href={`tel:${task.company_phone}`}
                    className="flex items-center gap-2 px-4 py-2.5 bg-green-600 text-white rounded-xl text-sm font-semibold hover:bg-green-700 transition-all active:scale-95 shrink-0"
                  >
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z" />
                    </svg>
                    {task.company_phone}
                  </a>
                )}
              </div>

              {task.reason && (
                <p className="mt-3 text-xs text-gray-500 bg-gray-50 rounded-lg px-3 py-2">
                  理由: {task.reason}
                </p>
              )}

              {task.note && task.status !== "pending" && (
                <p className="mt-2 text-xs text-gray-600 bg-blue-50 rounded-lg px-3 py-2">
                  メモ: {task.note}
                </p>
              )}

              <div className="mt-3 flex items-center justify-between">
                <span className="text-xs text-gray-400">{task.created_at}</span>

                {task.status === "pending" && (
                  <div className="flex gap-2">
                    {editingId === task.id ? (
                      <div className="flex items-center gap-2 flex-wrap">
                        <input
                          type="text"
                          value={note}
                          onChange={(e) => setNote(e.target.value)}
                          placeholder="メモ（任意）"
                          className="px-3 py-1.5 border border-gray-300 rounded-lg text-xs w-40 focus:outline-none focus:ring-2 focus:ring-blue-500"
                        />
                        <button
                          onClick={() => handleUpdate(task.id, "completed")}
                          disabled={updating}
                          className="px-3 py-1.5 bg-green-600 text-white rounded-lg text-xs font-medium hover:bg-green-700 disabled:opacity-50 transition-colors"
                        >
                          完了
                        </button>
                        <button
                          onClick={() => handleUpdate(task.id, "cancelled")}
                          disabled={updating}
                          className="px-3 py-1.5 bg-gray-400 text-white rounded-lg text-xs font-medium hover:bg-gray-500 disabled:opacity-50 transition-colors"
                        >
                          取消
                        </button>
                        <button
                          onClick={() => {
                            setEditingId(null);
                            setNote("");
                          }}
                          className="px-2 py-1.5 text-gray-400 text-xs hover:text-gray-600 transition-colors"
                        >
                          戻る
                        </button>
                      </div>
                    ) : (
                      <button
                        onClick={() => setEditingId(task.id)}
                        className="px-3 py-1.5 bg-blue-600 text-white rounded-lg text-xs font-medium hover:bg-blue-700 transition-colors active:scale-[0.98]"
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
