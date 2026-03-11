"use client";

import { useState, useEffect } from "react";
import { API_BASE } from "../../lib/api";
import { PLATFORM_LABELS, PLATFORM_OPTIONS } from "../../lib/constants";
import type { KnowledgeItem } from "../../lib/types";

export default function AdminKnowledgePage() {
  const [items, setItems] = useState<KnowledgeItem[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [formData, setFormData] = useState({
    company_name: "",
    company_phone: "",
    platform: "itanji",
  });
  const [editingId, setEditingId] = useState<number | null>(null);

  const getAuthHeaders = (): Record<string, string> => {
    const key = typeof window !== "undefined" ? localStorage.getItem("admin_api_key") : null;
    return key ? { Authorization: `Bearer ${key}` } : {};
  };

  const fetchItems = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/knowledge`, {
        headers: getAuthHeaders(),
      });
      if (res.ok) setItems(await res.json());
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    fetchItems();
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        ...getAuthHeaders(),
      };
      if (editingId) {
        await fetch(`${API_BASE}/api/knowledge/${editingId}`, {
          method: "PUT",
          headers,
          body: JSON.stringify(formData),
        });
      } else {
        await fetch(`${API_BASE}/api/knowledge`, {
          method: "POST",
          headers,
          body: JSON.stringify(formData),
        });
      }
      setShowForm(false);
      setEditingId(null);
      setFormData({ company_name: "", company_phone: "", platform: "itanji" });
      fetchItems();
    } catch {
      // ignore
    }
  };

  const handleEdit = (item: KnowledgeItem) => {
    setFormData({
      company_name: item.company_name,
      company_phone: item.company_phone,
      platform: item.platform,
    });
    setEditingId(item.id);
    setShowForm(true);
  };

  const handleDelete = async (id: number) => {
    if (!confirm("このナレッジを削除しますか？")) return;
    try {
      await fetch(`${API_BASE}/api/knowledge/${id}`, {
        method: "DELETE",
        headers: getAuthHeaders(),
      });
      fetchItems();
    } catch {
      // ignore
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">ナレッジ管理</h1>
          <p className="text-sm text-gray-500 mt-1">
            管理会社ごとにどのプラットフォームで空室確認するかのナレッジを管理します。
          </p>
        </div>
        <button
          onClick={() => {
            setShowForm(!showForm);
            setEditingId(null);
            setFormData({
              company_name: "",
              company_phone: "",
              platform: "itanji",
            });
          }}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors active:scale-[0.98]"
        >
          {showForm ? "キャンセル" : "新規追加"}
        </button>
      </div>

      {/* 登録フォーム */}
      {showForm && (
        <form
          onSubmit={handleSubmit}
          className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 space-y-4"
        >
          <div>
            <label htmlFor="company-name" className="block text-sm font-medium text-gray-700 mb-1">
              管理会社名
            </label>
            <input
              id="company-name"
              type="text"
              value={formData.company_name}
              onChange={(e) =>
                setFormData({ ...formData, company_name: e.target.value })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
          </div>
          <div>
            <label htmlFor="company-phone" className="block text-sm font-medium text-gray-700 mb-1">
              電話番号
            </label>
            <input
              id="company-phone"
              type="text"
              value={formData.company_phone}
              onChange={(e) =>
                setFormData({ ...formData, company_phone: e.target.value })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
          <div>
            <label htmlFor="platform-select" className="block text-sm font-medium text-gray-700 mb-1">
              プラットフォーム
            </label>
            <select
              id="platform-select"
              value={formData.platform}
              onChange={(e) =>
                setFormData({ ...formData, platform: e.target.value })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              {PLATFORM_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>
          <button
            type="submit"
            className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors active:scale-[0.98]"
          >
            {editingId ? "更新" : "登録"}
          </button>
        </form>
      )}

      {/* 一覧テーブル */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        {items.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <div className="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mb-3">
              <svg className="w-6 h-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
              </svg>
            </div>
            <p className="text-sm text-gray-400">ナレッジが登録されていません</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm min-w-[600px]">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-3 font-medium text-gray-600">
                    管理会社名
                  </th>
                  <th className="text-left px-4 py-3 font-medium text-gray-600">
                    電話番号
                  </th>
                  <th className="text-left px-4 py-3 font-medium text-gray-600">
                    プラットフォーム
                  </th>
                  <th className="text-center px-4 py-3 font-medium text-gray-600">
                    使用回数
                  </th>
                  <th className="text-right px-4 py-3 font-medium text-gray-600">
                    操作
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {items.map((item) => (
                  <tr key={item.id} className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-3 text-gray-900 font-medium">
                      {item.company_name}
                    </td>
                    <td className="px-4 py-3 text-gray-600 font-mono">
                      {item.company_phone || "-"}
                    </td>
                    <td className="px-4 py-3">
                      <span className="px-2.5 py-1 bg-blue-50 text-blue-700 rounded-full text-xs font-medium">
                        {PLATFORM_LABELS[item.platform] || item.platform}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-center text-gray-600 tabular-nums">
                      {item.use_count}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <button
                        onClick={() => handleEdit(item)}
                        className="text-blue-600 hover:underline mr-3 text-xs font-medium"
                      >
                        編集
                      </button>
                      <button
                        onClick={() => handleDelete(item.id)}
                        className="text-red-600 hover:underline text-xs font-medium"
                      >
                        削除
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
