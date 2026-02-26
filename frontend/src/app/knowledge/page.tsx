"use client";

import { useState, useEffect } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type KnowledgeItem = {
  id: number;
  company_name: string;
  company_phone: string;
  platform: string;
  use_count: number;
  last_used_at: string;
};

const PLATFORM_LABELS: Record<string, string> = {
  itanji: "イタンジBB",
  ierabu: "いえらぶBB",
  es_square: "いい生活スクエア",
};

const PLATFORM_OPTIONS = [
  { value: "itanji", label: "イタンジBB" },
  { value: "ierabu", label: "いえらぶBB" },
  { value: "es_square", label: "いい生活スクエア" },
];

export default function KnowledgePage() {
  const [items, setItems] = useState<KnowledgeItem[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [formData, setFormData] = useState({
    company_name: "",
    company_phone: "",
    platform: "itanji",
  });
  const [editingId, setEditingId] = useState<number | null>(null);

  const fetchItems = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/knowledge`);
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
      if (editingId) {
        await fetch(`${API_BASE}/api/knowledge/${editingId}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(formData),
        });
      } else {
        await fetch(`${API_BASE}/api/knowledge`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
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
      await fetch(`${API_BASE}/api/knowledge/${id}`, { method: "DELETE" });
      fetchItems();
    } catch {
      // ignore
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-gray-900">ナレッジ管理</h1>
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
          className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
        >
          {showForm ? "キャンセル" : "新規追加"}
        </button>
      </div>

      <p className="text-sm text-gray-500">
        管理会社ごとにどのプラットフォームで空室確認するかのナレッジを管理します。
        使用回数が多いほど自動選択の信頼度が上がります。
      </p>

      {/* 登録フォーム */}
      {showForm && (
        <form
          onSubmit={handleSubmit}
          className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 space-y-4"
        >
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              管理会社名
            </label>
            <input
              type="text"
              value={formData.company_name}
              onChange={(e) =>
                setFormData({ ...formData, company_name: e.target.value })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              電話番号
            </label>
            <input
              type="text"
              value={formData.company_phone}
              onChange={(e) =>
                setFormData({ ...formData, company_phone: e.target.value })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              プラットフォーム
            </label>
            <select
              value={formData.platform}
              onChange={(e) =>
                setFormData({ ...formData, platform: e.target.value })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
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
            className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
          >
            {editingId ? "更新" : "登録"}
          </button>
        </form>
      )}

      {/* 一覧テーブル */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        {items.length === 0 ? (
          <p className="p-6 text-sm text-gray-400">
            ナレッジが登録されていません
          </p>
        ) : (
          <table className="w-full text-sm">
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
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 text-gray-900">
                    {item.company_name}
                  </td>
                  <td className="px-4 py-3 text-gray-600">
                    {item.company_phone || "-"}
                  </td>
                  <td className="px-4 py-3">
                    <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">
                      {PLATFORM_LABELS[item.platform] || item.platform}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-center text-gray-600">
                    {item.use_count}
                  </td>
                  <td className="px-4 py-3 text-right">
                    <button
                      onClick={() => handleEdit(item)}
                      className="text-blue-600 hover:underline mr-3"
                    >
                      編集
                    </button>
                    <button
                      onClick={() => handleDelete(item.id)}
                      className="text-red-600 hover:underline"
                    >
                      削除
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
