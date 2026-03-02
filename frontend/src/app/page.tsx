"use client";

import { useState, useEffect } from "react";
import { API_BASE } from "./lib/api";
import { USER_STATUS_LABELS, getResultStyle, isProcessing, isTerminal } from "./lib/constants";
import type { CheckItem } from "./lib/types";

export default function HomePage() {
  const [url, setUrl] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [checks, setChecks] = useState<CheckItem[]>([]);
  const [error, setError] = useState("");

  useEffect(() => {
    const fetchChecks = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/checks?limit=20`);
        if (res.ok) {
          setChecks(await res.json());
        }
      } catch {
        // API未起動時は静かに失敗
      }
    };
    fetchChecks();
    const interval = setInterval(fetchChecks, 5000);
    return () => clearInterval(interval);
  }, []);

  const validateUrl = (input: string): string | null => {
    const trimmed = input.trim();
    if (!trimmed) return "URLを入力してください";
    try {
      new URL(trimmed);
      return null;
    } catch {
      return "有効なURLを入力してください";
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    const validationError = validateUrl(url);
    if (validationError) {
      setError(validationError);
      return;
    }

    setSubmitting(true);
    try {
      const res = await fetch(`${API_BASE}/api/check`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: url.trim() }),
      });
      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.detail || "送信に失敗しました");
      }
      const data = await res.json();
      setUrl("");
      window.location.href = `/check/${data.id}`;
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "エラーが発生しました");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="space-y-8">
      <section className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          空室確認
        </h2>
        <p className="text-sm text-gray-500 mb-4">
          物件ポータルサイトのURLを貼り付けて、空室状況を自動確認します
        </p>
        <form onSubmit={handleSubmit} className="flex gap-3">
          <input
            type="url"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="物件ページのURLを貼り付け"
            className="flex-1 px-4 py-3 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            required
          />
          <button
            type="submit"
            disabled={submitting}
            className="px-6 py-3 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
          >
            {submitting ? "送信中..." : "確認する"}
          </button>
        </form>
        {error && <p className="mt-2 text-sm text-red-600">{error}</p>}
      </section>

      <section className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          最近の確認結果
        </h2>
        {checks.length === 0 ? (
          <p className="text-sm text-gray-400">まだ確認結果がありません</p>
        ) : (
          <div className="space-y-2">
            {checks.map((item) => (
              <a
                key={item.id}
                href={`/check/${item.id}`}
                className="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50 border border-gray-100 transition-colors"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <span className="text-sm text-gray-900 truncate">
                    {item.property_name || "(確認中)"}
                  </span>
                </div>
                <div className="flex items-center gap-3 shrink-0">
                  {item.vacancy_result ? (
                    <span
                      className={`text-xs px-2.5 py-1 rounded-full font-medium ${getResultStyle(
                        item.vacancy_result,
                      )}`}
                    >
                      {item.vacancy_result.length > 10
                        ? item.vacancy_result.slice(0, 10) + "..."
                        : item.vacancy_result}
                    </span>
                  ) : isTerminal(item.status) ? (
                    <span className="text-xs font-medium text-gray-500">
                      {USER_STATUS_LABELS[item.status] || item.status}
                    </span>
                  ) : (
                    <span className="text-xs font-medium text-blue-500 flex items-center">
                      <span className="inline-block w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse mr-1.5" />
                      確認中...
                    </span>
                  )}
                </div>
              </a>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
