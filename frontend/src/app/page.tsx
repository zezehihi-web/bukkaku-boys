"use client";

import { useState, useEffect } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type CheckItem = {
  id: number;
  property_name: string;
  status: string;
  vacancy_result: string;
  portal_source: string;
  created_at: string;
};

const STATUS_LABELS: Record<string, string> = {
  pending: "待機中",
  parsing: "URL解析中",
  matching: "ATBB照合中",
  awaiting_platform: "PF選択待ち",
  checking: "空室確認中",
  done: "完了",
  not_found: "該当なし",
  error: "エラー",
};

const STATUS_STYLES: Record<string, string> = {
  pending: "text-gray-400",
  parsing: "text-blue-500",
  matching: "text-blue-500",
  awaiting_platform: "text-orange-500",
  checking: "text-blue-500",
  done: "text-green-600",
  not_found: "text-gray-500",
  error: "text-red-500",
};

const RESULT_STYLES: Record<string, string> = {
  "募集中": "bg-green-100 text-green-800",
  "申込あり": "bg-yellow-100 text-yellow-800",
  "募集終了": "bg-red-100 text-red-800",
  "該当なし": "bg-gray-100 text-gray-800",
  "確認不可": "bg-orange-100 text-orange-800",
  "電話確認": "bg-blue-100 text-blue-800",
};

const getResultStyle = (result: string): string => {
  if (RESULT_STYLES[result]) return RESULT_STYLES[result];
  for (const [key, style] of Object.entries(RESULT_STYLES)) {
    if (result.startsWith(key)) return style;
  }
  return "bg-gray-100 text-gray-700";
};

const isProcessing = (status: string) =>
  ["pending", "parsing", "matching", "checking"].includes(status);

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
    if (trimmed.includes("suumo.jp")) return null;
    if (trimmed.includes("homes.co.jp")) return null;
    return "SUUMOまたはHOMESのURLを入力してください";
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
          SUUMOやHOMESの物件URLを貼り付けて、空室状況を自動確認します
        </p>
        <form onSubmit={handleSubmit} className="flex gap-3">
          <input
            type="url"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="https://suumo.jp/chintai/... または https://www.homes.co.jp/..."
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
                  <span className="text-xs text-gray-400 font-mono w-8 shrink-0">
                    #{item.id}
                  </span>
                  <span className="text-sm text-gray-900 truncate">
                    {item.property_name || "(解析中)"}
                  </span>
                  {item.portal_source && (
                    <span className="text-xs px-2 py-0.5 bg-gray-100 text-gray-500 rounded shrink-0">
                      {item.portal_source.toUpperCase()}
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-3 shrink-0">
                  {item.created_at && (
                    <span className="text-xs text-gray-300 hidden sm:inline">
                      {item.created_at.split(" ")[0]}
                    </span>
                  )}
                  {item.vacancy_result ? (
                    <span
                      className={`text-xs px-2.5 py-1 rounded-full font-medium ${
                        getResultStyle(item.vacancy_result)
                      }`}
                    >
                      {item.vacancy_result.length > 10
                        ? item.vacancy_result.slice(0, 10) + "..."
                        : item.vacancy_result}
                    </span>
                  ) : (
                    <span
                      className={`text-xs font-medium ${
                        STATUS_STYLES[item.status] || "text-gray-400"
                      }`}
                    >
                      {isProcessing(item.status) && (
                        <span className="inline-block w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse mr-1.5 align-middle" />
                      )}
                      {STATUS_LABELS[item.status] || item.status}
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
