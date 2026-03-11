"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { API_BASE } from "./lib/api";
import { isTerminal } from "./lib/constants";
import type { CheckItem } from "./lib/types";
import { usePolling } from "./lib/usePolling";

export default function HomePage() {
  const router = useRouter();
  const [urls, setUrls] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitSuccess, setSubmitSuccess] = useState(false);
  const [checks, setChecks] = useState<CheckItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const fetchChecks = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/checks?limit=20`);
      if (res.ok) {
        setChecks(await res.json());
      }
    } catch {
      // API未起動時は静かに失敗
    } finally {
      setLoading(false);
    }
  };

  usePolling(fetchChecks, 5000);

  /** textarea内容をURL配列に変換（空行除去） */
  const parseUrls = (input: string): string[] => {
    return input
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);
  };

  /** URL配列を検証。エラーがあれば文字列を返す */
  const validateUrls = (urlList: string[]): string | null => {
    if (urlList.length === 0) return "URLを入力してください";
    if (urlList.length > 5) return "一度に確認できるのは最大5件です";
    const errors: string[] = [];
    urlList.forEach((u, i) => {
      try {
        new URL(u);
      } catch {
        errors.push(`${i + 1}行目: 有効なURLではありません`);
      }
    });
    if (errors.length > 0) return errors.join("\n");
    return null;
  };

  const urlCount = parseUrls(urls).length;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    const urlList = parseUrls(urls);
    const validationError = validateUrls(urlList);
    if (validationError) {
      setError(validationError);
      return;
    }

    setSubmitting(true);
    try {
      if (urlList.length === 1) {
        // 1件の場合: 従来通り個別API
        const res = await fetch(`${API_BASE}/api/check`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: urlList[0] }),
        });
        if (!res.ok) {
          const data = await res.json();
          throw new Error(data.detail || "送信に失敗しました");
        }
        const data = await res.json();
        setUrls("");
        setSubmitSuccess(true);
        setTimeout(() => {
          router.push(`/check/${data.id}`);
        }, 300);
      } else {
        // 複数件: バッチAPI
        const res = await fetch(`${API_BASE}/api/checks/batch`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ urls: urlList }),
        });
        if (!res.ok) {
          const data = await res.json();
          throw new Error(data.detail || "送信に失敗しました");
        }
        const data = await res.json();
        setUrls("");
        setSubmitSuccess(true);
        setTimeout(() => {
          router.push(`/batch?ids=${data.ids.join(",")}`);
        }, 300);
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "エラーが発生しました");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="space-y-8">
      {/* URL入力セクション */}
      <section className="bg-white rounded-2xl shadow-sm border border-gray-200 p-8">
        <h2 className="text-xl font-bold text-gray-900 mb-2">空室確認</h2>
        <p className="text-sm text-gray-500 mb-6">
          SUUMOまたはHOMESの物件ページURLを貼り付けてください（最大5件）
        </p>
        <form onSubmit={handleSubmit} className="space-y-3">
          <div className="relative">
            <textarea
              value={urls}
              onChange={(e) => setUrls(e.target.value)}
              placeholder={"https://suumo.jp/...\nhttps://www.homes.co.jp/...\n（1行に1件、最大5件まで）"}
              rows={4}
              className="w-full px-4 py-4 border border-gray-300 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent placeholder:text-gray-400 transition-shadow resize-none"
            />
            {urlCount > 0 && (
              <span
                className={`absolute bottom-3 right-3 text-xs font-medium px-2 py-0.5 rounded-full ${
                  urlCount > 5
                    ? "bg-red-100 text-red-600"
                    : "bg-blue-100 text-blue-600"
                }`}
              >
                {urlCount}/5 件
              </span>
            )}
          </div>
          <button
            type="submit"
            disabled={submitting || submitSuccess}
            className="w-full py-3.5 bg-blue-600 text-white rounded-xl text-sm font-semibold hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all active:scale-[0.98]"
          >
            {submitSuccess ? (
              <span className="flex items-center justify-center gap-2">
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                </svg>
                確認開始!
              </span>
            ) : submitting ? (
              <span className="flex items-center justify-center gap-2">
                <span className="inline-block w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                確認を開始しています...
              </span>
            ) : urlCount > 1 ? (
              `${urlCount}件の空室を一括確認する`
            ) : (
              "空室を確認する"
            )}
          </button>
        </form>
        {error && (
          <div className="mt-3 bg-red-50 border border-red-200 rounded-lg px-4 py-3">
            <p className="text-sm text-red-700 whitespace-pre-line">{error}</p>
          </div>
        )}
        <div className="flex items-center gap-4 mt-4 pt-4 border-t border-gray-100">
          <span className="text-xs text-gray-400">対応サイト:</span>
          <span className="text-xs px-2 py-0.5 bg-green-50 text-green-700 rounded">SUUMO</span>
          <span className="text-xs px-2 py-0.5 bg-orange-50 text-orange-700 rounded">HOMES</span>
        </div>
      </section>

      {/* 最近の確認結果 */}
      <section className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          最近の確認結果
        </h2>
        {loading ? (
          <div className="space-y-3">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="flex items-center justify-between p-4 rounded-xl border border-gray-100">
                <div className="skeleton h-4 w-48" />
                <div className="skeleton h-6 w-16 rounded-full" />
              </div>
            ))}
          </div>
        ) : checks.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <div className="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mb-3">
              <svg className="w-6 h-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
              </svg>
            </div>
            <p className="text-sm text-gray-400">まだ確認結果がありません</p>
            <p className="text-xs text-gray-300 mt-1">上のフォームからURLを入力して確認を開始してください</p>
          </div>
        ) : (
          <div className="space-y-2" aria-live="polite">
            {checks.map((item, index) => (
              <a
                key={item.id}
                href={`/check/${item.id}`}
                onClick={(e) => {
                  e.preventDefault();
                  router.push(`/check/${item.id}`);
                }}
                className="flex items-center justify-between p-4 rounded-xl hover:bg-gray-50 border border-gray-100 transition-all hover:border-gray-200 hover:shadow-sm group animate-fade-in"
                style={{ animationDelay: `${index * 30}ms` }}
              >
                <div className="flex items-center gap-3 min-w-0">
                  <span className="text-sm text-gray-900 truncate font-medium">
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
                      確認不可
                    </span>
                  ) : (
                    <span className="text-xs font-medium text-blue-500 flex items-center">
                      <span className="relative inline-flex h-2 w-2 mr-1.5">
                        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75" />
                        <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500" />
                      </span>
                      確認中...
                    </span>
                  )}
                  <svg
                    className="w-4 h-4 text-gray-300 group-hover:text-gray-400 transition-transform group-hover:translate-x-0.5"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={2}
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                  </svg>
                </div>
              </a>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

// Import from constants
import { getResultStyle } from "./lib/constants";
