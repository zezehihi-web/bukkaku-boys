"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { API_BASE } from "./lib/api";
import { isTerminal } from "./lib/constants";
import type { CheckItem } from "./lib/types";
import { usePolling } from "./lib/usePolling";

const MAX_URLS = 5;

export default function HomePage() {
  const router = useRouter();
  const [urlFields, setUrlFields] = useState<string[]>([""]);
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

  const updateUrl = (index: number, value: string) => {
    setUrlFields((prev) => {
      const next = [...prev];
      next[index] = value;
      return next;
    });
  };

  const addField = () => {
    if (urlFields.length < MAX_URLS) {
      setUrlFields((prev) => [...prev, ""]);
    }
  };

  const removeField = (index: number) => {
    if (urlFields.length <= 1) return;
    setUrlFields((prev) => prev.filter((_, i) => i !== index));
  };

  /** 入力済みURL（空欄除去） */
  const filledUrls = urlFields.map((u) => u.trim()).filter((u) => u.length > 0);

  /** URL配列を検証 */
  const validateUrls = (urlList: string[]): string | null => {
    if (urlList.length === 0) return "URLを入力してください";
    const errors: string[] = [];
    urlList.forEach((u, i) => {
      try {
        new URL(u);
      } catch {
        errors.push(`${i + 1}件目: 有効なURLではありません`);
      }
    });
    if (errors.length > 0) return errors.join("\n");
    return null;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    const validationError = validateUrls(filledUrls);
    if (validationError) {
      setError(validationError);
      return;
    }

    setSubmitting(true);
    try {
      if (filledUrls.length === 1) {
        const res = await fetch(`${API_BASE}/api/check`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: filledUrls[0] }),
        });
        if (!res.ok) {
          const data = await res.json();
          throw new Error(data.detail || "送信に失敗しました");
        }
        const data = await res.json();
        setUrlFields([""]);
        setSubmitSuccess(true);
        setTimeout(() => {
          router.push(`/check/${data.id}`);
        }, 300);
      } else {
        const res = await fetch(`${API_BASE}/api/checks/batch`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ urls: filledUrls }),
        });
        if (!res.ok) {
          const data = await res.json();
          throw new Error(data.detail || "送信に失敗しました");
        }
        const data = await res.json();
        setUrlFields([""]);
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
          SUUMOまたはHOMESの物件ページURLを貼り付けてください（最大{MAX_URLS}件）
        </p>
        <form onSubmit={handleSubmit} className="space-y-3">
          <div className="space-y-2">
            {urlFields.map((url, index) => (
              <div key={index} className="flex items-center gap-2">
                <span className="text-xs text-gray-400 font-mono w-5 shrink-0 text-right">
                  {index + 1}.
                </span>
                <input
                  type="url"
                  value={url}
                  onChange={(e) => updateUrl(index, e.target.value)}
                  placeholder="https://suumo.jp/... または https://www.homes.co.jp/..."
                  className="flex-1 px-4 py-3 border border-gray-300 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent placeholder:text-gray-400 transition-shadow"
                />
                {urlFields.length > 1 && (
                  <button
                    type="button"
                    onClick={() => removeField(index)}
                    className="p-2 text-gray-300 hover:text-red-500 transition-colors shrink-0"
                    title="削除"
                  >
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                )}
              </div>
            ))}
          </div>

          {urlFields.length < MAX_URLS && (
            <button
              type="button"
              onClick={addField}
              className="flex items-center gap-1.5 text-sm text-blue-600 hover:text-blue-800 font-medium px-1 py-1 transition-colors"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m8-8H4" />
              </svg>
              URLを追加（{urlFields.length}/{MAX_URLS}）
            </button>
          )}

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
            ) : filledUrls.length > 1 ? (
              `${filledUrls.length}件の空室を一括確認する`
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
