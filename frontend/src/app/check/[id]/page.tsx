"use client";

import { useState, useEffect, use } from "react";
import Link from "next/link";
import { API_BASE } from "../../lib/api";
import { getResultDetailStyle, isTerminal, PLATFORM_LABELS } from "../../lib/constants";
import type { CheckStatus } from "../../lib/types";

// ステップインジケーター
const STEPS = [
  { key: "parsing", label: "URL解析" },
  { key: "matching", label: "物件照合" },
  { key: "checking", label: "空室確認" },
] as const;

function getStepIndex(status: string): number {
  const map: Record<string, number> = {
    pending: 0, parsing: 0, matching: 1,
    awaiting_platform: 1, checking: 2,
    done: 3, not_found: 3, error: 3,
  };
  return map[status] ?? 0;
}

// 結果アイコンSVG
function ResultIcon({ result }: { result: string }) {
  if (result.startsWith("募集中")) {
    return (
      <svg className="w-8 h-8 text-green-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
    );
  }
  if (result.startsWith("申込あり")) {
    return (
      <svg className="w-8 h-8 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
      </svg>
    );
  }
  if (result.startsWith("募集終了")) {
    return (
      <svg className="w-8 h-8 text-red-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
    );
  }
  if (result.startsWith("電話確認")) {
    return (
      <svg className="w-8 h-8 text-blue-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z" />
      </svg>
    );
  }
  return (
    <svg className="w-8 h-8 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  );
}

export default function CheckDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const [data, setData] = useState<CheckStatus | null>(null);

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/check/${id}`);
        if (res.ok) {
          const result = await res.json();
          setData(result);
          if (isTerminal(result.status)) {
            return false;
          }
        }
      } catch {
        // ignore
      }
      return true;
    };

    fetchStatus();
    const interval = setInterval(async () => {
      const shouldContinue = await fetchStatus();
      if (!shouldContinue) clearInterval(interval);
    }, 2000);
    return () => clearInterval(interval);
  }, [id]);

  // スケルトンローディング
  if (!data) {
    return (
      <div className="space-y-6">
        <div className="skeleton h-4 w-24" />
        <div className="skeleton h-8 w-64" />
        <div className="bg-white rounded-xl border border-gray-200 p-8">
          <div className="flex justify-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
          </div>
        </div>
      </div>
    );
  }

  const terminal = isTerminal(data.status);
  const resultStyle = data.vacancy_result
    ? getResultDetailStyle(data.vacancy_result)
    : null;
  const stepIndex = getStepIndex(data.status);

  return (
    <div className="space-y-6">
      {/* ヘッダー */}
      <div>
        <Link href="/" className="text-sm text-blue-600 hover:underline inline-flex items-center gap-1">
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M15 19l-7-7 7-7" />
          </svg>
          一覧に戻る
        </Link>
        <h1 className="mt-2 text-xl font-bold text-gray-900">
          {data.property_name || "(確認中)"}
        </h1>
      </div>

      {/* 結果表示 or ステップインジケーター */}
      {resultStyle && data.vacancy_result ? (
        <div className={`rounded-xl p-6 border ${resultStyle.bg} border-gray-200 animate-scale-in`}>
          <div className="flex items-center gap-4">
            <ResultIcon result={data.vacancy_result} />
            <div>
              <span className={`text-2xl font-bold ${resultStyle.text}`}>
                {data.vacancy_result}
              </span>
              {data.platform && (
                <p className="text-xs text-gray-500 mt-1">
                  確認元: {PLATFORM_LABELS[data.platform] || data.platform}
                  {data.completed_at && ` (${data.completed_at})`}
                </p>
              )}
            </div>
          </div>
        </div>
      ) : data.status === "error" || data.status === "not_found" ? (
        <div className="rounded-xl p-6 bg-gray-50 border border-gray-200 animate-scale-in">
          <div className="flex items-center gap-4">
            <div className="w-10 h-10 rounded-full bg-gray-200 flex items-center justify-center shrink-0">
              <svg className="w-5 h-5 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
              </svg>
            </div>
            <div>
              <p className="text-lg font-semibold text-gray-600">確認不可</p>
              <p className="text-sm text-gray-500 mt-1">
                {data.error_message || "この物件の空室状況を自動確認できませんでした。"}
              </p>
            </div>
          </div>
          <Link
            href="/"
            className="mt-4 inline-flex px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            新しいURLで確認する
          </Link>
        </div>
      ) : (
        /* ステップインジケーター */
        <div className="rounded-xl p-6 bg-white border border-gray-200">
          <div className="flex items-center justify-between mb-6">
            {STEPS.map((step, i) => (
              <div key={step.key} className="flex items-center flex-1">
                <div className="flex flex-col items-center">
                  <div
                    className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold transition-colors duration-500 ${
                      i < stepIndex
                        ? "bg-blue-600 text-white"
                        : i === stepIndex
                          ? "bg-blue-100 text-blue-600 ring-4 ring-blue-50"
                          : "bg-gray-100 text-gray-400"
                    }`}
                  >
                    {i < stepIndex ? (
                      <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                      </svg>
                    ) : (
                      i + 1
                    )}
                  </div>
                  <span
                    className={`mt-2 text-xs font-medium ${
                      i <= stepIndex ? "text-blue-600" : "text-gray-400"
                    }`}
                  >
                    {step.label}
                  </span>
                </div>
                {i < STEPS.length - 1 && (
                  <div
                    className={`flex-1 h-0.5 mx-3 transition-colors duration-500 ${
                      i < stepIndex ? "bg-blue-600" : "bg-gray-200"
                    }`}
                  />
                )}
              </div>
            ))}
          </div>
          <p className="text-center text-sm text-gray-500">
            空室状況を確認しています。通常30秒ほどで完了します。
          </p>
        </div>
      )}

      {/* 物件情報 */}
      {terminal && data.property_name && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 animate-fade-in">
          <h2 className="text-sm font-semibold text-gray-700 mb-4">
            物件情報
          </h2>
          <dl className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-3 text-sm">
            {[
              ["物件名", data.property_name],
              ["住所", data.property_address],
              ["賃料", data.property_rent],
              ["面積", data.property_area],
              ["間取り", data.property_layout],
              ["築年月", data.property_build_year],
            ]
              .filter(([, v]) => v)
              .map(([label, value]) => (
                <div key={label}>
                  <dt className="text-gray-500">{label}</dt>
                  <dd className="text-gray-900 font-medium">{value}</dd>
                </div>
              ))}
          </dl>
        </div>
      )}
    </div>
  );
}
