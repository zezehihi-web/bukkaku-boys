"use client";

import { useState, useEffect, use } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type CheckStatus = {
  id: number;
  submitted_url: string;
  portal_source: string;
  property_name: string;
  property_address: string;
  property_rent: string;
  property_area: string;
  property_layout: string;
  atbb_matched: boolean;
  atbb_company: string;
  platform: string;
  platform_auto: boolean;
  status: string;
  vacancy_result: string;
  error_message: string;
  created_at: string;
  completed_at: string | null;
};

const STEPS = [
  { key: "parsing", label: "URL解析" },
  { key: "matching", label: "ATBB照合" },
  { key: "checking", label: "空室確認" },
  { key: "done", label: "完了" },
];

const STEP_ORDER: Record<string, number> = {
  pending: -1,
  parsing: 0,
  matching: 1,
  awaiting_platform: 2,
  checking: 2,
  done: 3,
  not_found: 3,
  error: -2,
};

const PLATFORM_LABELS: Record<string, string> = {
  itanji: "イタンジBB",
  ierabu: "いえらぶBB",
  es_square: "いい生活スクエア",
};

const RESULT_STYLES: Record<string, { bg: string; text: string; icon: string }> = {
  "募集中": { bg: "bg-green-50", text: "text-green-700", icon: "bg-green-500" },
  "申込あり": { bg: "bg-yellow-50", text: "text-yellow-700", icon: "bg-yellow-500" },
  "募集終了": { bg: "bg-red-50", text: "text-red-700", icon: "bg-red-500" },
  "該当なし": { bg: "bg-gray-50", text: "text-gray-700", icon: "bg-gray-500" },
};

export default function CheckDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const [data, setData] = useState<CheckStatus | null>(null);
  const [selectingPlatform, setSelectingPlatform] = useState(false);
  const [remember, setRemember] = useState(true);

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/check/${id}`);
        if (res.ok) {
          const result = await res.json();
          setData(result);
          // 完了状態ならポーリング停止
          if (
            ["done", "not_found", "error"].includes(result.status) &&
            result.status !== "awaiting_platform"
          ) {
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

  const handlePlatformSelect = async (platform: string) => {
    setSelectingPlatform(true);
    try {
      await fetch(`${API_BASE}/api/check/${id}/platform`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ platform, remember }),
      });
    } catch {
      // ignore
    }
    setSelectingPlatform(false);
  };

  if (!data) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  const currentStep = STEP_ORDER[data.status] ?? -1;
  const resultStyle = data.vacancy_result
    ? RESULT_STYLES[data.vacancy_result] || RESULT_STYLES["該当なし"]
    : null;

  return (
    <div className="space-y-6">
      {/* ヘッダー */}
      <div>
        <a href="/" className="text-sm text-blue-600 hover:underline">
          &larr; 一覧に戻る
        </a>
        <h1 className="mt-2 text-xl font-bold text-gray-900">
          {data.property_name || "(解析中)"}
        </h1>
        <p className="text-sm text-gray-500 mt-1 break-all">
          {data.submitted_url}
        </p>
      </div>

      {/* ステップ表示 */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-sm font-semibold text-gray-700 mb-4">処理状況</h2>
        <div className="flex items-center gap-2">
          {STEPS.map((step, i) => {
            const isActive = currentStep === i;
            const isDone = currentStep > i;
            const isError = data.status === "error";
            return (
              <div key={step.key} className="flex items-center gap-2 flex-1">
                <div
                  className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold shrink-0 ${
                    isDone
                      ? "bg-blue-600 text-white"
                      : isActive
                        ? isError
                          ? "bg-red-500 text-white"
                          : "bg-blue-100 text-blue-700 ring-2 ring-blue-400"
                        : "bg-gray-100 text-gray-400"
                  }`}
                >
                  {isDone ? "\u2713" : i + 1}
                </div>
                <span
                  className={`text-xs ${
                    isActive
                      ? "text-gray-900 font-medium"
                      : isDone
                        ? "text-gray-600"
                        : "text-gray-400"
                  }`}
                >
                  {step.label}
                </span>
                {i < STEPS.length - 1 && (
                  <div
                    className={`flex-1 h-0.5 ${isDone ? "bg-blue-400" : "bg-gray-200"}`}
                  />
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* 結果表示 */}
      {resultStyle && data.vacancy_result && (
        <div
          className={`rounded-xl p-6 border ${resultStyle.bg} border-gray-200`}
        >
          <div className="flex items-center gap-3">
            <div className={`w-4 h-4 rounded-full ${resultStyle.icon}`} />
            <span className={`text-2xl font-bold ${resultStyle.text}`}>
              {data.vacancy_result}
            </span>
          </div>
          {data.platform && (
            <p className="mt-2 text-sm text-gray-600">
              確認先: {PLATFORM_LABELS[data.platform] || data.platform}
              {data.platform_auto && (
                <span className="ml-2 text-xs text-gray-400">
                  (自動選択)
                </span>
              )}
            </p>
          )}
        </div>
      )}

      {/* エラー表示 */}
      {data.status === "error" && data.error_message && (
        <div className="rounded-xl p-4 bg-red-50 border border-red-200">
          <p className="text-sm text-red-700">{data.error_message}</p>
        </div>
      )}

      {/* プラットフォーム選択 */}
      {data.status === "awaiting_platform" && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
          <h2 className="text-sm font-semibold text-gray-700 mb-2">
            プラットフォームを選択してください
          </h2>
          {data.atbb_company && (
            <p className="text-sm text-gray-500 mb-4">
              管理会社: {data.atbb_company}
            </p>
          )}
          <div className="grid grid-cols-2 gap-3">
            {(
              [
                ["itanji", "イタンジBB"],
                ["ierabu", "いえらぶBB"],
                ["es_square", "いい生活スクエア"],
              ] as const
            ).map(([key, label]) => (
              <button
                key={key}
                onClick={() => handlePlatformSelect(key)}
                disabled={selectingPlatform}
                className="px-4 py-3 border border-gray-300 rounded-lg text-sm font-medium hover:bg-blue-50 hover:border-blue-300 disabled:opacity-50 transition-colors"
              >
                {label}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-2 mt-4 text-sm text-gray-600">
            <input
              type="checkbox"
              checked={remember}
              onChange={(e) => setRemember(e.target.checked)}
              className="rounded"
            />
            この選択を記憶する
          </label>
        </div>
      )}

      {/* 物件詳細 */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-sm font-semibold text-gray-700 mb-4">
          物件情報
        </h2>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-3 text-sm">
          {[
            ["物件名", data.property_name],
            ["住所", data.property_address],
            ["賃料", data.property_rent],
            ["面積", data.property_area],
            ["間取り", data.property_layout],
            ["ATBB照合", data.atbb_matched ? "一致" : data.status === "not_found" ? "該当なし" : "照合中"],
            ["管理会社", data.atbb_company],
            ["ポータル", data.portal_source?.toUpperCase()],
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
    </div>
  );
}
