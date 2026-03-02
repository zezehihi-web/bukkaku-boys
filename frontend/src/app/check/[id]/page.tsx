"use client";

import { useState, useEffect, use } from "react";
import { API_BASE } from "../../lib/api";
import { USER_STATUS_LABELS, getResultDetailStyle, isTerminal } from "../../lib/constants";
import type { CheckStatus } from "../../lib/types";

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

  if (!data) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  const terminal = isTerminal(data.status);
  const resultStyle = data.vacancy_result
    ? getResultDetailStyle(data.vacancy_result)
    : null;

  return (
    <div className="space-y-6">
      {/* ヘッダー */}
      <div>
        <a href="/" className="text-sm text-blue-600 hover:underline">
          &larr; 一覧に戻る
        </a>
        <h1 className="mt-2 text-xl font-bold text-gray-900">
          {data.property_name || "(確認中)"}
        </h1>
      </div>

      {/* 結果表示 or 確認中 */}
      {resultStyle && data.vacancy_result ? (
        <div
          className={`rounded-xl p-6 border ${resultStyle.bg} border-gray-200`}
        >
          <div className="flex items-center gap-3">
            <div className={`w-4 h-4 rounded-full ${resultStyle.icon}`} />
            <span className={`text-2xl font-bold ${resultStyle.text}`}>
              {data.vacancy_result}
            </span>
          </div>
        </div>
      ) : data.status === "error" || data.status === "not_found" ? (
        <div className="rounded-xl p-6 bg-gray-50 border border-gray-200">
          <p className="text-lg font-semibold text-gray-600">確認不可</p>
          <p className="mt-2 text-sm text-gray-500">
            この物件の空室状況を自動確認できませんでした。
          </p>
          <button
            onClick={() => {
              window.location.href = "/";
            }}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
          >
            新しいURLで確認する
          </button>
        </div>
      ) : (
        <div className="rounded-xl p-8 bg-white border border-gray-200 text-center">
          <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-blue-100 mb-4">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600" />
          </div>
          <p className="text-lg font-semibold text-gray-900">確認中...</p>
          <p className="mt-1 text-sm text-gray-500">
            空室状況を確認しています。しばらくお待ちください。
          </p>
        </div>
      )}

      {/* 物件情報 (結果の出た最低限のみ) */}
      {terminal && data.property_name && (
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
