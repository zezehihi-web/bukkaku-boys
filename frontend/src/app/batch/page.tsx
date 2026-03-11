"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { API_BASE } from "../lib/api";
import { isTerminal, getResultStyle } from "../lib/constants";
import type { CheckStatus } from "../lib/types";

export default function BatchPage() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const [checks, setChecks] = useState<Map<number, CheckStatus>>(new Map());
  const [loading, setLoading] = useState(true);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const ids = (searchParams.get("ids") || "")
    .split(",")
    .map((s) => parseInt(s, 10))
    .filter((n) => !isNaN(n));

  const fetchAll = useCallback(async () => {
    const results = await Promise.allSettled(
      ids.map((id) =>
        fetch(`${API_BASE}/api/check/${id}`).then((r) =>
          r.ok ? r.json() : null
        )
      )
    );
    setChecks((prev) => {
      const next = new Map(prev);
      results.forEach((r, i) => {
        if (r.status === "fulfilled" && r.value) {
          next.set(ids[i], r.value as CheckStatus);
        }
      });
      return next;
    });
    setLoading(false);
  }, [ids.join(",")]);

  useEffect(() => {
    if (ids.length === 0) return;
    fetchAll();
    intervalRef.current = setInterval(fetchAll, 2000);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [ids.join(",")]);

  // 全件ターミナルならポーリング停止
  useEffect(() => {
    const allDone =
      checks.size === ids.length &&
      ids.every((id) => {
        const c = checks.get(id);
        return c && isTerminal(c.status);
      });
    if (allDone && intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, [checks, ids.join(",")]);

  const completedCount = ids.filter((id) => {
    const c = checks.get(id);
    return c && isTerminal(c.status);
  }).length;

  if (ids.length === 0) {
    return (
      <div className="text-center py-16">
        <p className="text-gray-500">確認対象がありません</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <section className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-xl font-bold text-gray-900">
              一括確認（{ids.length}件）
            </h2>
            <p className="text-sm text-gray-500 mt-1">
              {completedCount}/{ids.length} 件完了
            </p>
          </div>
          <button
            onClick={() => router.push("/")}
            className="text-sm text-blue-600 hover:text-blue-800 font-medium"
          >
            トップへ戻る
          </button>
        </div>

        {/* プログレスバー */}
        <div className="w-full bg-gray-100 rounded-full h-2 mb-6">
          <div
            className="bg-blue-500 h-2 rounded-full transition-all duration-500"
            style={{
              width: `${ids.length > 0 ? (completedCount / ids.length) * 100 : 0}%`,
            }}
          />
        </div>

        {loading ? (
          <div className="space-y-3">
            {ids.map((id) => (
              <div
                key={id}
                className="flex items-center justify-between p-4 rounded-xl border border-gray-100"
              >
                <div className="skeleton h-4 w-48" />
                <div className="skeleton h-6 w-16 rounded-full" />
              </div>
            ))}
          </div>
        ) : (
          <div className="space-y-2">
            {ids.map((id, index) => {
              const check = checks.get(id);
              const terminal = check && isTerminal(check.status);

              return (
                <a
                  key={id}
                  href={`/check/${id}`}
                  onClick={(e) => {
                    e.preventDefault();
                    router.push(`/check/${id}`);
                  }}
                  className="flex items-center justify-between p-4 rounded-xl hover:bg-gray-50 border border-gray-100 transition-all hover:border-gray-200 hover:shadow-sm group"
                >
                  <div className="flex items-center gap-3 min-w-0">
                    <span className="text-xs text-gray-400 font-mono w-5 shrink-0">
                      {index + 1}.
                    </span>
                    <span className="text-sm text-gray-900 truncate font-medium">
                      {check?.property_name || "(確認中)"}
                    </span>
                  </div>
                  <div className="flex items-center gap-3 shrink-0">
                    {check?.vacancy_result ? (
                      <span
                        className={`text-xs px-2.5 py-1 rounded-full font-medium ${getResultStyle(
                          check.vacancy_result
                        )}`}
                      >
                        {check.vacancy_result.length > 10
                          ? check.vacancy_result.slice(0, 10) + "..."
                          : check.vacancy_result}
                      </span>
                    ) : terminal ? (
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
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        d="M9 5l7 7-7 7"
                      />
                    </svg>
                  </div>
                </a>
              );
            })}
          </div>
        )}
      </section>
    </div>
  );
}
