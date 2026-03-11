"use client";

import { useState, useCallback } from "react";
import Link from "next/link";
import { apiFetchSafe, API_BASE } from "../lib/api";
import { usePolling } from "../lib/usePolling";
import { useNotifications } from "../lib/useNotifications";
import {
  ADMIN_STATUS_LABELS,
  ADMIN_STATUS_STYLES,
  PLATFORM_LABELS,
  getResultStyle,
} from "../lib/constants";
import type { CheckItem, CheckStatus, PlatformStatus } from "../lib/types";

type AwaitingItem = CheckStatus;

type DailyCount = { date: string; count: number };
type ResultCount = { label: string; count: number; color: string };

const RESULT_COLORS: Record<string, string> = {
  "募集中": "#22c55e",
  "申込あり": "#eab308",
  "募集終了": "#ef4444",
  "該当なし": "#6b7280",
  "確認不可": "#f97316",
  "電話確認": "#3b82f6",
};

export default function AdminDashboard() {
  // Stats
  const [todayTotal, setTodayTotal] = useState(0);
  const [processingCount, setProcessingCount] = useState(0);
  const [awaitingCount, setAwaitingCount] = useState(0);
  const [phoneTasksPending, setPhoneTasksPending] = useState(0);

  // PF選択キュー
  const [awaitingItems, setAwaitingItems] = useState<AwaitingItem[]>([]);
  const [selectingId, setSelectingId] = useState<number | null>(null);
  const [remember, setRemember] = useState(true);
  const [platformStatus, setPlatformStatus] = useState<PlatformStatus>({});
  const [platformLoaded, setPlatformLoaded] = useState(false);

  // 最近の確認結果
  const [recentChecks, setRecentChecks] = useState<CheckItem[]>([]);

  // チャートデータ
  const [dailyCounts, setDailyCounts] = useState<DailyCount[]>([]);
  const [resultDistribution, setResultDistribution] = useState<ResultCount[]>([]);

  // エラー表示
  const [apiError, setApiError] = useState<string | null>(null);

  // 通知
  const { permission, requestPermission, notifyOnIncrease } =
    useNotifications();

  const fetchData = useCallback(async () => {
    try {
      // プラットフォーム設定状態を初回のみ取得
      if (!platformLoaded) {
        const pfStatus = await apiFetchSafe<PlatformStatus>("/api/platforms/status");
        if (pfStatus) {
          setPlatformStatus(pfStatus);
          setPlatformLoaded(true);
        }
      }

      // 最近のチェック一覧
      const checks = await apiFetchSafe<CheckItem[]>("/api/checks?limit=30");
      if (checks) {
        setRecentChecks(checks);
        setApiError(null);

        // 統計計算
        const today = new Date().toISOString().slice(0, 10);
        const todayChecks = checks.filter(
          (c) => c.created_at && c.created_at.startsWith(today),
        );
        setTodayTotal(todayChecks.length);

        const processing = checks.filter((c) =>
          ["pending", "parsing", "matching", "checking"].includes(c.status),
        ).length;
        setProcessingCount(processing);

        const awaiting = checks.filter(
          (c) => c.status === "awaiting_platform",
        ).length;
        setAwaitingCount(awaiting);

        // PF選択待ちアイテムの詳細取得
        const awaitingIds = checks
          .filter((c) => c.status === "awaiting_platform")
          .map((c) => c.id);
        if (awaitingIds.length > 0) {
          const details = await Promise.all(
            awaitingIds.map((id) =>
              apiFetchSafe<AwaitingItem>(`/api/check/${id}`),
            ),
          );
          setAwaitingItems(
            details.filter((d): d is AwaitingItem => d !== null),
          );
        } else {
          setAwaitingItems([]);
        }

        // 通知: PF選択待ち増加
        notifyOnIncrease(
          "awaiting",
          awaiting,
          "プラットフォーム選択が必要です",
          `${awaiting}件のPF選択待ちがあります`,
        );

        // チャートデータ: 過去7日間の件数
        const last7Days: DailyCount[] = [];
        for (let i = 6; i >= 0; i--) {
          const d = new Date();
          d.setDate(d.getDate() - i);
          const dateStr = d.toISOString().slice(0, 10);
          const count = checks.filter(
            (c) => c.created_at && c.created_at.startsWith(dateStr),
          ).length;
          last7Days.push({ date: dateStr, count });
        }
        setDailyCounts(last7Days);

        // チャートデータ: 結果分布
        const distribution: Record<string, number> = {};
        checks.forEach((c) => {
          if (c.vacancy_result) {
            // 先頭のキーワードでグルーピング
            const key = Object.keys(RESULT_COLORS).find((k) =>
              c.vacancy_result.startsWith(k),
            ) || "その他";
            distribution[key] = (distribution[key] || 0) + 1;
          }
        });
        setResultDistribution(
          Object.entries(distribution).map(([label, count]) => ({
            label,
            count,
            color: RESULT_COLORS[label] || "#9ca3af",
          })),
        );
      } else {
        setApiError("APIサーバーに接続できません");
      }

      // 電話タスク数
      const phoneData = await apiFetchSafe<{ count: number }>(
        "/api/phone-tasks/count",
      );
      if (phoneData) {
        setPhoneTasksPending(phoneData.count);
        notifyOnIncrease(
          "phone",
          phoneData.count,
          "新しい電話タスクがあります",
          `${phoneData.count}件の未対応タスクがあります`,
        );
      }
    } catch {
      setApiError("データの取得に失敗しました");
    }
  }, [notifyOnIncrease, platformLoaded]);

  usePolling(fetchData, 5000);

  const handlePlatformSelect = async (checkId: number, platform: string) => {
    setSelectingId(checkId);
    try {
      const headers: Record<string, string> = { "Content-Type": "application/json" };
      const key = typeof window !== "undefined" ? localStorage.getItem("admin_api_key") : null;
      if (key) headers["Authorization"] = `Bearer ${key}`;

      await fetch(`${API_BASE}/api/check/${checkId}/platform`, {
        method: "POST",
        headers,
        body: JSON.stringify({ platform, remember }),
      });
      fetchData();
    } catch {
      // ignore
    }
    setSelectingId(null);
  };

  const isProcessingStatus = (status: string) =>
    ["pending", "parsing", "matching", "checking"].includes(status);

  const stats = [
    { label: "今日の確認数", value: todayTotal, color: "text-gray-900", bg: "bg-white", urgent: false },
    { label: "処理中", value: processingCount, color: "text-blue-600", bg: "bg-blue-50", urgent: false },
    { label: "PF選択待ち", value: awaitingCount, color: "text-orange-600", bg: awaitingCount > 0 ? "bg-orange-50" : "bg-white", urgent: awaitingCount > 0 },
    { label: "電話タスク未対応", value: phoneTasksPending, color: "text-red-600", bg: phoneTasksPending > 0 ? "bg-red-50" : "bg-white", urgent: phoneTasksPending > 0 },
  ];

  return (
    <div className="space-y-8">
      {/* エラーバナー */}
      {apiError && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 flex items-center gap-3">
          <svg className="w-5 h-5 text-red-600 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
          </svg>
          <p className="text-sm text-red-700">{apiError}</p>
        </div>
      )}

      {/* 通知バナー */}
      {permission === "default" && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 flex items-center justify-between">
          <p className="text-sm text-blue-800">
            デスクトップ通知を有効にすると、PF選択待ちや新しい電話タスクをリアルタイムで受け取れます。
          </p>
          <button
            onClick={requestPermission}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 shrink-0 ml-4 transition-colors"
          >
            通知を有効にする
          </button>
        </div>
      )}

      {/* 統計カード */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map((stat) => (
          <div
            key={stat.label}
            className={`${stat.bg} rounded-xl shadow-sm border border-gray-200 p-5 transition-shadow hover:shadow-md ${
              stat.urgent ? "ring-2 ring-orange-200" : ""
            }`}
          >
            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">
              {stat.label}
            </p>
            <p className={`text-3xl font-bold mt-2 tabular-nums ${stat.color}`}>
              {stat.value}
            </p>
          </div>
        ))}
      </div>

      {/* チャートエリア */}
      {(dailyCounts.length > 0 || resultDistribution.length > 0) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {/* ミニ棒グラフ */}
          {dailyCounts.length > 0 && (
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
              <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-4">
                過去7日間の確認件数
              </h3>
              <div className="flex items-end gap-2 h-24">
                {dailyCounts.map((d) => {
                  const maxCount = Math.max(...dailyCounts.map((x) => x.count), 1);
                  const height = (d.count / maxCount) * 100;
                  const dayLabel = new Date(d.date + "T00:00:00").toLocaleDateString("ja-JP", { weekday: "short" });
                  return (
                    <div key={d.date} className="flex-1 flex flex-col items-center gap-1">
                      <span className="text-xs text-gray-500 tabular-nums">{d.count}</span>
                      <div className="w-full relative" style={{ height: "80px" }}>
                        <div
                          className="absolute bottom-0 w-full bg-blue-500 rounded-t transition-all duration-500 hover:bg-blue-600"
                          style={{ height: `${Math.max(height, 4)}%` }}
                        />
                      </div>
                      <span className="text-[10px] text-gray-400">{dayLabel}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* ドーナツチャート */}
          {resultDistribution.length > 0 && (() => {
            const total = resultDistribution.reduce((sum, d) => sum + d.count, 0);
            let accumulated = 0;
            const segments = resultDistribution.map((d) => {
              const start = accumulated;
              const end = accumulated + (d.count / total) * 360;
              accumulated = end;
              return `${d.color} ${start}deg ${end}deg`;
            });
            const gradient = `conic-gradient(${segments.join(", ")})`;

            return (
              <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
                <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-4">
                  結果分布
                </h3>
                <div className="flex items-center gap-6">
                  <div
                    className="w-20 h-20 rounded-full shrink-0"
                    style={{
                      background: gradient,
                      mask: "radial-gradient(circle at center, transparent 55%, black 56%)",
                      WebkitMask: "radial-gradient(circle at center, transparent 55%, black 56%)",
                    }}
                  />
                  <div className="space-y-1.5 flex-1 min-w-0">
                    {resultDistribution.map((d) => (
                      <div key={d.label} className="flex items-center gap-2 text-xs">
                        <span
                          className="w-2.5 h-2.5 rounded-full shrink-0"
                          style={{ backgroundColor: d.color }}
                        />
                        <span className="text-gray-600 truncate">{d.label}</span>
                        <span className="text-gray-400 tabular-nums ml-auto">
                          {d.count}件 ({Math.round((d.count / total) * 100)}%)
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            );
          })()}
        </div>
      )}

      {/* 要対応セクション: PF選択キュー */}
      {awaitingItems.length > 0 && (
        <section className="bg-white rounded-xl shadow-sm border border-orange-200 p-6">
          <div className="flex items-center gap-2 mb-4">
            <span className="w-2 h-2 rounded-full bg-orange-500 animate-gentle-pulse" />
            <h2 className="text-lg font-semibold text-gray-900">
              プラットフォーム選択待ち
            </h2>
            <span className="text-xs px-2 py-0.5 bg-orange-100 text-orange-700 rounded-full font-medium">
              {awaitingItems.length}件
            </span>
          </div>
          <div className="space-y-4">
            {awaitingItems.map((item) => (
              <div
                key={item.id}
                className="border border-orange-200 bg-orange-50/30 rounded-lg p-4"
              >
                <div className="flex flex-col lg:flex-row lg:items-start justify-between gap-4">
                  <div className="min-w-0">
                    <Link
                      href={`/check/${item.id}`}
                      className="text-sm font-semibold text-gray-900 hover:text-blue-600 transition-colors"
                    >
                      #{item.id} {item.property_name || "(解析中)"}
                    </Link>
                    {item.atbb_company && (
                      <p className="text-xs text-gray-500 mt-0.5">
                        管理会社: {item.atbb_company}
                      </p>
                    )}
                    {item.property_address && (
                      <p className="text-xs text-gray-400 mt-0.5">
                        {item.property_address}
                      </p>
                    )}
                  </div>
                  <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2 shrink-0">
                    {(
                      [
                        ["itanji", "イタンジBB"],
                        ["es_square", "いい生活スクエア"],
                        ["goweb", "GoWeb"],
                        ["bukkaku", "物確.com"],
                        ["es_b2b", "いい生活B2B"],
                      ] as const
                    ).map(([key, label]) => {
                      const status = platformStatus[key];
                      const isConfigured = status?.configured !== false;
                      return (
                        <button
                          key={key}
                          onClick={() => handlePlatformSelect(item.id, key)}
                          disabled={selectingId === item.id || !isConfigured}
                          className={`px-3 py-2.5 border rounded-lg text-xs font-medium transition-all disabled:opacity-40 ${
                            isConfigured
                              ? "border-gray-200 hover:bg-blue-600 hover:text-white hover:border-blue-600 active:scale-95"
                              : "border-gray-100 text-gray-300 bg-gray-50 cursor-not-allowed"
                          }`}
                        >
                          {label}
                        </button>
                      );
                    })}
                  </div>
                </div>
                <label className="flex items-center gap-2 mt-3 text-xs text-gray-500 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={remember}
                    onChange={(e) => setRemember(e.target.checked)}
                    className="rounded"
                  />
                  この選択を記憶する
                </label>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* PF選択待ちが0件の場合 */}
      {awaitingItems.length === 0 && (
        <section className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            プラットフォーム選択待ち
          </h2>
          <p className="text-sm text-gray-400">選択待ちはありません</p>
        </section>
      )}

      {/* 最近の確認結果 */}
      <section className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          最近の確認結果
        </h2>
        {recentChecks.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <div className="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mb-3">
              <svg className="w-6 h-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-2.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
              </svg>
            </div>
            <p className="text-sm text-gray-400">まだ確認結果がありません</p>
          </div>
        ) : (
          <div className="space-y-2" aria-live="polite">
            {recentChecks.map((item) => (
              <Link
                key={item.id}
                href={`/check/${item.id}`}
                className="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50 border border-gray-100 transition-all hover:border-gray-200 hover:shadow-sm group"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <span className="text-xs text-gray-400 font-mono w-8 shrink-0">
                    #{item.id}
                  </span>
                  <span className="text-sm text-gray-900 truncate font-medium">
                    {item.property_name || "(解析中)"}
                  </span>
                  {item.portal_source && (
                    <span className="text-xs px-2 py-0.5 bg-gray-100 text-gray-500 rounded shrink-0 hidden sm:inline">
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
                      className={`text-xs px-2.5 py-1 rounded-full font-medium ${getResultStyle(
                        item.vacancy_result,
                      )}`}
                    >
                      {item.vacancy_result.length > 10
                        ? item.vacancy_result.slice(0, 10) + "..."
                        : item.vacancy_result}
                    </span>
                  ) : (
                    <span
                      className={`text-xs font-medium ${
                        ADMIN_STATUS_STYLES[item.status] || "text-gray-400"
                      }`}
                    >
                      {isProcessingStatus(item.status) && (
                        <span className="relative inline-flex h-2 w-2 mr-1.5 align-middle">
                          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75" />
                          <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500" />
                        </span>
                      )}
                      {ADMIN_STATUS_LABELS[item.status] || item.status}
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
              </Link>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
