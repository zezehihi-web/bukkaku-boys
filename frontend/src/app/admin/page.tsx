"use client";

import { useState, useEffect, useCallback } from "react";
import { apiFetchSafe, API_BASE } from "../lib/api";
import { usePolling } from "../lib/usePolling";
import { useNotifications } from "../lib/useNotifications";
import {
  ADMIN_STATUS_LABELS,
  ADMIN_STATUS_STYLES,
  RESULT_STYLES,
  PLATFORM_LABELS,
  getResultStyle,
} from "../lib/constants";
import type { CheckItem, CheckStatus, PlatformStatus } from "../lib/types";

type AwaitingItem = CheckStatus;

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

  // 最近の確認結果
  const [recentChecks, setRecentChecks] = useState<CheckItem[]>([]);

  // 通知
  const { permission, requestPermission, notifyOnIncrease } =
    useNotifications();

  // プラットフォーム設定状態を初回取得
  useEffect(() => {
    apiFetchSafe<PlatformStatus>("/api/platforms/status").then(
      (data) => data && setPlatformStatus(data),
    );
  }, []);

  const fetchData = useCallback(async () => {
    // 最近のチェック一覧
    const checks = await apiFetchSafe<CheckItem[]>("/api/checks?limit=30");
    if (checks) {
      setRecentChecks(checks);
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
  }, [notifyOnIncrease]);

  usePolling(fetchData, 5000);

  const handlePlatformSelect = async (checkId: number, platform: string) => {
    setSelectingId(checkId);
    try {
      await fetch(`${API_BASE}/api/check/${checkId}/platform`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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

  return (
    <div className="space-y-8">
      {/* 通知バナー */}
      {permission === "default" && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 flex items-center justify-between">
          <p className="text-sm text-blue-800">
            デスクトップ通知を有効にすると、PF選択待ちや新しい電話タスクをリアルタイムで受け取れます。
          </p>
          <button
            onClick={requestPermission}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 shrink-0 ml-4"
          >
            通知を有効にする
          </button>
        </div>
      )}

      {/* 統計カード */}
      <div className="grid grid-cols-4 gap-4">
        {[
          {
            label: "今日の確認数",
            value: todayTotal,
            color: "text-gray-900",
            bg: "bg-white",
          },
          {
            label: "処理中",
            value: processingCount,
            color: "text-blue-600",
            bg: "bg-blue-50",
          },
          {
            label: "PF選択待ち",
            value: awaitingCount,
            color: "text-orange-600",
            bg: awaitingCount > 0 ? "bg-orange-50" : "bg-white",
          },
          {
            label: "電話タスク未対応",
            value: phoneTasksPending,
            color: "text-red-600",
            bg: phoneTasksPending > 0 ? "bg-red-50" : "bg-white",
          },
        ].map((stat) => (
          <div
            key={stat.label}
            className={`${stat.bg} rounded-xl shadow-sm border border-gray-200 p-5`}
          >
            <p className="text-xs font-medium text-gray-500">{stat.label}</p>
            <p className={`text-3xl font-bold mt-1 ${stat.color}`}>
              {stat.value}
            </p>
          </div>
        ))}
      </div>

      {/* PF選択キュー */}
      <section className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          プラットフォーム選択待ち
        </h2>
        {awaitingItems.length === 0 ? (
          <p className="text-sm text-gray-400">選択待ちはありません</p>
        ) : (
          <div className="space-y-4">
            {awaitingItems.map((item) => (
              <div
                key={item.id}
                className="border border-orange-200 bg-orange-50/30 rounded-lg p-4"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <a
                      href={`/check/${item.id}`}
                      className="text-sm font-semibold text-gray-900 hover:text-blue-600"
                    >
                      #{item.id} {item.property_name || "(解析中)"}
                    </a>
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
                  <div className="flex items-center gap-2 shrink-0">
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
                          disabled={selectingId === item.id}
                          className={`px-3 py-1.5 border rounded-lg text-xs font-medium transition-colors disabled:opacity-50 ${
                            isConfigured
                              ? "border-gray-300 hover:bg-blue-50 hover:border-blue-300"
                              : "border-gray-200 text-gray-400 bg-gray-50"
                          }`}
                        >
                          {label}
                        </button>
                      );
                    })}
                  </div>
                </div>
                <label className="flex items-center gap-2 mt-2 text-xs text-gray-500">
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
        )}
      </section>

      {/* 最近の確認結果 */}
      <section className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          最近の確認結果
        </h2>
        {recentChecks.length === 0 ? (
          <p className="text-sm text-gray-400">まだ確認結果がありません</p>
        ) : (
          <div className="space-y-2">
            {recentChecks.map((item) => (
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
                        <span className="inline-block w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse mr-1.5 align-middle" />
                      )}
                      {ADMIN_STATUS_LABELS[item.status] || item.status}
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
