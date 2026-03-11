import Link from "next/link";
import { getResultStyle } from "../../lib/constants";
import type { CheckItem } from "../../lib/types";

type CheckListItemProps = {
  item: CheckItem;
  variant?: "user" | "admin";
};

const ADMIN_STATUS_LABELS: Record<string, string> = {
  pending: "待機中",
  parsing: "URL解析中",
  matching: "ATBB照合中",
  awaiting_platform: "PF選択待ち",
  checking: "空室確認中",
  done: "完了",
  not_found: "該当なし",
  error: "エラー",
};

const ADMIN_STATUS_STYLES: Record<string, string> = {
  pending: "text-gray-400",
  parsing: "text-blue-500",
  matching: "text-blue-500",
  awaiting_platform: "text-orange-500",
  checking: "text-blue-500",
  done: "text-green-600",
  not_found: "text-gray-500",
  error: "text-red-500",
};

const isProcessingStatus = (status: string) =>
  ["pending", "parsing", "matching", "checking"].includes(status);

export function CheckListItem({ item, variant = "user" }: CheckListItemProps) {
  return (
    <Link
      href={`/check/${item.id}`}
      className="flex items-center justify-between p-4 rounded-xl hover:bg-gray-50 border border-gray-100 transition-all hover:border-gray-200 hover:shadow-sm group"
    >
      <div className="flex items-center gap-3 min-w-0">
        {variant === "admin" && (
          <span className="text-xs text-gray-400 font-mono w-8 shrink-0">
            #{item.id}
          </span>
        )}
        <span className="text-sm text-gray-900 truncate font-medium">
          {item.property_name || "(確認中)"}
        </span>
        {variant === "admin" && item.portal_source && (
          <span className="text-xs px-2 py-0.5 bg-gray-100 text-gray-500 rounded shrink-0">
            {item.portal_source.toUpperCase()}
          </span>
        )}
      </div>
      <div className="flex items-center gap-3 shrink-0">
        {variant === "admin" && item.created_at && (
          <span className="text-xs text-gray-300 hidden sm:inline">
            {item.created_at.split(" ")[0]}
          </span>
        )}
        {item.vacancy_result ? (
          <span
            className={`text-xs px-2.5 py-1 rounded-full font-medium ${getResultStyle(item.vacancy_result)}`}
          >
            {item.vacancy_result.length > 10
              ? item.vacancy_result.slice(0, 10) + "..."
              : item.vacancy_result}
          </span>
        ) : variant === "admin" ? (
          <span className={`text-xs font-medium ${ADMIN_STATUS_STYLES[item.status] || "text-gray-400"}`}>
            {isProcessingStatus(item.status) && (
              <span className="relative inline-flex h-2 w-2 mr-1.5 align-middle">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500" />
              </span>
            )}
            {ADMIN_STATUS_LABELS[item.status] || item.status}
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
    </Link>
  );
}
