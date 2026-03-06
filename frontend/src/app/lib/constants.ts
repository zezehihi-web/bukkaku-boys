// ユーザー向け: 内部ステップを隠すシンプル表示
export const USER_STATUS_LABELS: Record<string, string> = {
  pending: "確認中...",
  parsing: "確認中...",
  matching: "確認中...",
  awaiting_platform: "確認中...",
  checking: "確認中...",
  done: "完了",
  not_found: "確認不可",
  error: "確認不可",
};

// 管理者向け: 全ステップ表示
export const ADMIN_STATUS_LABELS: Record<string, string> = {
  pending: "待機中",
  parsing: "URL解析中",
  matching: "ATBB照合中",
  awaiting_platform: "PF選択待ち",
  checking: "空室確認中",
  done: "完了",
  not_found: "該当なし",
  error: "エラー",
};

export const ADMIN_STATUS_STYLES: Record<string, string> = {
  pending: "text-gray-400",
  parsing: "text-blue-500",
  matching: "text-blue-500",
  awaiting_platform: "text-orange-500",
  checking: "text-blue-500",
  done: "text-green-600",
  not_found: "text-gray-500",
  error: "text-red-500",
};

export const RESULT_STYLES: Record<string, string> = {
  "募集中": "bg-green-100 text-green-800",
  "申込あり": "bg-yellow-100 text-yellow-800",
  "募集終了": "bg-red-100 text-red-800",
  "該当なし": "bg-gray-100 text-gray-800",
  "確認不可": "bg-orange-100 text-orange-800",
  "電話確認": "bg-blue-100 text-blue-800",
};

export const RESULT_DETAIL_STYLES: Record<
  string,
  { bg: string; text: string; icon: string }
> = {
  "募集中": { bg: "bg-green-50", text: "text-green-700", icon: "bg-green-500" },
  "申込あり": { bg: "bg-yellow-50", text: "text-yellow-700", icon: "bg-yellow-500" },
  "募集終了": { bg: "bg-red-50", text: "text-red-700", icon: "bg-red-500" },
  "該当なし": { bg: "bg-gray-50", text: "text-gray-700", icon: "bg-gray-500" },
  "確認不可": { bg: "bg-orange-50", text: "text-orange-700", icon: "bg-orange-500" },
  "電話確認": { bg: "bg-blue-50", text: "text-blue-700", icon: "bg-blue-500" },
};

export const PLATFORM_LABELS: Record<string, string> = {
  itanji: "イタンジBB",
  es_square: "いい生活スクエア",
  goweb: "GoWeb",
  bukkaku: "物確.com",
  es_b2b: "いい生活B2B",
};

export const PLATFORM_OPTIONS = [
  { value: "itanji", label: "イタンジBB" },
  { value: "es_square", label: "いい生活スクエア" },
  { value: "goweb", label: "GoWeb" },
  { value: "bukkaku", label: "物確.com" },
  { value: "es_b2b", label: "いい生活B2B" },
] as const;

export function getResultStyle(result: string): string {
  if (RESULT_STYLES[result]) return RESULT_STYLES[result];
  for (const [key, style] of Object.entries(RESULT_STYLES)) {
    if (result.startsWith(key)) return style;
  }
  return "bg-gray-100 text-gray-700";
}

export function getResultDetailStyle(result: string) {
  if (RESULT_DETAIL_STYLES[result]) return RESULT_DETAIL_STYLES[result];
  for (const [key, style] of Object.entries(RESULT_DETAIL_STYLES)) {
    if (result.startsWith(key)) return style;
  }
  return RESULT_DETAIL_STYLES["該当なし"];
}

export const isProcessing = (status: string) =>
  ["pending", "parsing", "matching", "awaiting_platform", "checking"].includes(status);

export const isTerminal = (status: string) =>
  ["done", "not_found", "error"].includes(status);
