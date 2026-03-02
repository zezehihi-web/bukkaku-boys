"use client";

import { usePathname } from "next/navigation";

export function NavBar() {
  const pathname = usePathname();

  // 管理画面ではNavBarを非表示（admin/layout.tsxが独自ナビを持つ）
  if (pathname.startsWith("/admin")) {
    return null;
  }

  // ユーザー画面: ミニマルなヘッダー
  return (
    <header className="bg-white border-b border-gray-200 px-6 py-4">
      <div className="max-w-4xl mx-auto flex items-center justify-between">
        <a href="/" className="text-xl font-bold text-gray-900">
          空確くん
        </a>
      </div>
    </header>
  );
}
