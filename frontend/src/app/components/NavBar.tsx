"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

export function NavBar() {
  const pathname = usePathname();

  // 管理画面ではNavBarを非表示（admin/layout.tsxが独自ナビを持つ）
  if (pathname.startsWith("/admin")) {
    return null;
  }

  // ユーザー画面: ミニマルなstickyヘッダー
  return (
    <header className="bg-white/80 backdrop-blur-sm border-b border-gray-200 px-6 py-4 sticky top-0 z-50">
      <div className="max-w-4xl mx-auto flex items-center justify-between">
        <Link href="/" className="text-xl font-bold text-gray-900">
          空確くん
        </Link>
        <span className="text-xs text-gray-400">空室自動確認</span>
      </div>
    </header>
  );
}
