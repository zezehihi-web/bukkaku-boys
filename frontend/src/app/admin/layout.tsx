"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useState, useEffect } from "react";
import { API_BASE } from "../lib/api";

const NAV_ITEMS = [
  { href: "/admin", label: "ダッシュボード", icon: "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1" },
  { href: "/admin/phone-tasks", label: "電話タスク", icon: "M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z" },
  { href: "/admin/knowledge", label: "ナレッジ管理", icon: "M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" },
];

export default function AdminLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const router = useRouter();
  const [phoneCount, setPhoneCount] = useState(0);
  const [authChecked, setAuthChecked] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const isLoginPage = pathname === "/admin/login";

  // 認証チェック
  useEffect(() => {
    if (isLoginPage) {
      setAuthChecked(true);
      setAuthenticated(true);
      return;
    }

    const checkAuth = async () => {
      const key = localStorage.getItem("admin_api_key");
      if (!key) {
        router.push("/admin/login");
        return;
      }

      try {
        const res = await fetch(`${API_BASE}/api/auth/verify`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${key}`,
          },
        });

        if (res.ok) {
          setAuthenticated(true);
        } else {
          localStorage.removeItem("admin_api_key");
          router.push("/admin/login");
        }
      } catch {
        setAuthenticated(true);
      } finally {
        setAuthChecked(true);
      }
    };

    checkAuth();
  }, [isLoginPage, router]);

  // 電話タスク数のポーリング
  useEffect(() => {
    if (!authenticated || isLoginPage) return;

    const fetchCount = async () => {
      try {
        const key = localStorage.getItem("admin_api_key");
        const headers: Record<string, string> = {};
        if (key) headers["Authorization"] = `Bearer ${key}`;

        const res = await fetch(`${API_BASE}/api/phone-tasks/count`, { headers });
        if (res.ok) {
          const data = await res.json();
          setPhoneCount(data.count || 0);
        }
      } catch {
        // ignore
      }
    };
    fetchCount();
    const interval = setInterval(fetchCount, 15000);
    return () => clearInterval(interval);
  }, [authenticated, isLoginPage]);

  // ページ遷移時にサイドバーを閉じる
  useEffect(() => {
    setSidebarOpen(false);
  }, [pathname]);

  // ログインページ
  if (isLoginPage) {
    return <>{children}</>;
  }

  // 認証チェック中
  if (!authChecked) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-gray-50">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (!authenticated) {
    return null;
  }

  return (
    <div className="flex min-h-screen">
      {/* モバイルオーバーレイ */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* サイドバー */}
      <aside
        className={`
          fixed lg:static inset-y-0 left-0 z-50
          w-56 bg-gray-900 text-gray-300 flex flex-col shrink-0
          transform transition-transform duration-200 ease-in-out
          ${sidebarOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0"}
        `}
      >
        <div className="px-4 py-5 border-b border-gray-800 flex items-center justify-between">
          <div>
            <Link href="/admin" className="text-lg font-bold text-white">
              空確くん
            </Link>
            <p className="text-xs text-gray-500 mt-0.5">管理画面</p>
          </div>
          {/* モバイル: 閉じるボタン */}
          <button
            onClick={() => setSidebarOpen(false)}
            className="lg:hidden p-1.5 rounded-lg hover:bg-gray-800 text-gray-400 hover:text-white transition-colors"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <nav className="flex-1 px-3 py-4 space-y-1">
          {NAV_ITEMS.map((item) => {
            const isActive =
              item.href === "/admin"
                ? pathname === "/admin"
                : pathname.startsWith(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? "bg-gray-800 text-white"
                    : "hover:bg-gray-800/50 hover:text-white"
                }`}
              >
                <svg
                  className="w-5 h-5 shrink-0"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d={item.icon}
                  />
                </svg>
                {item.label}
                {item.href === "/admin/phone-tasks" && phoneCount > 0 && (
                  <span className="ml-auto min-w-[20px] h-5 flex items-center justify-center px-1.5 bg-red-500 text-white text-[10px] font-bold rounded-full">
                    {phoneCount}
                  </span>
                )}
              </Link>
            );
          })}
        </nav>
        <div className="px-4 py-3 border-t border-gray-800 space-y-2">
          <Link
            href="/"
            className="flex items-center gap-2 text-sm text-gray-400 hover:text-white px-3 py-2 rounded-lg hover:bg-gray-800/50 transition-colors"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15 19l-7-7 7-7" />
            </svg>
            ユーザー画面へ
          </Link>
          <button
            onClick={() => {
              localStorage.removeItem("admin_api_key");
              router.push("/admin/login");
            }}
            className="flex items-center gap-2 text-xs text-gray-500 hover:text-gray-300 px-3 py-1.5 transition-colors w-full"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 9V5.25A2.25 2.25 0 0013.5 3h-6a2.25 2.25 0 00-2.25 2.25v13.5A2.25 2.25 0 007.5 21h6a2.25 2.25 0 002.25-2.25V15m3 0l3-3m0 0l-3-3m3 3H9" />
            </svg>
            ログアウト
          </button>
        </div>
      </aside>

      {/* メインコンテンツ */}
      <main className="flex-1 bg-gray-50 overflow-auto min-w-0">
        {/* モバイル用ヘッダー */}
        <div className="lg:hidden sticky top-0 z-30 bg-white border-b border-gray-200 px-4 py-3 flex items-center gap-3">
          <button
            onClick={() => setSidebarOpen(true)}
            className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors"
          >
            <svg className="w-5 h-5 text-gray-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          </button>
          <span className="font-semibold text-gray-900 text-sm">空確くん 管理</span>
          {phoneCount > 0 && (
            <span className="ml-auto min-w-[20px] h-5 flex items-center justify-center px-1.5 bg-red-500 text-white text-[10px] font-bold rounded-full">
              {phoneCount}
            </span>
          )}
        </div>
        <div className="max-w-6xl mx-auto px-4 sm:px-6 py-6 sm:py-8">{children}</div>
      </main>
    </div>
  );
}
