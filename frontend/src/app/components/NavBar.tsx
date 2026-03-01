"use client";

import { useState, useEffect } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export function NavBar() {
  const [phoneCount, setPhoneCount] = useState(0);

  useEffect(() => {
    const fetchCount = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/phone-tasks/count`);
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
  }, []);

  return (
    <header className="bg-white border-b border-gray-200 px-6 py-4">
      <div className="max-w-4xl mx-auto flex items-center justify-between">
        <a href="/" className="text-xl font-bold text-gray-900">
          空確くん
        </a>
        <nav className="flex gap-4 text-sm">
          <a href="/" className="text-gray-600 hover:text-gray-900">
            ホーム
          </a>
          <a
            href="/phone-tasks"
            className="text-gray-600 hover:text-gray-900 relative"
          >
            電話タスク
            {phoneCount > 0 && (
              <span className="absolute -top-2 -right-4 min-w-[18px] h-[18px] flex items-center justify-center px-1 bg-red-500 text-white text-[10px] font-bold rounded-full">
                {phoneCount}
              </span>
            )}
          </a>
          <a
            href="/knowledge"
            className="text-gray-600 hover:text-gray-900"
          >
            ナレッジ管理
          </a>
        </nav>
      </div>
    </header>
  );
}
