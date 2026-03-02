"use client";

import { usePathname } from "next/navigation";

export function MainContent({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const isAdmin = pathname.startsWith("/admin");

  if (isAdmin) {
    // 管理画面: admin/layout.tsxが独自レイアウトを持つのでラップしない
    return <>{children}</>;
  }

  // ユーザー画面: 中央寄せコンテナ
  return (
    <main className="max-w-4xl mx-auto px-6 py-8">{children}</main>
  );
}
