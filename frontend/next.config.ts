import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // ローカル開発では日本語パスが問題になるため
  // NEXT_WEBPACK=1 環境変数でwebpack使用可能
  // Vercelでは自動でTurbopackが使われる（パスがASCIIなので問題なし）
};

export default nextConfig;
