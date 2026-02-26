import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  // Turbopackが日本語パスでクラッシュするためwebpack使用
  turbopack: {
    root: path.resolve(__dirname),
  },
};

export default nextConfig;
