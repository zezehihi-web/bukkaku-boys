"""neon_listener をバックグラウンド起動するヘルパー"""
import subprocess
import sys
import os

env = os.environ.copy()
env["DATABASE_URL"] = "postgresql://neondb_owner:npg_t8L6IUlWuKgF@ep-floral-bread-a1kdt3p1-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
env["PYTHONIOENCODING"] = "utf-8"

# .env を読み込み
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(dotenv_path):
    with open(dotenv_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                env[key.strip()] = val.strip()

proc = subprocess.Popen(
    [sys.executable, "-m", "backend.neon_listener"],
    cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    env=env,
    stdout=open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "neon_listener.log"), "w", encoding="utf-8"),
    stderr=subprocess.STDOUT,
    creationflags=0x00000008,  # DETACHED_PROCESS on Windows
)
print(f"neon_listener started with PID: {proc.pid}")
