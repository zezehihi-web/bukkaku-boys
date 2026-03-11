"use client";

import { useEffect, useRef, useCallback } from "react";

export function usePolling(
  callback: () => void | Promise<void>,
  intervalMs: number,
  enabled: boolean = true,
) {
  const savedCallback = useRef(callback);
  savedCallback.current = callback;

  const tick = useCallback(async () => {
    await savedCallback.current();
  }, []);

  useEffect(() => {
    if (!enabled) return;

    let id: ReturnType<typeof setInterval>;

    const start = () => {
      tick();
      id = setInterval(tick, intervalMs);
    };

    const handleVisibility = () => {
      clearInterval(id);
      if (!document.hidden) {
        start();
      }
    };

    start();
    document.addEventListener("visibilitychange", handleVisibility);

    return () => {
      clearInterval(id);
      document.removeEventListener("visibilitychange", handleVisibility);
    };
  }, [tick, intervalMs, enabled]);
}
