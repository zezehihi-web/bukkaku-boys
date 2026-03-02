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
    tick();
    const id = setInterval(tick, intervalMs);
    return () => clearInterval(id);
  }, [tick, intervalMs, enabled]);
}
