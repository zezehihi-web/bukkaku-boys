"use client";

import { useState, useEffect, useCallback, useRef } from "react";

type NotificationState = "default" | "granted" | "denied" | "unsupported";

export function useNotifications() {
  const [permission, setPermission] = useState<NotificationState>("default");
  const prevCounts = useRef<Record<string, number>>({});

  useEffect(() => {
    if (typeof window === "undefined" || !("Notification" in window)) {
      setPermission("unsupported");
      return;
    }
    setPermission(Notification.permission as NotificationState);
  }, []);

  const requestPermission = useCallback(async () => {
    if (typeof window === "undefined" || !("Notification" in window)) return;
    const result = await Notification.requestPermission();
    setPermission(result as NotificationState);
  }, []);

  const notify = useCallback(
    (title: string, body?: string) => {
      if (permission !== "granted") return;
      new Notification(title, { body, icon: "/favicon.ico" });
    },
    [permission],
  );

  const notifyOnIncrease = useCallback(
    (key: string, count: number, title: string, body?: string) => {
      const prev = prevCounts.current[key] ?? count;
      prevCounts.current[key] = count;
      if (count > prev) {
        notify(title, body);
      }
    },
    [notify],
  );

  return { permission, requestPermission, notify, notifyOnIncrease };
}
