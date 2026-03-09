import { useEffect, useState } from "react";

export function usePersistentState<T>(storageKey: string, initialValue: T) {
  const [value, setValue] = useState<T>(() => {
    try {
      const raw = localStorage.getItem(storageKey);
      return (raw == null ? initialValue : raw as T);
    } catch {
      return initialValue;
    }
  });

  useEffect(() => {
    try {
      if (value == null || value === "") localStorage.removeItem(storageKey);
      else localStorage.setItem(storageKey, String(value));
    } catch {}
  }, [storageKey, value]);

  return [value, setValue] as const;
}
