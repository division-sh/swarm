import { useEffect, useState } from "react";

export function usePersistentState(storageKey, initialValue) {
  const [value, setValue] = useState(() => {
    try {
      const raw = localStorage.getItem(storageKey);
      return raw == null ? initialValue : raw;
    } catch {
      return initialValue;
    }
  });

  useEffect(() => {
    try {
      if (value == null || value === "") localStorage.removeItem(storageKey);
      else localStorage.setItem(storageKey, value);
    } catch {}
  }, [storageKey, value]);

  return [value, setValue];
}
