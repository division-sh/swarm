export function getEmpireKey(): string {
  try {
    return (localStorage.getItem("empire_api_key") || "").trim();
  } catch {
    return "";
  }
}

export async function fetchJSON<T = unknown>(url: string): Promise<T> {
  const headers: Record<string, string> = {};
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, { headers });
  if (!r.ok) {
    throw new Error(`HTTP ${r.status}`);
  }
  return r.json() as Promise<T>;
}

export async function postJSON<T = unknown>(url: string, body: Record<string, unknown>): Promise<T> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body || {}),
  });
  const data = await r.json().catch(() => ({} as Record<string, unknown>));
  if (!r.ok) {
    const message = typeof data === "object" && data && "error" in data ? String((data as Record<string, unknown>).error || "") : "";
    throw new Error(message || `HTTP ${r.status}`);
  }
  return data as T;
}

export async function putJSON<T = unknown>(url: string, body: Record<string, unknown>): Promise<T> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, {
    method: "PUT",
    headers,
    body: JSON.stringify(body || {}),
  });
  const data = await r.json().catch(() => ({} as Record<string, unknown>));
  if (!r.ok) {
    const message = typeof data === "object" && data && "error" in data ? String((data as Record<string, unknown>).error || "") : "";
    throw new Error(message || `HTTP ${r.status}`);
  }
  return data as T;
}

export async function deleteJSON<T = unknown>(url: string): Promise<T> {
  const headers: Record<string, string> = {};
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, { method: "DELETE", headers });
  const data = await r.json().catch(() => ({} as Record<string, unknown>));
  if (!r.ok) {
    const message = typeof data === "object" && data && "error" in data ? String((data as Record<string, unknown>).error || "") : "";
    throw new Error(message || `HTTP ${r.status}`);
  }
  return data as T;
}
