export function getEmpireKey() {
  try {
    return (localStorage.getItem("empire_api_key") || "").trim();
  } catch {
    return "";
  }
}

export async function fetchJSON(url) {
  const headers = {};
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, { headers });
  if (!r.ok) {
    throw new Error(`HTTP ${r.status}`);
  }
  return r.json();
}

export async function postJSON(url, body) {
  const headers = { "content-type": "application/json" };
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body || {}),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(data && data.error ? data.error : `HTTP ${r.status}`);
  }
  return data;
}

export async function putJSON(url, body) {
  const headers = { "content-type": "application/json" };
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, {
    method: "PUT",
    headers,
    body: JSON.stringify(body || {}),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(data && data.error ? data.error : `HTTP ${r.status}`);
  }
  return data;
}

export async function deleteJSON(url) {
  const headers = {};
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, { method: "DELETE", headers });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(data && data.error ? data.error : `HTTP ${r.status}`);
  }
  return data;
}
