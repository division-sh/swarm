export function readSavedPositions(storageKey) {
  try {
    const raw = localStorage.getItem(storageKey) || "";
    if (!raw) return new Map();
    const obj = JSON.parse(raw);
    const positions = new Map();
    for (const [id, point] of Object.entries(obj || {})) {
      if (!point || typeof point.x !== "number" || typeof point.y !== "number") continue;
      positions.set(id, { x: point.x, y: point.y });
    }
    return positions;
  } catch {
    return new Map();
  }
}

export function writeSavedPositions(storageKey, nodes) {
  try {
    const serialized = {};
    for (const node of nodes || []) {
      if (!node || !node.id || !node.position) continue;
      serialized[node.id] = { x: node.position.x, y: node.position.y };
    }
    localStorage.setItem(storageKey, JSON.stringify(serialized));
  } catch {}
}
