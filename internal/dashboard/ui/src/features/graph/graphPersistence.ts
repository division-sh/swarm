type SavedPoint = { x: number; y: number };

export function readSavedPositions(storageKey: string) {
  try {
    const raw = localStorage.getItem(storageKey) || "";
    if (!raw) return new Map<string, SavedPoint>();
    const obj = JSON.parse(raw) as Record<string, unknown>;
    const positions = new Map<string, SavedPoint>();
    for (const [id, point] of Object.entries(obj || {})) {
      if (!point || typeof point !== "object") continue;
      const candidate = point as Partial<SavedPoint>;
      if (typeof candidate.x !== "number" || typeof candidate.y !== "number") continue;
      positions.set(id, { x: candidate.x, y: candidate.y });
    }
    return positions;
  } catch {
    return new Map<string, SavedPoint>();
  }
}

export function writeSavedPositions(storageKey: string, nodes: Array<{ id?: string; position?: SavedPoint }> | null | undefined) {
  try {
    const serialized: Record<string, SavedPoint> = {};
    for (const node of nodes || []) {
      if (!node || !node.id || !node.position) continue;
      serialized[node.id] = { x: node.position.x, y: node.position.y };
    }
    localStorage.setItem(storageKey, JSON.stringify(serialized));
  } catch {}
}

export function readSavedView(storageKey) {
  try {
    const raw = localStorage.getItem(storageKey) || "";
    if (!raw) return {};
    const data = JSON.parse(raw);
    return data && typeof data === "object" ? data : {};
  } catch {
    return {};
  }
}

export function writeSavedView(storageKey, value) {
  try {
    localStorage.setItem(storageKey, JSON.stringify(value || {}));
  } catch {}
}
