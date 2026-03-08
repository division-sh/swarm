import React from "react";
import { BaseEdge, MarkerType, useInternalNode } from "@xyflow/react";

function clipLineToRect(x1, y1, x2, y2, rx, ry, rw, rh) {
  const cx = rx + rw / 2;
  const cy = ry + rh / 2;
  const dx = x2 - x1;
  const dy = y2 - y1;
  if (dx === 0 && dy === 0) return { x: cx, y: cy };
  const hw = rw / 2 + 2;
  const hh = rh / 2 + 2;
  let tMin = Infinity;
  const sides = [
    { nx: -1, ny: 0, d: -hw },
    { nx: 1, ny: 0, d: -hw },
    { nx: 0, ny: -1, d: -hh },
    { nx: 0, ny: 1, d: -hh },
  ];
  for (const side of sides) {
    const edgeX = cx + side.nx * hw;
    const edgeY = cy + side.ny * hh;
    const denom = side.nx !== 0 ? dx : dy;
    if (denom === 0) continue;
    const t = side.nx !== 0 ? (edgeX - x1) / dx : (edgeY - y1) / dy;
    if (t < 0 || t > 1) continue;
    const ix = x1 + dx * t;
    const iy = y1 + dy * t;
    if (Math.abs(ix - cx) <= hw + 0.5 && Math.abs(iy - cy) <= hh + 0.5 && t < tMin) tMin = t;
  }
  if (!Number.isFinite(tMin)) return { x: cx, y: cy };
  return { x: x1 + dx * tMin, y: y1 + dy * tMin };
}

export function StraightClippedEdge({ id, source, target, style, markerEnd }) {
  const sourceNode = useInternalNode(source);
  const targetNode = useInternalNode(target);
  if (!sourceNode || !targetNode) return null;

  const sourcePos = sourceNode.internals.positionAbsolute || sourceNode.position;
  const targetPos = targetNode.internals.positionAbsolute || targetNode.position;
  const sourceWidth = sourceNode.measured?.width || sourceNode.width || 210;
  const sourceHeight = sourceNode.measured?.height || sourceNode.height || 60;
  const targetWidth = targetNode.measured?.width || targetNode.width || 210;
  const targetHeight = targetNode.measured?.height || targetNode.height || 60;

  const sx = sourcePos.x + sourceWidth / 2;
  const sy = sourcePos.y + sourceHeight / 2;
  const tx = targetPos.x + targetWidth / 2;
  const ty = targetPos.y + targetHeight / 2;

  const start = clipLineToRect(sx, sy, tx, ty, sourcePos.x, sourcePos.y, sourceWidth, sourceHeight);
  const end = clipLineToRect(tx, ty, sx, sy, targetPos.x, targetPos.y, targetWidth, targetHeight);
  return <BaseEdge id={id} path={`M ${start.x} ${start.y} L ${end.x} ${end.y}`} style={style} markerEnd={markerEnd} />;
}

export function edgeStroke(edge) {
  if (edge.kind === "management") return "var(--edge-mgmt)";
  if (edge.kind === "message") return "var(--edge-message)";
  if (edge.kind === "mailbox") return "var(--edge-mailbox)";
  if (edge.kind === "producer") return "var(--edge-producer)";
  if (edge.source === "bootstrap") return "var(--edge-bootstrap)";
  if (edge.source === "seeded") return "var(--edge-seeded)";
  if (edge.source === "discovered") return "var(--edge-discovered)";
  return "var(--edge-routing)";
}

export function edgeDash(edge) {
  if (edge.kind === "management") return "2 6";
  if (edge.kind === "mailbox") return "7 4";
  if (edge.kind === "producer") return "4 5";
  if (edge.kind === "routing" || edge.kind === "subscription") {
    if (edge.source === "seeded") return "8 5";
    if (edge.source === "discovered") return "2 6";
    return "";
  }
  if (edge.status === "proposed") return "4 4";
  if (edge.status === "deactivated") return "2 6";
  return "";
}

export function getEdgeType(forceLayout, edge) {
  if (forceLayout) return "straightClipped";
  return edge.kind === "management" ? "smoothstep" : "default";
}

export function buildEdgePresentation(edge, edgeID, activeEdgeKeys, hoverNodeID, selectedEdgeID, forceLayout, allowAnimation = true) {
  const stroke = edgeStroke(edge);
  const dash = edgeDash(edge);
  const edgeKey = `${edge.from}->${edge.to}|${edge.label || ""}`;
  const hoverActive = !!hoverNodeID && (edge.from === hoverNodeID || edge.to === hoverNodeID);
  const isSelected = selectedEdgeID === edgeID;
  const isActive = !!(activeEdgeKeys && activeEdgeKeys.has(edgeKey)) || hoverActive || isSelected;
  const isDimmed = !!edge.dimmed;
  return {
    id: edgeID,
    source: edge.from,
    target: edge.to,
    type: getEdgeType(forceLayout, edge),
    animated: allowAnimation && isActive,
    data: edge,
    selected: isSelected,
    className: `${isDimmed ? "rf-edge-dimmed" : ""} ${isActive ? "rf-edge-active" : ""}`.trim(),
    style: {
      stroke,
      strokeWidth: isSelected ? 3.6 : (isActive ? 2.8 : 1.8),
      strokeDasharray: dash || undefined,
      opacity: isDimmed ? 0.18 : (isActive ? 0.98 : 0.72),
    },
    markerEnd: { type: MarkerType.ArrowClosed, color: stroke },
  };
}
