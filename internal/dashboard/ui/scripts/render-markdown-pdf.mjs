#!/usr/bin/env node

import { readFile, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

function usage() {
  console.error("Usage: node internal/dashboard/ui/scripts/render-markdown-pdf.mjs <input.md> [output.pdf]");
  process.exit(1);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function slugify(value, seen) {
  const base = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "section";
  const count = seen.get(base) ?? 0;
  seen.set(base, count + 1);
  return count === 0 ? base : `${base}-${count + 1}`;
}

function prettifyStem(stem) {
  return stem
    .replace(/[-_]+/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function formatInline(text) {
  const escaped = escapeHtml(text);
  return escaped.replace(
    /(`[^`]+`|\*\*[^*]+\*\*|\*[^*]+\*|_[^_]+_|\[([^\]]+)\]\(([^)]+)\))/g,
    (token, _full, label, href) => {
      if (token.startsWith("`")) {
        return `<code class="md-inline-code">${escapeHtml(token.slice(1, -1))}</code>`;
      }
      if (token.startsWith("**")) {
        return `<strong>${escapeHtml(token.slice(2, -2))}</strong>`;
      }
      if (token.startsWith("*") || token.startsWith("_")) {
        return `<em>${escapeHtml(token.slice(1, -1))}</em>`;
      }
      if (label && href) {
        return `<a class="md-link" href="${escapeHtml(href)}">${escapeHtml(label)}</a>`;
      }
      return token;
    },
  );
}

function renderMarkdown(markdown) {
  const lines = markdown.split(/\r?\n/);
  const out = [];
  const toc = [];
  const seenSlugs = new Map();
  let inCode = false;
  let codeLang = "";
  let codeBuf = [];

  const flushCode = () => {
    if (!inCode) return;
    const code = escapeHtml(codeBuf.join("\n"));
    out.push(
      `<pre class="md-code-block" data-lang="${escapeHtml(codeLang)}"><code>${code}</code></pre>`,
    );
    inCode = false;
    codeLang = "";
    codeBuf = [];
  };

  for (const line of lines) {
    if (line.startsWith("```")) {
      if (inCode) {
        flushCode();
      } else {
        inCode = true;
        codeLang = line.slice(3).trim().toLowerCase();
      }
      continue;
    }

    if (inCode) {
      codeBuf.push(line);
      continue;
    }

    const heading = line.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      const level = Math.min(heading[1].length, 4);
      const text = heading[2].trim();
      const id = slugify(text, seenSlugs);
      toc.push({ level, text, id });
      out.push(
        `<div id="${id}" class="md-h${level}">${formatInline(text)}</div>`,
      );
      continue;
    }

    const bullet = line.match(/^(\s*)[-*]\s+(.*)$/);
    if (bullet) {
      const indent = Math.floor(bullet[1].length / 2);
      out.push(
        `<div class="md-li" style="--indent:${indent}">${formatInline(bullet[2])}</div>`,
      );
      continue;
    }

    const numbered = line.match(/^(\s*)(\d+\.)\s+(.*)$/);
    if (numbered) {
      const indent = Math.floor(numbered[1].length / 2);
      out.push(
        `<div class="md-li md-li-num" style="--indent:${indent}"><span class="md-li-marker">${escapeHtml(numbered[2])}</span>${formatInline(numbered[3])}</div>`,
      );
      continue;
    }

    if (line.trim() === "") {
      out.push(`<div class="md-blank"></div>`);
      continue;
    }

    out.push(`<div class="md-p">${formatInline(line)}</div>`);
  }

  flushCode();

  return { bodyHtml: out.join("\n"), toc };
}

function buildToc(toc) {
  const items = toc
    .filter((entry) => entry.level <= 3)
    .map(
      (entry) =>
        `<a class="toc-item toc-level-${entry.level}" href="#${entry.id}">${escapeHtml(entry.text)}</a>`,
    )
    .join("\n");
  return items || `<div class="toc-empty">No headings found.</div>`;
}

function buildHtml({ title, sourceLabel, generatedAt, bodyHtml, tocHtml }) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
    <style>
      :root {
        --paper: #ffffff;
        --ink: #122033;
        --muted: #5d6b7c;
        --line: #d7dde5;
        --line-strong: #c2cad6;
        --accent: #0f4c81;
        --accent-soft: #dce9f7;
        --accent-warm: #b26836;
        --bg: #eef2f7;
        --code-bg: #f6f8fb;
        --code-ink: #24364c;
        --shadow: 0 18px 60px rgba(17, 24, 39, 0.10);
      }

      @page {
        size: Letter;
        margin: 16mm 14mm 18mm 14mm;
      }

      * { box-sizing: border-box; }

      html, body {
        margin: 0;
        padding: 0;
        background: var(--bg);
        color: var(--ink);
      }

      body {
        font-family: "Charter", "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
        line-height: 1.55;
        -webkit-font-smoothing: antialiased;
      }

      .sheet {
        max-width: 920px;
        margin: 0 auto;
        padding: 18px;
      }

      .document {
        background: var(--paper);
        border: 1px solid rgba(18, 32, 51, 0.08);
        box-shadow: var(--shadow);
        overflow: hidden;
      }

      .cover {
        padding: 44px 52px 36px;
        background:
          radial-gradient(circle at top right, rgba(178, 104, 54, 0.15), transparent 30%),
          linear-gradient(145deg, #17375c 0%, #0f4c81 48%, #12335a 100%);
        color: #f9fbff;
        page-break-after: always;
      }

      .eyebrow {
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        opacity: 0.78;
        margin-bottom: 22px;
      }

      .title {
        font-size: 36px;
        line-height: 1.1;
        margin: 0 0 14px;
        max-width: 12ch;
      }

      .subtitle {
        font-size: 15px;
        line-height: 1.7;
        color: rgba(249, 251, 255, 0.84);
        max-width: 58ch;
        margin: 0 0 28px;
      }

      .meta-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 14px 20px;
        max-width: 640px;
      }

      .meta-card {
        padding: 14px 16px;
        border: 1px solid rgba(255, 255, 255, 0.18);
        background: rgba(255, 255, 255, 0.08);
        backdrop-filter: blur(4px);
      }

      .meta-label {
        display: block;
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        font-size: 10px;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: rgba(249, 251, 255, 0.72);
        margin-bottom: 8px;
      }

      .meta-value {
        font-size: 13px;
        line-height: 1.5;
        color: #fff;
        overflow-wrap: anywhere;
      }

      .toc {
        padding: 34px 52px 26px;
        border-bottom: 1px solid var(--line);
        page-break-after: always;
      }

      .section-kicker {
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        color: var(--accent-warm);
        margin-bottom: 10px;
      }

      .section-title {
        font-size: 26px;
        line-height: 1.15;
        margin: 0 0 18px;
      }

      .toc-grid {
        display: grid;
        gap: 6px;
      }

      .toc-item {
        color: var(--ink);
        text-decoration: none;
        padding: 5px 0;
        border-bottom: 1px solid rgba(18, 32, 51, 0.06);
        font-size: 13px;
      }

      .toc-level-1 {
        font-weight: 700;
        color: var(--accent);
      }

      .toc-level-2 { padding-left: 16px; }
      .toc-level-3 {
        padding-left: 32px;
        color: var(--muted);
      }

      .content {
        padding: 34px 52px 46px;
      }

      .md-body {
        font-size: 12.5px;
        line-height: 1.72;
        color: var(--ink);
      }

      .md-h1, .md-h2, .md-h3, .md-h4 {
        break-after: avoid-page;
        page-break-after: avoid;
      }

      .md-h1 {
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        font-size: 24px;
        font-weight: 800;
        letter-spacing: 0.01em;
        color: var(--accent);
        margin: 36px 0 14px;
        padding-top: 8px;
        border-top: 2px solid var(--accent-soft);
      }

      .md-h1:first-child {
        margin-top: 0;
        padding-top: 0;
        border-top: 0;
      }

      .md-h2 {
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        font-size: 17px;
        font-weight: 700;
        margin: 24px 0 10px;
        color: #16324f;
        padding-bottom: 4px;
        border-bottom: 1px solid var(--line-strong);
      }

      .md-h3 {
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        font-size: 12px;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--accent-warm);
        margin: 18px 0 8px;
      }

      .md-h4 {
        font-size: 13px;
        font-weight: 700;
        margin: 14px 0 6px;
        color: var(--muted);
      }

      .md-p {
        margin: 4px 0;
        text-wrap: pretty;
      }

      .md-li {
        position: relative;
        margin: 4px 0;
        padding-left: calc(18px + (var(--indent, 0) * 18px));
        text-wrap: pretty;
      }

      .md-li::before {
        content: "\\2022";
        position: absolute;
        left: calc((var(--indent, 0) * 18px) + 4px);
        color: var(--accent-warm);
        font-weight: 700;
      }

      .md-li-num::before { content: none; }

      .md-li-marker {
        display: inline-block;
        min-width: 2.6em;
        margin-left: calc(-1 * (2.6em - 2px));
        color: var(--accent);
        font-weight: 700;
      }

      .md-blank { height: 8px; }

      .md-code-block {
        margin: 10px 0 12px;
        padding: 11px 13px;
        background: var(--code-bg);
        border: 1px solid var(--line);
        border-left: 4px solid rgba(15, 76, 129, 0.35);
        color: var(--code-ink);
        font-family: "JetBrains Mono", "SFMono-Regular", Menlo, Consolas, monospace;
        font-size: 10.5px;
        line-height: 1.6;
        white-space: pre-wrap;
        overflow-wrap: anywhere;
      }

      .md-inline-code {
        font-family: "JetBrains Mono", "SFMono-Regular", Menlo, Consolas, monospace;
        font-size: 0.92em;
        background: rgba(15, 76, 129, 0.08);
        border: 1px solid rgba(15, 76, 129, 0.12);
        border-radius: 4px;
        padding: 0.08em 0.38em;
        color: #103d66;
      }

      .md-link {
        color: var(--accent);
        text-decoration: underline;
        text-decoration-color: rgba(15, 76, 129, 0.28);
      }

      strong { color: #0f2740; }

      @media print {
        html, body { background: #fff; }
        .sheet {
          max-width: none;
          padding: 0;
        }
        .document {
          border: 0;
          box-shadow: none;
        }
      }
    </style>
  </head>
  <body>
    <div class="sheet">
      <article class="document">
        <section class="cover">
          <div class="eyebrow">Reference Export</div>
          <h1 class="title">${escapeHtml(title)}</h1>
          <p class="subtitle">Print-optimized rendering of the Markdown reference document for review, annotation, and offline reading.</p>
          <div class="meta-grid">
            <div class="meta-card">
              <span class="meta-label">Source</span>
              <span class="meta-value">${escapeHtml(sourceLabel)}</span>
            </div>
            <div class="meta-card">
              <span class="meta-label">Generated</span>
              <span class="meta-value">${escapeHtml(generatedAt)}</span>
            </div>
          </div>
        </section>
        <section class="toc">
          <div class="section-kicker">Navigation</div>
          <h2 class="section-title">Table of Contents</h2>
          <div class="toc-grid">${tocHtml}</div>
        </section>
        <section class="content">
          <div class="md-body">${bodyHtml}</div>
        </section>
      </article>
    </div>
  </body>
</html>`;
}

async function main() {
  const [, , inputArg, outputArg] = process.argv;
  if (!inputArg) usage();

  const repoRoot = path.resolve(fileURLToPath(new URL("../../../../", import.meta.url)));
  const inputPath = path.resolve(process.cwd(), inputArg);
  const outputPath = outputArg
    ? path.resolve(process.cwd(), outputArg)
    : inputPath.replace(/\.md$/i, ".pdf");

  const markdown = await readFile(inputPath, "utf8");
  const { bodyHtml, toc } = renderMarkdown(markdown);
  const relativeSource = path.relative(repoRoot, inputPath) || path.basename(inputPath);
  const title = prettifyStem(path.basename(inputPath, path.extname(inputPath)));
  const generatedAt = new Intl.DateTimeFormat("en-US", {
    dateStyle: "long",
    timeStyle: "short",
  }).format(new Date());
  const html = buildHtml({
    title,
    sourceLabel: relativeSource,
    generatedAt,
    bodyHtml,
    tocHtml: buildToc(toc),
  });

  await mkdir(path.dirname(outputPath), { recursive: true });

  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "load" });
    await page.pdf({
      path: outputPath,
      format: "Letter",
      printBackground: true,
      displayHeaderFooter: true,
      headerTemplate: `<div></div>`,
      footerTemplate: `
        <div style="width:100%; padding:0 10mm; font-size:8px; color:#5d6b7c; font-family:'Avenir Next','Segoe UI',sans-serif; display:flex; align-items:center; justify-content:space-between;">
          <span>${escapeHtml(relativeSource)}</span>
          <span><span class="pageNumber"></span> / <span class="totalPages"></span></span>
        </div>
      `,
      margin: {
        top: "18mm",
        right: "14mm",
        bottom: "18mm",
        left: "14mm",
      },
    });
  } finally {
    await browser.close();
  }

  console.log(outputPath);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
