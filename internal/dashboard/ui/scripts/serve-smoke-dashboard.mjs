import http from "node:http";
import { createReadStream, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const root = path.resolve(__dirname, "..");
const distDir = path.join(root, "dist");
const htmlPath = path.resolve(root, "..", "assets", "dashboard.html");
const monacoRoot = path.join(root, "node_modules", "monaco-editor", "min");
const port = Number(process.env.PLAYWRIGHT_DASHBOARD_PORT || 4173);
const apiOrigin = process.env.DASHBOARD_API_ORIGIN || "http://127.0.0.1:8081";

const contentTypes = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ttf": "font/ttf",
};

function sendFile(res, filePath) {
  if (!existsSync(filePath)) {
    res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    res.end("Not found");
    return;
  }
  const ext = path.extname(filePath).toLowerCase();
  res.writeHead(200, { "content-type": contentTypes[ext] || "application/octet-stream" });
  createReadStream(filePath).pipe(res);
}

function resolveMonacoAsset(requestPath) {
  const relative = requestPath
    .replace(/^\/dashboard\/assets/, "")
    .replace(/^\/vs/, "/vs");
  return path.join(monacoRoot, relative);
}

function proxyRequest(req, res, requestUrl) {
  const upstreamUrl = new URL(requestUrl.pathname + requestUrl.search, apiOrigin);
  const proxy = http.request(upstreamUrl, {
    method: req.method,
    headers: {
      ...req.headers,
      host: upstreamUrl.host,
    },
  }, (upstream) => {
    const headers = { ...upstream.headers };
    delete headers["content-length"];
    res.writeHead(upstream.statusCode || 502, headers);
    upstream.pipe(res);
  });

  proxy.on("error", (err) => {
    res.writeHead(502, { "content-type": "text/plain; charset=utf-8" });
    res.end(`Proxy error: ${err.message}`);
  });

  req.pipe(proxy);
}

const server = http.createServer((req, res) => {
  const requestUrl = new URL(req.url || "/", `http://127.0.0.1:${port}`);
  const pathname = requestUrl.pathname;

  if (pathname === "/" || pathname === "/dashboard" || pathname === "/dashboard/") {
    sendFile(res, htmlPath);
    return;
  }
  if (pathname === "/dashboard/assets/dashboard.app.js") {
    sendFile(res, path.join(distDir, "dashboard.app.js"));
    return;
  }
  if (pathname === "/dashboard/assets/dashboard.css") {
    sendFile(res, path.join(distDir, "dashboard.css"));
    return;
  }
  if (pathname.startsWith("/vs/") || pathname.startsWith("/dashboard/assets/vs/")) {
    sendFile(res, resolveMonacoAsset(pathname));
    return;
  }
  if (pathname.startsWith("/api/") || pathname.startsWith("/dashboard/api/") || pathname === "/healthz") {
    proxyRequest(req, res, requestUrl);
    return;
  }

  res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  res.end(`Unhandled path: ${pathname}`);
});

server.listen(port, "127.0.0.1", () => {
  process.stdout.write(`dashboard smoke server listening on ${port}\n`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    server.close(() => process.exit(0));
  });
}
