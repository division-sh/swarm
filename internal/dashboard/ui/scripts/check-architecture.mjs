import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const failures = [];

function read(relPath) {
  return fs.readFileSync(path.join(root, relPath), "utf8");
}

function lineCount(relPath) {
  return read(relPath).split("\n").length;
}

function expect(condition, message) {
  if (!condition) failures.push(message);
}

function expectMissing(relPath, message) {
  if (fs.existsSync(path.join(root, relPath))) failures.push(message);
}

const mainPath = "src/main.jsx";
const routerPath = "src/app/DashboardViewRouter.jsx";
const runtimeRouterPath = "src/app/DashboardRuntimeViews.jsx";
const opsRouterPath = "src/app/DashboardOpsViews.jsx";
const derivedPath = "src/app/useDashboardDerivedState.js";
const architectureDoc = "ARCHITECTURE.md";

expect(lineCount(mainPath) <= 20, `${mainPath} must stay bootstrap-only`);
expect(lineCount(derivedPath) <= 80, `${derivedPath} must stay thin and global-only`);
expect(!read(routerPath).includes("{ app }"), `${routerPath} must not accept an app bag`);
expect(!read(runtimeRouterPath).includes("{ app }"), `${runtimeRouterPath} must not accept an app bag`);
expect(!read(opsRouterPath).includes("{ app }"), `${opsRouterPath} must not accept an app bag`);
expect(read(architectureDoc).includes("No `app` bag props."), `${architectureDoc} must document the no-app-bag rule`);
expectMissing("src/hooks/useDashboardActions.js", "src/hooks/useDashboardActions.js must not exist");
expectMissing("src/app/useDashboardDataSources.js", "src/app/useDashboardDataSources.js must not exist");

if (failures.length > 0) {
  console.error("Architecture check failed:");
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exit(1);
}

console.log("Architecture check passed.");
