import React from "react";
import { createRoot } from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";
import AppShell from "./app/AppShell.tsx";
import { dashboardQueryClient } from "./app/queryClient.ts";

createRoot(document.getElementById("root")).render(
  <QueryClientProvider client={dashboardQueryClient}>
    <AppShell />
  </QueryClientProvider>,
);
