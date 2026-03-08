import { useMemo } from "react";

export function useHoldingController({
  domain,
  controls,
  openHoldingVerticalDetail,
}) {
  return useMemo(() => ({
    state: { ...domain, ...controls },
    actions: { openHoldingVerticalDetail },
  }), [controls, domain, openHoldingVerticalDetail]);
}
