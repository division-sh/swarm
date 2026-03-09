import { useMemo } from "react";

type HoldingControllerInput = {
  domain: Record<string, any>;
  controls: Record<string, any>;
  openHoldingVerticalDetail: (verticalID: string) => Promise<void> | void;
};

export function useHoldingController({
  domain,
  controls,
  openHoldingVerticalDetail,
}: HoldingControllerInput) {
  return useMemo(() => ({
    state: { ...domain, ...controls },
    actions: { ...controls, openHoldingVerticalDetail },
  }), [controls, domain, openHoldingVerticalDetail]);
}
