import { useEffect, useState } from "react";

type DockviewPanelApiLike = {
  isVisible?: boolean;
  isActive?: boolean;
  onDidVisibilityChange?: (listener: (event: { isVisible: boolean }) => void) => { dispose?: () => void };
  onDidActiveChange?: (listener: (event: { isActive: boolean }) => void) => { dispose?: () => void };
};

export default function useDockviewPanelVisibility(api?: DockviewPanelApiLike | null) {
  const initialVisible = Boolean(api?.isVisible || api?.isActive);
  const [isVisible, setIsVisible] = useState(initialVisible);
  const [hasRendered, setHasRendered] = useState(initialVisible);

  useEffect(() => {
    const nextVisible = Boolean(api?.isVisible || api?.isActive);
    setIsVisible(nextVisible);
    if (nextVisible) {
      setHasRendered(true);
    }

    if (!api) return;

    const visibilityDisposable = api.onDidVisibilityChange?.((event) => {
      const visible = Boolean(event.isVisible || api.isActive);
      setIsVisible(visible);
      if (visible) {
        setHasRendered(true);
      }
    });

    const activeDisposable = api.onDidActiveChange?.((event) => {
      const visible = Boolean(event.isActive || api.isVisible);
      setIsVisible(visible);
      if (visible) {
        setHasRendered(true);
      }
    });

    return () => {
      visibilityDisposable?.dispose?.();
      activeDisposable?.dispose?.();
    };
  }, [api]);

  return { isVisible, hasRendered };
}
