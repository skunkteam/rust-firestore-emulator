// @ts-check

import { html, nothing } from "../lit-html.js";

/** @typedef {import('../lit-html.js').TemplateResult} TemplateResult */

/**
 * @typedef {{
 *   top: number,
 *   bottom: number,
 *   left: number,
 *   width: number,
 *   flipUp: boolean,
 *   popupLeft: number,
 *   popupWidth: number,
 *   popupBottom: number,
 *   maxListboxHeight: number,
 * }} ComboboxRect
 */

/**
 * @typedef {{
 *   openId: string | null,
 *   activeIndex: number,
 *   rect: ComboboxRect | null,
 *   cleanup: (() => void) | null,
 * }} ComboboxUiState
 */

/**
 * @typedef {{
 *   value: string,
 *   label: string,
 *   matchText?: string,
 *   badge?: string,
 *   badgeClass?: string,
 *   meta?: string,
 * }} ComboboxItem
 */

/**
 * @typedef {{
 *   top: number,
 *   bottom: number,
 *   left: number,
 *   width: number,
 *   right?: number,
 * }} RectLike
 */

/**
 * @typedef {{
 *   getBoundingClientRect?: () => RectLike,
 *   isConnected?: boolean,
 *   value?: string,
 *   scrollIntoView?: (opts?: { block?: string }) => void,
 *   contains?: (other: unknown) => boolean,
 *   focus?: () => void,
 * }} ElementLike
 */

const POPUP_FLIP_THRESHOLD_PX = 280;
const POPUP_MIN_WIDTH_PX = 220;
const DEFAULT_VIEWPORT_HEIGHT_PX = 800;
const DEFAULT_VIEWPORT_WIDTH_PX = 1280;

/** @type {string | null} */
let suppressFocusOpenFor = null;

/** @type {WeakMap<ComboboxUiState, () => void>} */
const popupPointerCleanups = new WeakMap();

/**
 * Looks up an element by ID safely across browser DOM and headless Node test stubs.
 * @param {string} id
 * @returns {ElementLike | null}
 */
function getElementByIdSafe(id) {
  if (typeof document === "undefined" || typeof document?.getElementById !== "function") {
    return null;
  }
  return /** @type {ElementLike | null} */ (
    /** @type {unknown} */ (document.getElementById(id))
  );
}

/**
 * Returns true if `target` is `el` itself or a descendant contained by `el`.
 * @param {ElementLike | null} el
 * @param {unknown} target
 * @returns {boolean}
 */
function isTargetInside(el, target) {
  return Boolean(el && target != null && (el === target || el.contains?.(target)));
}

/**
 * Returns true while a pointer-down gesture that began inside the combobox popup is active.
 * @param {ComboboxUiState} uiState
 * @returns {boolean}
 */
function isPopupPointerDown(uiState) {
  return popupPointerCleanups.has(uiState);
}

/**
 * Clears active popup pointer state and removes any temporary window release listeners.
 * @param {ComboboxUiState} uiState
 */
function clearPopupPointerState(uiState) {
  const cleanup = popupPointerCleanups.get(uiState);
  if (!cleanup) {
    return;
  }
  popupPointerCleanups.delete(uiState);
  cleanup();
}

/**
 * Focuses the combobox `<input>` element, optionally suppressing synchronous focus-open.
 * @param {string} id
 * @param {boolean} suppressOpen
 */
function focusComboboxInput(id, suppressOpen) {
  const inputEl = getElementByIdSafe(id);
  if (
    !inputEl ||
    inputEl.isConnected === false ||
    typeof inputEl.focus !== "function"
  ) {
    return;
  }
  if (suppressOpen) {
    suppressFocusOpenFor = id;
    try {
      inputEl.focus();
    } finally {
      suppressFocusOpenFor = null;
    }
  } else {
    inputEl.focus();
  }
}

/**
 * Tracks a pointer-down inside `#<id>-popup` so native scrollbar dragging and popup chrome clicks
 * do not collapse the popup on blur (including Safari `relatedTarget === null`), and restores focus
 * back to `<input>` on release with deterministic listener cleanup.
 * @param {string} id
 * @param {string} popupId
 * @param {ComboboxUiState} uiState
 * @param {() => void} onRerender
 */
function beginPopupPointerInteraction(id, popupId, uiState, onRerender) {
  clearPopupPointerState(uiState);
  if (uiState.openId !== id) {
    return;
  }
  if (typeof window === "undefined" || typeof window?.addEventListener !== "function") {
    popupPointerCleanups.set(uiState, () => {});
    return;
  }

  const dismissIfOpen = () => {
    clearPopupPointerState(uiState);
    if (uiState.openId === id) {
      closeActiveCombobox(uiState);
      onRerender();
    }
  };

  const onRelease = () => {
    clearPopupPointerState(uiState);
    if (uiState.openId !== id) {
      return;
    }
    const doc = typeof document !== "undefined" ? document : null;
    const active = doc ? /** @type {unknown} */ (doc.activeElement ?? null) : null;
    const inputEl = getElementByIdSafe(id);
    const popupEl = getElementByIdSafe(popupId);
    const focusMovedOutside =
      active != null &&
      active !== doc?.body &&
      active !== doc?.documentElement &&
      active !== inputEl &&
      !isTargetInside(popupEl, active);
    if (focusMovedOutside) {
      dismissIfOpen();
      return;
    }
    if (active !== inputEl) {
      focusComboboxInput(id, false);
    }
  };

  const onMouseMove = (/** @type {MouseEvent} */ e) => {
    if (typeof e?.buttons === "number" && e.buttons === 0) {
      onRelease();
    }
  };

  const onOutsideMouseDown = (/** @type {Event} */ e) => {
    const target = /** @type {unknown} */ (e?.target ?? null);
    if (isTargetInside(getElementByIdSafe(popupId), target)) {
      return;
    }
    const inputEl = getElementByIdSafe(id);
    if (inputEl && target === inputEl) {
      clearPopupPointerState(uiState);
      return;
    }
    dismissIfOpen();
  };

  window.addEventListener("mouseup", onRelease, { capture: true });
  window.addEventListener("pointerup", onRelease, { capture: true });
  window.addEventListener("mousemove", onMouseMove, {
    capture: true,
    passive: true,
  });
  window.addEventListener("blur", dismissIfOpen);
  window.addEventListener("mousedown", onOutsideMouseDown, { capture: true });

  popupPointerCleanups.set(uiState, () => {
    if (
      typeof window !== "undefined" &&
      typeof window?.removeEventListener === "function"
    ) {
      window.removeEventListener("mouseup", onRelease, { capture: true });
      window.removeEventListener("pointerup", onRelease, { capture: true });
      window.removeEventListener("mousemove", onMouseMove, { capture: true });
      window.removeEventListener("blur", dismissIfOpen);
      window.removeEventListener("mousedown", onOutsideMouseDown, {
        capture: true,
      });
    }
  });
}

/**
 * Creates initial combobox popup UI state.
 * @returns {ComboboxUiState}
 */
export function createComboboxUiState() {
  return {
    openId: null,
    activeIndex: -1,
    rect: null,
    cleanup: null,
  };
}

/**
 * Closes any active combobox popup and runs registered window listener cleanup.
 * @param {ComboboxUiState} uiState
 */
export function closeActiveCombobox(uiState) {
  clearPopupPointerState(uiState);
  const cleanup = uiState.cleanup;
  uiState.cleanup = null;
  uiState.openId = null;
  uiState.activeIndex = -1;
  uiState.rect = null;
  cleanup?.();
}

/**
 * Closes an active combobox when its control ID is no longer rendered in `activeIds`.
 * @param {ComboboxUiState} uiState
 * @param {Set<string>} activeIds
 */
export function reconcileComboboxUi(uiState, activeIds) {
  if (uiState.openId !== null && !activeIds.has(uiState.openId)) {
    closeActiveCombobox(uiState);
  }
}

/**
 * Returns the viewport height if available, or a safe fallback for headless tests.
 * @returns {number}
 */
function getViewportHeight() {
  return typeof window !== "undefined" && typeof window?.innerHeight === "number"
    ? window.innerHeight
    : DEFAULT_VIEWPORT_HEIGHT_PX;
}

/**
 * Returns the viewport width if available, or a safe fallback for headless tests.
 * @returns {number}
 */
function getViewportWidth() {
  return typeof window !== "undefined" && typeof window?.innerWidth === "number"
    ? window.innerWidth
    : DEFAULT_VIEWPORT_WIDTH_PX;
}

/**
 * Computes popup geometry, clamped viewport bounds, and flip direction from an anchor bounding box.
 * @param {RectLike} rect
 * @returns {ComboboxRect}
 */
function computeComboboxRect(rect) {
  const viewportHeight = getViewportHeight();
  const viewportWidth = getViewportWidth();
  const availableBelow = viewportHeight - rect.bottom;
  const availableAbove = rect.top;
  const flipUp =
    availableBelow < POPUP_FLIP_THRESHOLD_PX && availableAbove > availableBelow;
  const availableSpace = flipUp ? availableAbove : availableBelow;
  const popupWidth = Math.min(
    Math.max(rect.width, POPUP_MIN_WIDTH_PX),
    Math.max(120, viewportWidth - 16)
  );
  const popupLeft = Math.min(
    Math.max(8, rect.left),
    Math.max(8, viewportWidth - popupWidth - 8)
  );
  const popupBottom = viewportHeight - rect.top + 2;
  const maxListboxHeight = Math.max(
    80,
    Math.min(260, Math.floor(availableSpace - 44))
  );
  return {
    top: rect.top,
    bottom: rect.bottom,
    left: rect.left,
    width: rect.width,
    flipUp,
    popupLeft,
    popupWidth,
    popupBottom,
    maxListboxHeight,
  };
}

/**
 * Returns true if two `ComboboxRect` snapshots have identical geometry.
 * @param {ComboboxRect | null} a
 * @param {ComboboxRect | null} b
 * @returns {boolean}
 */
function comboboxRectsEqual(a, b) {
  if (a === b) return true;
  if (!a || !b) return false;
  return (
    a.top === b.top &&
    a.bottom === b.bottom &&
    a.left === b.left &&
    a.width === b.width &&
    a.flipUp === b.flipUp &&
    a.popupLeft === b.popupLeft &&
    a.popupWidth === b.popupWidth &&
    a.popupBottom === b.popupBottom &&
    a.maxListboxHeight === b.maxListboxHeight
  );
}

/**
 * Measures `el` if `getBoundingClientRect` is supported and returns computed popup rect.
 * @param {ElementLike | null} el
 * @returns {ComboboxRect | null}
 */
function measureAnchorRect(el) {
  if (typeof el?.getBoundingClientRect !== "function") {
    return null;
  }
  return computeComboboxRect(el.getBoundingClientRect());
}

/**
 * Returns true if a scroll event originated inside `.combobox-listbox`.
 * @param {Event | null | undefined} e
 * @returns {boolean}
 */
function isEventInsideListbox(e) {
  const rawTarget = /** @type {unknown} */ (e?.target ?? null);
  if (!rawTarget || typeof rawTarget !== "object") {
    return false;
  }
  const target = /**
   * @type {{
   *   classList?: { contains?: (cls: string) => boolean },
   *   className?: unknown,
   *   closest?: (selector: string) => unknown,
   * }}
   */ (rawTarget);
  if (typeof target.classList?.contains === "function" && target.classList.contains("combobox-listbox")) {
    return true;
  }
  if (
    typeof target.className === "string" &&
    target.className.split(/\s+/).includes("combobox-listbox")
  ) {
    return true;
  }
  if (typeof target.closest === "function" && target.closest(".combobox-listbox") !== null) {
    return true;
  }
  return false;
}

/**
 * Looks up an element by CSS selector, falling back to `getElementById` for simple `#id` selectors.
 * @param {string | undefined} selector
 * @returns {ElementLike | null}
 */
function findElementBySelector(selector) {
  if (!selector || typeof document === "undefined") {
    return null;
  }
  if (typeof document?.querySelector === "function") {
    const found = /** @type {ElementLike | null} */ (
      /** @type {unknown} */ (document.querySelector(selector))
    );
    if (found) {
      return found;
    }
  }
  if (/^#[^\s.[\]:>+~]+$/.test(selector)) {
    return getElementByIdSafe(selector.slice(1));
  }
  return null;
}

/**
 * Returns true if `rect` is outside the viewport or clipped by optional obstruction/container bounds.
 * @param {RectLike} rect
 * @param {{ topObstructionSelector?: string, clipContainerSelector?: string }} [opts]
 * @returns {boolean}
 */
function isAnchorClipped(rect, opts = {}) {
  if (typeof window !== "undefined" && typeof window?.innerHeight === "number") {
    if (rect.bottom <= 0 || rect.top >= window.innerHeight) {
      return true;
    }
    if (typeof window?.innerWidth === "number") {
      const right = typeof rect.right === "number" ? rect.right : rect.left + rect.width;
      if (right <= 0 || rect.left >= window.innerWidth) {
        return true;
      }
    }
  }

  const topEl = findElementBySelector(opts.topObstructionSelector);
  if (typeof topEl?.getBoundingClientRect === "function") {
    const topRect = topEl.getBoundingClientRect();
    if (rect.top < topRect.bottom || rect.bottom <= topRect.bottom) {
      return true;
    }
  }

  const pane = findElementBySelector(opts.clipContainerSelector);
  if (typeof pane?.getBoundingClientRect === "function") {
    const paneRect = pane.getBoundingClientRect();
    if (rect.bottom <= paneRect.top || rect.top >= paneRect.bottom) {
      return true;
    }
  }

  return false;
}

/**
 * Scrolls the active listbox option into view when running in a browser DOM.
 * @param {string} listboxId
 * @param {number} index
 */
function scrollOptionIntoView(listboxId, index) {
  if (index < 0 || typeof requestAnimationFrame !== "function") {
    return;
  }
  requestAnimationFrame(() => {
    const optEl = getElementByIdSafe(`${listboxId}-opt-${index}`);
    if (typeof optEl?.scrollIntoView === "function") {
      optEl.scrollIntoView({ block: "nearest" });
    }
  });
}

/**
 * Registers capture-phase `scroll` and `resize` listeners to keep an open popup aligned or dismiss
 * it when detached or scrolled out of visible bounds.
 * @param {string} id
 * @param {ComboboxUiState} uiState
 * @param {() => void} onRerender
 * @param {{ topObstructionSelector?: string, clipContainerSelector?: string }} [clipOpts]
 */
function registerViewportListeners(id, uiState, onRerender, clipOpts = {}) {
  if (typeof window === "undefined" || typeof window?.addEventListener !== "function") {
    return;
  }

  const syncOrDismiss = () => {
    if (uiState.openId !== id) {
      return;
    }
    const anchor = getElementByIdSafe(id);
    if (
      !anchor ||
      anchor.isConnected === false ||
      typeof anchor.getBoundingClientRect !== "function"
    ) {
      closeActiveCombobox(uiState);
      onRerender();
      return;
    }

    const rect = anchor.getBoundingClientRect();
    if (isAnchorClipped(rect, clipOpts)) {
      closeActiveCombobox(uiState);
      onRerender();
      return;
    }

    const nextRect = computeComboboxRect(rect);
    if (comboboxRectsEqual(uiState.rect, nextRect)) {
      return;
    }
    uiState.rect = nextRect;
    onRerender();
  };

  const onScroll = (/** @type {Event} */ e) => {
    if (isEventInsideListbox(e)) {
      return;
    }
    syncOrDismiss();
  };

  const onResize = () => {
    syncOrDismiss();
  };

  window.addEventListener("scroll", onScroll, { capture: true, passive: true });
  window.addEventListener("resize", onResize, { passive: true });
  uiState.cleanup = () => {
    clearPopupPointerState(uiState);
    if (typeof window !== "undefined" && typeof window?.removeEventListener === "function") {
      window.removeEventListener("scroll", onScroll, { capture: true });
      window.removeEventListener("resize", onResize);
    }
  };
}

/**
 * Filters combobox items using case-insensitive substring matching across `value`, `label`, and `matchText`.
 * @param {ComboboxItem[]} items
 * @param {string} rawValue
 * @returns {ComboboxItem[]}
 */
function filterComboboxItems(items, rawValue) {
  const needle = rawValue.toLowerCase();
  if (!needle) {
    return items;
  }
  return items.filter((item) => {
    if (item.value.toLowerCase().includes(needle)) return true;
    if (item.label.toLowerCase().includes(needle)) return true;
    if (item.matchText && item.matchText.toLowerCase().includes(needle)) return true;
    return false;
  });
}

/**
 * Renders a pure `lit-html` WAI-ARIA 1.2 combobox with viewport-fixed positioning and keyboard support.
 *
 * @param {{
 *   id: string,
 *   value: string,
 *   placeholder: string,
 *   ariaLabel: string,
 *   items: ComboboxItem[],
 *   uiState: ComboboxUiState,
 *   footerText: string,
 *   loading: boolean,
 *   disabled?: boolean,
 *   inputClass?: string,
 *   inputStyle?: string,
 *   keepOpenOnSelect?: boolean | ((item: ComboboxItem) => boolean),
 *   topObstructionSelector?: string,
 *   clipContainerSelector?: string,
 *   onInput: (value: string) => void,
 *   onSelect: (item: ComboboxItem) => void,
 *   onOpen: () => void,
 *   onRefresh: () => void,
 *   onRerender: () => void,
 * }} opts
 * @returns {TemplateResult}
 */
export function renderCombobox(opts) {
  const {
    id,
    value,
    placeholder,
    ariaLabel,
    items,
    uiState,
    footerText,
    loading,
    disabled = false,
    inputClass = "input mono",
    inputStyle = "",
    keepOpenOnSelect = false,
    topObstructionSelector,
    clipContainerSelector,
    onInput,
    onSelect,
    onOpen,
    onRefresh,
    onRerender,
  } = opts;

  if (disabled && uiState.openId === id) {
    closeActiveCombobox(uiState);
  }

  const popupId = `${id}-popup`;
  const listboxId = `${id}-listbox`;
  const isOpen = !disabled && uiState.openId === id;
  const filteredItems = isOpen ? filterComboboxItems(items, value) : [];

  if (isOpen && uiState.activeIndex >= filteredItems.length) {
    uiState.activeIndex = filteredItems.length > 0 ? filteredItems.length - 1 : -1;
  }

  const activeIndex = isOpen ? uiState.activeIndex : -1;
  const activeDescendant =
    isOpen && activeIndex >= 0 && activeIndex < filteredItems.length
      ? `${listboxId}-opt-${activeIndex}`
      : nothing;

  const openPopup = (/** @type {ElementLike | null} */ el) => {
    if (disabled) {
      return;
    }
    const wasOpen = uiState.openId === id;
    if (!wasOpen) {
      closeActiveCombobox(uiState);
      uiState.openId = id;
      uiState.activeIndex = -1;
      registerViewportListeners(id, uiState, onRerender, {
        topObstructionSelector,
        clipContainerSelector,
      });
    }
    uiState.rect = measureAnchorRect(el);
    if (!wasOpen) {
      onOpen();
    }
  };

  const selectItem = (/** @type {ComboboxItem} */ item) => {
    const keepOpen =
      typeof keepOpenOnSelect === "function"
        ? Boolean(keepOpenOnSelect(item))
        : Boolean(keepOpenOnSelect);
    clearPopupPointerState(uiState);
    onSelect(item);
    if (keepOpen) {
      uiState.activeIndex = -1;
      if (uiState.openId === id) {
        focusComboboxInput(id, false);
      }
    } else {
      if (uiState.openId === id) {
        closeActiveCombobox(uiState);
      }
      focusComboboxInput(id, true);
    }
    onRerender();
  };

  const handleNavigationKey = (
    /** @type {KeyboardEvent} */ e,
    /** @type {ElementLike | null} */ anchorEl
  ) => {
    const currentOpen = uiState.openId === id;
    const currentActiveIndex = currentOpen ? uiState.activeIndex : -1;
    switch (e.key) {
      case "ArrowDown": {
        e.preventDefault();
        if (!currentOpen) {
          openPopup(anchorEl);
          const nextItems = filterComboboxItems(items, value);
          uiState.activeIndex = nextItems.length > 0 ? 0 : -1;
        } else if (filteredItems.length === 0) {
          uiState.activeIndex = -1;
        } else {
          uiState.activeIndex =
            currentActiveIndex < 0
              ? 0
              : (currentActiveIndex + 1) % filteredItems.length;
        }
        scrollOptionIntoView(listboxId, uiState.activeIndex);
        onRerender();
        break;
      }
      case "ArrowUp": {
        e.preventDefault();
        if (!currentOpen) {
          openPopup(anchorEl);
          const nextItems = filterComboboxItems(items, value);
          uiState.activeIndex =
            nextItems.length > 0 ? nextItems.length - 1 : -1;
        } else if (filteredItems.length === 0) {
          uiState.activeIndex = -1;
        } else {
          uiState.activeIndex =
            currentActiveIndex <= 0
              ? filteredItems.length - 1
              : currentActiveIndex - 1;
        }
        scrollOptionIntoView(listboxId, uiState.activeIndex);
        onRerender();
        break;
      }
      case "Enter": {
        if (!currentOpen) return;
        if (
          currentActiveIndex >= 0 &&
          currentActiveIndex < filteredItems.length
        ) {
          e.preventDefault();
          selectItem(filteredItems[currentActiveIndex]);
        } else {
          closeActiveCombobox(uiState);
          onRerender();
        }
        break;
      }
    }
  };

  const handleOpenTrigger = (/** @type {Event} */ e) => {
    if (disabled || uiState.openId === id || suppressFocusOpenFor === id) return;
    const el = /** @type {ElementLike | null} */ (
      /** @type {unknown} */ (e.currentTarget ?? e.target)
    );
    openPopup(el);
    onRerender();
  };

  const rect = uiState.rect;
  const popupStyle = rect
    ? `left: ${rect.popupLeft}px; width: ${rect.popupWidth}px; ${!rect.flipUp ? `top: ${rect.bottom + 2}px; bottom: auto;` : `bottom: ${rect.popupBottom}px; top: auto;`}`
    : "";
  const listboxStyle = rect
    ? `max-height: ${rect.maxListboxHeight}px;`
    : "";

  const emptyMessage =
    loading && items.length === 0 ? "Loading..." : "No matching suggestions";

  return html`
    <div class="combobox-wrap">
      <input
        id=${id}
        type="text"
        class=${inputClass}
        style=${inputStyle || nothing}
        role="combobox"
        aria-label=${ariaLabel}
        aria-autocomplete="list"
        aria-expanded=${isOpen ? "true" : "false"}
        aria-controls=${isOpen ? listboxId : nothing}
        aria-activedescendant=${activeDescendant}
        autocomplete="off"
        placeholder=${placeholder}
        ?disabled=${disabled}
        .value=${value ?? ""}
        @focus=${handleOpenTrigger}
        @click=${handleOpenTrigger}
        @input=${(/** @type {Event} */ e) => {
          if (disabled) return;
          const el = /** @type {ElementLike | null} */ (
            /** @type {unknown} */ (e.target ?? e.currentTarget)
          );
          const nextValue = String(el?.value ?? "");
          uiState.activeIndex = -1;
          if (uiState.openId !== id) {
            openPopup(el);
          } else {
            uiState.rect = measureAnchorRect(el);
          }
          onInput(nextValue);
          onRerender();
        }}
        @keydown=${(/** @type {KeyboardEvent} */ e) => {
          if (disabled || e.isComposing) return;
          const el = /** @type {ElementLike | null} */ (
            /** @type {unknown} */ (e.currentTarget ?? e.target)
          );
          const currentOpen = uiState.openId === id;
          switch (e.key) {
            case "ArrowDown":
            case "ArrowUp":
            case "Enter": {
              handleNavigationKey(e, el);
              break;
            }
            case "Escape": {
              if (!currentOpen) return;
              e.preventDefault();
              e.stopPropagation();
              closeActiveCombobox(uiState);
              onRerender();
              break;
            }
            case "Tab": {
              if (currentOpen) {
                closeActiveCombobox(uiState);
                onRerender();
              }
              break;
            }
          }
        }}
        @blur=${(/** @type {FocusEvent} */ e) => {
          if (uiState.openId !== id) {
            return;
          }
          const related = /** @type {unknown} */ (e?.relatedTarget ?? null);
          if (!related && isPopupPointerDown(uiState)) {
            return;
          }
          if (isTargetInside(getElementByIdSafe(popupId), related)) {
            return;
          }
          closeActiveCombobox(uiState);
          onRerender();
        }}
      />

      ${isOpen
        ? html`
            <div
              id=${popupId}
              class="combobox-popup"
              tabindex="-1"
              style=${popupStyle || nothing}
              @mousedown=${() => {
                if (uiState.openId === id) {
                  beginPopupPointerInteraction(id, popupId, uiState, onRerender);
                }
              }}
              @focusout=${(/** @type {FocusEvent} */ e) => {
                if (uiState.openId !== id) {
                  return;
                }
                const related = /** @type {unknown} */ (e?.relatedTarget ?? null);
                if (!related && isPopupPointerDown(uiState)) {
                  return;
                }
                if (related) {
                  const popupEl = /** @type {ElementLike | null} */ (
                    /** @type {unknown} */ (
                      e?.currentTarget ?? getElementByIdSafe(popupId)
                    )
                  );
                  if (isTargetInside(popupEl, related)) {
                    return;
                  }
                  const inputEl = getElementByIdSafe(id);
                  if (inputEl && related === inputEl) {
                    return;
                  }
                }
                closeActiveCombobox(uiState);
                onRerender();
              }}
              @keydown=${(/** @type {KeyboardEvent} */ e) => {
                if (e.isComposing || uiState.openId !== id) {
                  return;
                }
                const targetTag = /** @type {{ tagName?: string } | null} */ (
                  /** @type {unknown} */ (e.target)
                )?.tagName?.toUpperCase();
                switch (e.key) {
                  case "Escape": {
                    e.preventDefault();
                    e.stopPropagation();
                    closeActiveCombobox(uiState);
                    focusComboboxInput(id, true);
                    onRerender();
                    break;
                  }
                  case "Tab": {
                    closeActiveCombobox(uiState);
                    focusComboboxInput(id, true);
                    onRerender();
                    break;
                  }
                  case "ArrowDown":
                  case "ArrowUp": {
                    clearPopupPointerState(uiState);
                    focusComboboxInput(id, false);
                    handleNavigationKey(e, null);
                    break;
                  }
                  case "Enter": {
                    if (targetTag === "BUTTON") {
                      return;
                    }
                    clearPopupPointerState(uiState);
                    if (
                      uiState.activeIndex >= 0 &&
                      uiState.activeIndex < filteredItems.length
                    ) {
                      handleNavigationKey(e, null);
                    } else {
                      closeActiveCombobox(uiState);
                      focusComboboxInput(id, true);
                      onRerender();
                    }
                    break;
                  }
                  default: {
                    const hasModifier = e.ctrlKey || e.metaKey || e.altKey;
                    const isPrintable =
                      typeof e.key === "string" &&
                      e.key.length === 1 &&
                      !hasModifier;
                    const isBackspace = e.key === "Backspace" && !hasModifier;
                    if (targetTag === "BUTTON" || (!isPrintable && !isBackspace)) {
                      return;
                    }
                    e.preventDefault();
                    clearPopupPointerState(uiState);
                    const inputEl = getElementByIdSafe(id);
                    const curVal =
                      typeof inputEl?.value === "string"
                        ? inputEl.value
                        : (value ?? "");
                    const nextVal = isBackspace
                      ? curVal.slice(0, -1)
                      : curVal + e.key;
                    uiState.activeIndex = -1;
                    onInput(nextVal);
                    focusComboboxInput(id, false);
                    onRerender();
                    break;
                  }
                }
              }}
            >
              <ul
                id=${listboxId}
                class="combobox-listbox"
                role="listbox"
                aria-label=${ariaLabel}
                style=${listboxStyle || nothing}
                ?hidden=${filteredItems.length === 0}
              >
                ${filteredItems.map(
                  (item, idx) => html`
                    <li
                      id=${`${listboxId}-opt-${idx}`}
                      class="combobox-option ${idx === activeIndex ? "active" : ""}"
                      role="option"
                      aria-selected=${idx === activeIndex ? "true" : "false"}
                      @mousedown=${(/** @type {Event} */ e) => {
                        e.preventDefault();
                      }}
                      @click=${() => {
                        selectItem(item);
                      }}
                    >
                      <span class="combobox-option-label">${item.label}</span>
                      ${item.meta
                        ? html`<span class="combobox-option-meta">${item.meta}</span>`
                        : nothing}
                      ${item.badge
                        ? html`<span class="type-badge ${item.badgeClass ?? ""}">${item.badge}</span>`
                        : nothing}
                    </li>
                  `
                )}
              </ul>
              ${filteredItems.length === 0
                ? html`<div class="combobox-empty" role="status">${emptyMessage}</div>`
                : nothing}
              <div class="combobox-footer">
                <span class="combobox-footer-status">${footerText || ""}</span>
                <button
                  type="button"
                  class="combobox-refresh-btn"
                  tabindex="-1"
                  @mousedown=${(/** @type {Event} */ e) => {
                    e.preventDefault();
                  }}
                  @click=${() => {
                    onRefresh();
                    onRerender();
                  }}
                >
                  Refresh
                </button>
              </div>
            </div>
          `
        : nothing}
    </div>
  `;
}
