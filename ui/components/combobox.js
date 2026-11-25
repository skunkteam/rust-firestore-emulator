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
 * }} ElementLike
 */

const POPUP_FLIP_THRESHOLD_PX = 280;
const POPUP_MIN_WIDTH_PX = 220;
const DEFAULT_VIEWPORT_HEIGHT_PX = 800;

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
 * Computes popup geometry and flip direction from an anchor bounding box.
 * @param {RectLike} rect
 * @returns {ComboboxRect}
 */
function computeComboboxRect(rect) {
  const viewportHeight = getViewportHeight();
  const availableBelow = viewportHeight - rect.bottom;
  const availableAbove = rect.top;
  const flipUp =
    availableBelow < POPUP_FLIP_THRESHOLD_PX && availableAbove > availableBelow;
  return {
    top: rect.top,
    bottom: rect.bottom,
    left: rect.left,
    width: rect.width,
    flipUp,
  };
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
 * Returns true if `rect` is outside viewport, clipped by `#topbar`, or outside `.query-builder-pane`.
 * @param {RectLike} rect
 * @returns {boolean}
 */
function isAnchorClipped(rect) {
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

  if (typeof document !== "undefined" && typeof document?.getElementById === "function") {
    const topbar = /** @type {ElementLike | null} */ (
      /** @type {unknown} */ (document.getElementById("topbar"))
    );
    if (typeof topbar?.getBoundingClientRect === "function") {
      const topbarRect = topbar.getBoundingClientRect();
      if (rect.top < topbarRect.bottom || rect.bottom <= topbarRect.bottom) {
        return true;
      }
    }
  }

  if (typeof document !== "undefined" && typeof document?.querySelector === "function") {
    const pane = /** @type {ElementLike | null} */ (
      /** @type {unknown} */ (document.querySelector(".query-builder-pane"))
    );
    if (typeof pane?.getBoundingClientRect === "function") {
      const paneRect = pane.getBoundingClientRect();
      if (rect.bottom <= paneRect.top || rect.top >= paneRect.bottom) {
        return true;
      }
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
    if (typeof document === "undefined" || typeof document?.getElementById !== "function") {
      return;
    }
    const optEl = /** @type {ElementLike | null} */ (
      /** @type {unknown} */ (document.getElementById(`${listboxId}-opt-${index}`))
    );
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
 */
function registerViewportListeners(id, uiState, onRerender) {
  if (typeof window === "undefined" || typeof window?.addEventListener !== "function") {
    return;
  }

  const syncOrDismiss = () => {
    if (uiState.openId !== id) {
      return;
    }
    const anchor =
      typeof document !== "undefined" && typeof document?.getElementById === "function"
        ? /** @type {ElementLike | null} */ (
            /** @type {unknown} */ (document.getElementById(id))
          )
        : null;
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
    if (isAnchorClipped(rect)) {
      closeActiveCombobox(uiState);
      onRerender();
      return;
    }

    uiState.rect = computeComboboxRect(rect);
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
    onInput,
    onSelect,
    onOpen,
    onRefresh,
    onRerender,
  } = opts;

  if (disabled && uiState.openId === id) {
    closeActiveCombobox(uiState);
  }

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
      registerViewportListeners(id, uiState, onRerender);
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
    onSelect(item);
    if (keepOpen) {
      uiState.activeIndex = -1;
    } else if (uiState.openId === id) {
      closeActiveCombobox(uiState);
    }
    onRerender();
  };

  const handleOpenTrigger = (/** @type {Event} */ e) => {
    if (disabled || uiState.openId === id) return;
    const el = /** @type {ElementLike | null} */ (
      /** @type {unknown} */ (e.currentTarget ?? e.target)
    );
    openPopup(el);
    onRerender();
  };

  const rect = uiState.rect;
  const viewportHeight = getViewportHeight();
  const popupStyle = rect
    ? `left: ${rect.left}px; width: ${Math.max(rect.width, POPUP_MIN_WIDTH_PX)}px; ${!rect.flipUp ? `top: ${rect.bottom + 2}px; bottom: auto;` : `bottom: ${viewportHeight - rect.top + 2}px; top: auto;`}`
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
          if (disabled) return;
          const el = /** @type {ElementLike | null} */ (
            /** @type {unknown} */ (e.currentTarget ?? e.target)
          );
          const currentOpen = uiState.openId === id;
          const currentActiveIndex = currentOpen ? uiState.activeIndex : -1;
          switch (e.key) {
            case "ArrowDown": {
              e.preventDefault();
              if (!currentOpen) {
                openPopup(el);
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
                openPopup(el);
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
            case "Home": {
              if (currentOpen && filteredItems.length > 0) {
                e.preventDefault();
                uiState.activeIndex = 0;
                scrollOptionIntoView(listboxId, 0);
                onRerender();
              }
              break;
            }
            case "End": {
              if (currentOpen && filteredItems.length > 0) {
                e.preventDefault();
                uiState.activeIndex = filteredItems.length - 1;
                scrollOptionIntoView(listboxId, uiState.activeIndex);
                onRerender();
              }
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
        @blur=${() => {
          if (uiState.openId === id) {
            closeActiveCombobox(uiState);
            onRerender();
          }
        }}
      />

      ${isOpen
        ? html`
            <div class="combobox-popup" style=${popupStyle || nothing}>
              <ul id=${listboxId} class="combobox-listbox" role="listbox">
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
