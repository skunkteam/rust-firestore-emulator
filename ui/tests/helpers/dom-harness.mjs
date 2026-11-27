// @ts-check

const LIT_NOTHING = Symbol.for("lit-nothing");
const VOID_TAGS = new Set(["INPUT", "BR", "HR", "IMG"]);

/**
 * Lightweight DOM Node base class sufficient for lit-html `render()`.
 */
export class DomNode {
  /**
   * @param {number} nodeType
   */
  constructor(nodeType) {
    this.nodeType = nodeType;
    /** @type {DomNode | null} */
    this.parentNode = null;
    /** @type {DomNode[]} */
    this.childNodes = [];
  }

  /** @returns {DomNode | null} */
  get firstChild() {
    return this.childNodes[0] ?? null;
  }

  /** @returns {DomNode | null} */
  get nextSibling() {
    if (!this.parentNode) return null;
    const list = this.parentNode.childNodes;
    const idx = list.indexOf(this);
    return idx >= 0 && idx + 1 < list.length ? list[idx + 1] : null;
  }

  /**
   * @param {DomNode} node
   * @param {DomNode | null} [refNode]
   * @returns {DomNode}
   */
  insertBefore(node, refNode = null) {
    if (node.nodeType === 11) {
      const kids = [...node.childNodes];
      for (const k of kids) this.insertBefore(k, refNode);
      node.childNodes = [];
      return node;
    }
    if (node.parentNode) node.remove();
    node.parentNode = this;
    const idx = refNode ? this.childNodes.indexOf(refNode) : -1;
    if (idx >= 0) {
      this.childNodes.splice(idx, 0, node);
    } else {
      this.childNodes.push(node);
    }
    return node;
  }

  /**
   * @param {...(DomNode | string)} items
   */
  append(...items) {
    for (const item of items) {
      const node = typeof item === "string" ? new DomText(item) : item;
      this.insertBefore(node, null);
    }
  }

  remove() {
    if (!this.parentNode) return;
    const list = this.parentNode.childNodes;
    const idx = list.indexOf(this);
    if (idx >= 0) list.splice(idx, 1);
    this.parentNode = null;
  }

  /**
   * @param {boolean} [deep]
   * @returns {DomNode}
   */
  cloneNode(deep = false) {
    const copy = this._cloneShallow();
    if (deep) {
      for (const child of this.childNodes) {
        copy.insertBefore(child.cloneNode(true), null);
      }
    }
    return copy;
  }

  /** @returns {DomNode} */
  _cloneShallow() {
    return new DomNode(this.nodeType);
  }
}

export class DomText extends DomNode {
  /** @param {unknown} data */
  constructor(data) {
    super(3);
    this.data = String(data);
  }
  _cloneShallow() {
    return new DomText(this.data);
  }
}

export class DomComment extends DomNode {
  /** @param {unknown} data */
  constructor(data) {
    super(8);
    this.data = String(data);
  }
  _cloneShallow() {
    return new DomComment(this.data);
  }
}

export class DomFragment extends DomNode {
  constructor() {
    super(11);
  }
  _cloneShallow() {
    return new DomFragment();
  }
}

/**
 * Parses an HTML template fragment string into lightweight DomNodes for lit-html.
 * @param {string} htmlStr
 * @returns {DomFragment}
 */
function parseHtmlToFragment(htmlStr) {
  const frag = new DomFragment();
  /** @type {Array<DomNode | DomElement>} */
  const stack = [frag];
  const tokenRe =
    /<!--([\s\S]*?)-->|<\?([^>]*)>|<\/([A-Za-z0-9-]+)\s*>|<([A-Za-z0-9-]+)((?:\s+[^\s=/>]+(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'=<>`]+))?)*)\s*(\/?)>|([^<]+)/g;
  let match;
  while ((match = tokenRe.exec(htmlStr)) !== null) {
    const parent = stack[stack.length - 1];
    if (match[1] !== undefined) {
      parent.insertBefore(new DomComment(match[1]), null);
    } else if (match[2] !== undefined) {
      parent.insertBefore(new DomComment("?" + match[2]), null);
    } else if (match[3] !== undefined) {
      const closeTag = match[3].toUpperCase();
      while (stack.length > 1) {
        const top = stack.pop();
        if (top instanceof DomElement && top.tagName === closeTag) break;
      }
    } else if (match[4] !== undefined) {
      const tag = match[4].toUpperCase();
      const el = new DomElement(tag);
      const attrStr = match[5] ?? "";
      const attrRe =
        /([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
      let am;
      while ((am = attrRe.exec(attrStr)) !== null) {
        const val = am[2] ?? am[3] ?? am[4] ?? "";
        el.setAttribute(am[1], val);
      }
      parent.insertBefore(el, null);
      if (!match[6] && !VOID_TAGS.has(tag)) {
        stack.push(el);
      }
    } else if (match[7] !== undefined) {
      parent.insertBefore(new DomText(match[7]), null);
    }
  }
  return frag;
}

/**
 * Matches basic DOM selectors supported by the mock UI DOM.
 * @param {DomElement} el
 * @param {string} sel
 * @returns {boolean}
 */
function matchesSimpleSelector(el, sel) {
  const s = sel.trim();
  if (!s) return false;
  if (s === '[role="dialog"]') return el.getAttribute("role") === "dialog";
  if (s === "[data-autofocus]:not([disabled])") {
    return el.hasAttribute("data-autofocus") && !el.hasAttribute("disabled");
  }
  if (s === "a[href]") return el.tagName === "A" && el.hasAttribute("href");
  if (s === "button:not([disabled])") {
    return el.tagName === "BUTTON" && !el.hasAttribute("disabled");
  }
  if (s === 'input:not([disabled]):not([type="hidden"])') {
    return (
      el.tagName === "INPUT" &&
      !el.hasAttribute("disabled") &&
      el.getAttribute("type") !== "hidden"
    );
  }
  if (s === "select:not([disabled])") {
    return el.tagName === "SELECT" && !el.hasAttribute("disabled");
  }
  if (s === "textarea:not([disabled])") {
    return el.tagName === "TEXTAREA" && !el.hasAttribute("disabled");
  }
  if (s === '[tabindex]:not([tabindex="-1"])') {
    return el.hasAttribute("tabindex") && el.getAttribute("tabindex") !== "-1";
  }
  if (s.startsWith("[") && s.endsWith("]")) {
    return el.hasAttribute(s.slice(1, -1));
  }
  if (s.startsWith(".")) {
    const className = s.slice(1);
    return (el.getAttribute("class") ?? "").split(/\s+/).includes(className);
  }
  return false;
}

export class DomElement extends DomNode {
  /** @param {string} tagName */
  constructor(tagName) {
    super(1);
    this.tagName = tagName;
    /** @type {Map<string, string>} */
    this._attrs = new Map();
    this.focusCalls = 0;
    if (tagName === "TEMPLATE") {
      this.content = new DomFragment();
    }
  }

  /** @param {string} val */
  set innerHTML(val) {
    this.content = parseHtmlToFragment(String(val));
  }

  /** @returns {string} */
  get textContent() {
    let out = "";
    for (const c of this.childNodes) {
      if (c instanceof DomText) out += c.data;
      else if (c instanceof DomElement) out += c.textContent;
    }
    return out;
  }

  /** @param {unknown} val */
  set textContent(val) {
    for (const c of [...this.childNodes]) c.remove();
    if (val !== "" && val !== null && val !== undefined) {
      this.insertBefore(new DomText(String(val)), null);
    }
  }

  /** @returns {boolean} */
  hasAttributes() {
    return this._attrs.size > 0;
  }

  /** @returns {string[]} */
  getAttributeNames() {
    return Array.from(this._attrs.keys());
  }

  /**
   * @param {string} name
   * @returns {string | null}
   */
  getAttribute(name) {
    return this._attrs.has(name) ? (this._attrs.get(name) ?? null) : null;
  }

  /**
   * @param {string} name
   * @returns {boolean}
   */
  hasAttribute(name) {
    return this._attrs.has(name);
  }

  /**
   * @param {string} name
   * @param {unknown} val
   */
  setAttribute(name, val) {
    this._attrs.set(name, String(val));
  }

  /** @param {string} name */
  removeAttribute(name) {
    this._attrs.delete(name);
  }

  /**
   * @param {string} name
   * @param {boolean} [force]
   */
  toggleAttribute(name, force) {
    if (force) this._attrs.set(name, "");
    else this._attrs.delete(name);
  }

  addEventListener() {}
  removeEventListener() {}

  /** @returns {boolean} */
  get isConnected() {
    return true;
  }

  focus() {
    this.focusCalls++;
    harness.activeElement = this;
  }

  /**
   * @param {unknown} target
   * @returns {boolean}
   */
  contains(target) {
    if (target === this) return true;
    for (const c of this.childNodes) {
      if (c === target || (c instanceof DomElement && c.contains(target))) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param {DomNode} node
   * @returns {DomNode}
   */
  appendChild(node) {
    return this.insertBefore(node, null);
  }

  /**
   * @param {string} selector
   * @returns {DomElement[]}
   */
  querySelectorAll(selector) {
    const parts = selector
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean);
    /** @type {DomElement[]} */
    const out = [];
    /** @param {DomNode} node */
    const visit = (node) => {
      for (const c of node.childNodes) {
        if (c instanceof DomElement) {
          if (parts.some((p) => matchesSimpleSelector(c, p))) {
            out.push(c);
          }
          visit(c);
        }
      }
    };
    visit(this);
    return out;
  }

  /**
   * @param {string} selector
   * @returns {DomElement | null}
   */
  querySelector(selector) {
    return this.querySelectorAll(selector)[0] ?? null;
  }

  _cloneShallow() {
    const el = new DomElement(this.tagName);
    for (const [k, v] of this._attrs) el._attrs.set(k, v);
    return el;
  }
}

/**
 * @param {DomNode} root
 * @param {number} whatToShow
 * @returns {DomNode[]}
 */
function collectPreOrder(root, whatToShow) {
  /** @type {DomNode[]} */
  const nodes = [];
  /** @param {DomNode} n */
  const walk = (n) => {
    for (const c of n.childNodes) {
      if (whatToShow === 129 ? c.nodeType === 1 || c.nodeType === 8 : true) {
        nodes.push(c);
      }
      walk(c);
    }
  };
  walk(root);
  return nodes;
}

const origSetTimeout = globalThis.setTimeout;
const origClearTimeout = globalThis.clearTimeout;
const origSetInterval = globalThis.setInterval;
const origClearInterval = globalThis.clearInterval;
const origDateNow = Date.now;
const defaultRaf = (/** @type {(time: number) => void} */ fn) => {
  fn(0);
  return 0;
};

/** @type {Set<ReturnType<typeof origSetTimeout>>} */
const trackedTimeouts = new Set();

export class MockEventSource {
  /** @param {string} url */
  constructor(url) {
    this.url = url;
    this.closed = false;
    this.closeCount = 0;
    /** @type {Map<string, Array<(ev: unknown) => void>>} */
    this.listeners = new Map();
    harness.eventSources.push(this);
    harness.lastEventSource = this;
  }

  /**
   * @param {string} type
   * @param {(ev: unknown) => void} fn
   */
  addEventListener(type, fn) {
    const list = this.listeners.get(type) ?? [];
    list.push(fn);
    this.listeners.set(type, list);
  }

  /**
   * @param {string} type
   * @param {unknown} [data]
   */
  emit(type, data) {
    if (this.closed) return;
    for (const fn of this.listeners.get(type) ?? []) {
      fn(data);
    }
  }

  close() {
    this.closed = true;
    this.closeCount++;
  }
}

/**
 * @param {string} url
 * @param {RequestInit} [_init]
 * @returns {Promise<unknown>}
 */
async function unexpectedFetch(url, _init) {
  throw new Error(`unexpected fetch call: ${url}`);
}

export const harness = {
  /** @type {(url: string, init: Record<string, unknown>) => Promise<unknown>} */
  fetchImpl: unexpectedFetch,
  /** @type {MockEventSource[]} */
  eventSources: [],
  /** @type {MockEventSource | null} */
  lastEventSource: null,
  /** @type {((url: string) => unknown) | null} */
  eventSourceFactory: null,
  /** @type {'visible' | 'hidden'} */
  visibilityState: "visible",
  /** @type {unknown} */
  activeElement: null,
  /** @type {(id: string) => unknown} */
  getElementById: () => null,
  /** @type {(selector: string) => unknown} */
  querySelector: () => null,
  /** @type {Array<() => void>} */
  popstateListeners: [],
  /** @type {Array<(e: unknown) => void>} */
  docKeydownListeners: [],
  /** @type {Array<() => unknown>} */
  docVisibilityListeners: [],
  /** @type {Map<string, Array<{ fn: Function, opts: unknown }>>} */
  winListeners: new Map(),
};

/**
 * Installs default timeout tracking so un-awaited toast timers don't leak across tests.
 */
function installTrackedRealTimers() {
  globalThis.setTimeout = /** @type {typeof globalThis.setTimeout} */ (
    (
      /** @type {(...args: unknown[]) => void} */ fn,
      ms = 0,
      /** @type {unknown[]} */ ...args
    ) => {
      const id = origSetTimeout(
        (...cbArgs) => {
          trackedTimeouts.delete(id);
          fn(...cbArgs);
        },
        ms,
        ...args
      );
      if (typeof id === "object" && id !== null && "unref" in id && ms >= 1000) {
        /** @type {{ unref?: () => void }} */ (id).unref?.();
      }
      trackedTimeouts.add(id);
      return id;
    }
  );
  globalThis.clearTimeout = /** @type {typeof globalThis.clearTimeout} */ (
    (/** @type {ReturnType<typeof origSetTimeout> | undefined} */ id) => {
      if (id !== undefined) {
        trackedTimeouts.delete(id);
        origClearTimeout(id);
      }
    }
  );
}

installTrackedRealTimers();

/**
 * Installs global browser mocks once before UI modules are imported.
 */
function installGlobalShims() {
  const docStub = {
    nodeType: 9,
    childNodes: [],
    get visibilityState() {
      return harness.visibilityState;
    },
    get activeElement() {
      return harness.activeElement;
    },
    set activeElement(el) {
      harness.activeElement = el;
    },
    getElementById: (/** @type {string} */ id) => harness.getElementById(id),
    querySelector: (/** @type {string} */ sel) => harness.querySelector(sel),
    createComment: (/** @type {unknown} */ data) => new DomComment(data),
    createTextNode: (/** @type {unknown} */ data) => new DomText(data),
    createElement: (/** @type {string} */ tag) =>
      new DomElement(String(tag).toUpperCase()),
    importNode: (/** @type {DomNode} */ node, /** @type {boolean} */ deep) =>
      node.cloneNode(Boolean(deep)),
    createTreeWalker(/** @type {DomNode} */ root, /** @type {number} */ whatToShow) {
      return {
        _root: root,
        _current: /** @type {DomNode | null} */ (root),
        get currentNode() {
          return this._current;
        },
        set currentNode(node) {
          if (node) {
            this._root = node;
            this._current = node;
          }
        },
        nextNode() {
          const list = collectPreOrder(this._root, whatToShow);
          if (this._current === this._root) {
            this._current = list[0] ?? null;
            return this._current;
          }
          const idx = this._current ? list.indexOf(this._current) : -1;
          this._current =
            idx >= 0 && idx + 1 < list.length ? list[idx + 1] : null;
          return this._current;
        },
      };
    },
    addEventListener(/** @type {string} */ type, /** @type {Function} */ fn) {
      if (type === "keydown") {
        harness.docKeydownListeners.push(
          /** @type {(e: unknown) => void} */ (fn)
        );
      }
      if (type === "visibilitychange") {
        harness.docVisibilityListeners.push(/** @type {() => unknown} */ (fn));
      }
    },
    removeEventListener(/** @type {string} */ type, /** @type {Function} */ fn) {
      if (type === "keydown") {
        const idx = harness.docKeydownListeners.indexOf(
          /** @type {(e: unknown) => void} */ (fn)
        );
        if (idx >= 0) harness.docKeydownListeners.splice(idx, 1);
      }
      if (type === "visibilitychange") {
        const idx = harness.docVisibilityListeners.indexOf(
          /** @type {() => unknown} */ (fn)
        );
        if (idx >= 0) harness.docVisibilityListeners.splice(idx, 1);
      }
    },
  };

  const winStub = {
    EMULATOR_VERSION: "test",
    innerHeight: 800,
    innerWidth: 1200,
    location: { pathname: "/" },
    history: {
      pushState(
        /** @type {unknown} */ _state,
        /** @type {unknown} */ _title,
        /** @type {unknown} */ url
      ) {
        winStub.location.pathname = String(url);
      },
      replaceState(
        /** @type {unknown} */ _state,
        /** @type {unknown} */ _title,
        /** @type {unknown} */ url
      ) {
        winStub.location.pathname = String(url);
      },
    },
    addEventListener(
      /** @type {string} */ type,
      /** @type {Function} */ fn,
      /** @type {unknown} */ opts
    ) {
      if (type === "popstate") {
        harness.popstateListeners.push(/** @type {() => void} */ (fn));
        return;
      }
      const list = harness.winListeners.get(type) ?? [];
      list.push({ fn, opts });
      harness.winListeners.set(type, list);
    },
    removeEventListener(
      /** @type {string} */ type,
      /** @type {Function} */ fn,
      /** @type {{ capture?: boolean } | undefined} */ opts
    ) {
      if (type === "popstate") {
        const idx = harness.popstateListeners.indexOf(
          /** @type {() => void} */ (fn)
        );
        if (idx >= 0) harness.popstateListeners.splice(idx, 1);
        return;
      }
      const list = harness.winListeners.get(type) ?? [];
      const idx = list.findIndex(
        (x) =>
          x.fn === fn &&
          Boolean(
            /** @type {{ capture?: boolean } | undefined} */ (x.opts)?.capture
          ) === Boolean(opts?.capture)
      );
      if (idx >= 0) list.splice(idx, 1);
    },
  };

  class GlobalEventSource {
    /** @param {string} url */
    constructor(url) {
      if (harness.eventSourceFactory) {
        return /** @type {GlobalEventSource} */ (
          harness.eventSourceFactory(url)
        );
      }
      return /** @type {GlobalEventSource} */ (
        /** @type {unknown} */ (new MockEventSource(url))
      );
    }
  }

  /** @type {Record<string, unknown>} */
  const g = globalThis;
  g.document = docStub;
  g.window = winStub;
  g.requestAnimationFrame = defaultRaf;
  g.EventSource = GlobalEventSource;
  g.fetch = (/** @type {unknown} */ url, /** @type {Record<string, unknown> | undefined} */ init) =>
    harness.fetchImpl(String(url), init ?? {});
}

installGlobalShims();

/**
 * Resets global mocks and (optionally) `ui/app.js` + `ui/state.js` singletons between tests.
 * Preserves permanent `app.js` `popstate` / `visibilitychange` listeners while resetting everything else.
 * @param {{
 *   state: import('../../state.js').AppState,
 *   flushSseQueue: () => Promise<void>,
 *   switchView: (view: 'explorer' | 'query' | 'contention') => Promise<void>,
 * }} [appModule]
 * @param {{
 *   clearActiveDatabaseState: (targetState?: import('../../state.js').AppState) => void,
 *   bumpDbEpoch: () => number,
 *   bumpNavEpoch: () => number,
 * }} [stateModule]
 */
export async function resetHarness(appModule, stateModule) {
  for (const id of [...trackedTimeouts]) {
    origClearTimeout(id);
    trackedTimeouts.delete(id);
  }
  Date.now = origDateNow;
  installTrackedRealTimers();
  globalThis.setInterval = origSetInterval;
  globalThis.clearInterval = origClearInterval;

  for (const es of harness.eventSources) {
    es.close();
    es.listeners.clear();
  }
  harness.eventSources = [];
  harness.lastEventSource = null;
  harness.eventSourceFactory = null;
  harness.fetchImpl = unexpectedFetch;
  harness.visibilityState = "visible";
  harness.activeElement = null;
  harness.getElementById = () => null;
  harness.querySelector = () => null;
  harness.winListeners.clear();

  /** @type {Record<string, unknown>} */
  const g = globalThis;
  const win = /** @type {Record<string, unknown>} */ (g.window);
  win.innerHeight = 800;
  win.innerWidth = 1200;
  /** @type {{ pathname: string }} */ (win.location).pathname = "/";
  g.requestAnimationFrame = defaultRaf;
  const doc = /** @type {Record<string, unknown>} */ (g.document);
  doc.getElementById = (/** @type {string} */ id) => harness.getElementById(id);
  doc.querySelector = (/** @type {string} */ sel) => harness.querySelector(sel);

  if (appModule) {
    if (appModule.state.modal) {
      appModule.state.modal.busy = false;
    } else if (harness.docKeydownListeners.length > 0) {
      // A test cleared `state.modal = null` without running `renderApp()`, leaving
      // `app.js`'s internal `wasModalOpen` and `modalKeydownListener` out of sync.
      // Install a temporary non-busy sentinel modal so dispatching `Escape` runs
      // `closeActiveModal()` -> `renderApp()` -> `syncModalAccessibility(...)`.
      appModule.state.modal = {
        kind: "clear_db",
        projectId: "",
        databaseId: "",
        busy: false,
        error: null,
      };
    }
    if (harness.docKeydownListeners.length > 0) {
      for (const fn of [...harness.docKeydownListeners]) {
        fn({ key: "Escape", preventDefault() {} });
      }
      harness.docKeydownListeners.length = 0;
    }
    appModule.state.modal = null;

    if (stateModule) {
      stateModule.bumpDbEpoch();
      stateModule.bumpNavEpoch();
      stateModule.clearActiveDatabaseState(appModule.state);
    } else {
      appModule.state.activeProjectId = null;
      appModule.state.activeDatabaseId = null;
      appModule.state.segments = [];
      appModule.state.activeDbMeta = null;
      appModule.state.rootCollections = [];
      appModule.state.rootLoading = false;
      appModule.state.collectionColumns.clear();
      appModule.state.documentLevels.clear();
    }
    appModule.state.databases = [];
    appModule.state.ssePaused = false;
    appModule.state.sseStatus = "live";
    appModule.state.contentionSseStatus = "idle";
    appModule.state.contention.scope = "active";
    appModule.state.toasts = [];
    if (appModule.state.view !== "explorer") {
      await appModule.switchView("explorer");
    }
    await appModule.flushSseQueue();
  }
}

/**
 * Creates an externally controllable Promise.
 * @template [T=unknown]
 * @returns {{ promise: Promise<T>, resolve: (val: T) => void, reject: (err?: unknown) => void }}
 */
export function deferred() {
  /** @type {(val: T) => void} */
  let resolve = () => {};
  /** @type {(err?: unknown) => void} */
  let reject = () => {};
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/**
 * Creates a minimal Response-like object returning JSON or raw string text.
 * @param {unknown} body
 * @param {number} [status]
 */
export function jsonResponse(body, status = 200) {
  const text = typeof body === "string" ? body : JSON.stringify(body);
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 404 ? "Not Found" : "OK",
    text: async () => text,
  };
}

/**
 * Yields to macrotask queue (`setTimeout(0)`).
 * @param {number} [ms]
 * @returns {Promise<void>}
 */
export function nextTick(ms = 0) {
  return new Promise((r) => origSetTimeout(r, ms));
}

/**
 * Flushes pending microtasks cleanly.
 * @param {number} [rounds]
 */
export async function flushMicrotasks(rounds = 6) {
  for (let i = 0; i < rounds; i++) {
    await Promise.resolve();
  }
}

/**
 * Installs deterministic virtual `setTimeout`/`clearTimeout` timers.
 */
export function installFakeTimers() {
  let virtualNow = 0;
  let nextTimerId = 1;
  /** @type {Map<number, { fn: () => void, runAt: number, seq: number, ms: number }>} */
  const scheduled = new Map();

  globalThis.setTimeout = /** @type {typeof globalThis.setTimeout} */ (
    (/** @type {() => void} */ fn, ms = 0) => {
      const id = nextTimerId++;
      scheduled.set(id, {
        fn,
        runAt: virtualNow + Number(ms),
        seq: id,
        ms: Number(ms),
      });
      return id;
    }
  );
  globalThis.clearTimeout = /** @type {typeof globalThis.clearTimeout} */ (
    (/** @type {number} */ id) => {
      scheduled.delete(id);
    }
  );

  return {
    scheduled,
    /** @param {number} deltaMs */
    async advanceTimers(deltaMs) {
      const target = virtualNow + deltaMs;
      while (true) {
        /** @type {{ fn: () => void, runAt: number, seq: number, ms: number } | null} */
        let next = null;
        for (const entry of scheduled.values()) {
          if (entry.runAt <= target) {
            if (
              !next ||
              entry.runAt < next.runAt ||
              (entry.runAt === next.runAt && entry.seq < next.seq)
            ) {
              next = entry;
            }
          }
        }
        if (!next) break;
        scheduled.delete(next.seq);
        virtualNow = next.runAt;
        next.fn();
        await flushMicrotasks();
      }
      virtualNow = target;
    },
    restore() {
      installTrackedRealTimers();
    },
  };
}

/**
 * Installs inspectable `setInterval`/`clearInterval` mocks.
 */
export function installFakeIntervals() {
  let nextIntervalId = 1;
  /** @type {Map<number, { fn: () => void, ms: number }>} */
  const activeIntervals = new Map();
  /** @type {Array<{ id: number, ms: number }>} */
  const startedIntervals = [];
  /** @type {number[]} */
  const clearedIntervals = [];

  globalThis.setInterval = /** @type {typeof globalThis.setInterval} */ (
    (/** @type {() => void} */ fn, ms = 0) => {
      const id = nextIntervalId++;
      activeIntervals.set(id, { fn, ms: Number(ms) });
      startedIntervals.push({ id, ms: Number(ms) });
      return id;
    }
  );
  globalThis.clearInterval = /** @type {typeof globalThis.clearInterval} */ (
    (/** @type {number} */ id) => {
      if (activeIntervals.has(id)) {
        activeIntervals.delete(id);
        clearedIntervals.push(id);
      }
      origClearInterval(id);
    }
  );

  return {
    activeIntervals,
    startedIntervals,
    clearedIntervals,
    restore() {
      globalThis.setInterval = origSetInterval;
      globalThis.clearInterval = origClearInterval;
    },
  };
}

/**
 * @param {unknown} node
 * @returns {node is { strings: readonly string[], values: readonly unknown[] }}
 */
function isTemplateResult(node) {
  return Boolean(
    node &&
      typeof node === "object" &&
      "strings" in node &&
      "values" in node &&
      Array.isArray(/** @type {{ strings: unknown }} */ (node).strings) &&
      Array.isArray(/** @type {{ values: unknown }} */ (node).values)
  );
}

/**
 * Recursively extracts visible text from a `lit-html` TemplateResult tree.
 * @param {unknown} node
 * @returns {string}
 */
export function flattenText(node) {
  if (node == null || typeof node === "symbol") return "";
  if (
    typeof node === "string" ||
    typeof node === "number" ||
    typeof node === "boolean"
  ) {
    return String(node);
  }
  if (Array.isArray(node)) {
    return node.map(flattenText).join("");
  }
  if (isTemplateResult(node)) {
    let out = "";
    for (let i = 0; i < node.strings.length; i++) {
      out += node.strings[i];
      if (i < node.values.length) {
        out += flattenText(node.values[i]);
      }
    }
    return out;
  }
  return "";
}

/**
 * Serializes a `lit-html` TemplateResult tree into an HTML markup string.
 * @param {unknown} node
 * @returns {string}
 */
export function renderTemplateMarkup(node) {
  if (node == null || node === LIT_NOTHING || typeof node === "symbol") {
    return "";
  }
  if (
    typeof node === "string" ||
    typeof node === "number" ||
    typeof node === "boolean"
  ) {
    return String(node);
  }
  if (Array.isArray(node)) {
    return node.map(renderTemplateMarkup).join("");
  }
  if (isTemplateResult(node)) {
    let out = "";
    for (let i = 0; i < node.strings.length; i++) {
      let chunk = node.strings[i];
      if (i < node.values.length) {
        const val = node.values[i];
        const boolMatch = /\s+\?([A-Za-z0-9_:-]+)=$/.exec(chunk);
        if (boolMatch) {
          chunk =
            chunk.slice(0, boolMatch.index) +
            (val && val !== LIT_NOTHING ? ` ${boolMatch[1]}` : "");
          out += chunk;
          continue;
        }
        if (val === LIT_NOTHING) {
          const attrMatch = /\s+[A-Za-z0-9_:-]+=$/.exec(chunk);
          if (attrMatch) {
            chunk = chunk.slice(0, attrMatch.index);
          }
          out += chunk;
          continue;
        }
        out += chunk;
        if (typeof val === "function") {
          out += "[fn]";
        } else if (typeof val === "string" && /=\s*$/.test(chunk)) {
          out += JSON.stringify(val);
        } else {
          out += renderTemplateMarkup(val);
        }
      } else {
        out += chunk;
      }
    }
    return out;
  }
  return "";
}

/**
 * @typedef {{
 *   tagName: string,
 *   attrs: Record<string, unknown>,
 *   handlers: Record<string, (...args: unknown[]) => unknown>,
 * }} TemplateElementInfo
 */

/**
 * Parses all HTML opening tags in a `lit-html` TemplateResult tree into structured
 * `{ tagName, attrs, handlers }` records without relying on positional `values[i - k]` offsets.
 * @param {unknown} node
 * @param {TemplateElementInfo[]} [out]
 * @returns {TemplateElementInfo[]}
 */
export function collectElements(node, out = []) {
  if (!node || typeof node !== "object") return out;
  if (Array.isArray(node)) {
    for (const item of node) collectElements(item, out);
    return out;
  }
  if (!isTemplateResult(node)) return out;

  /** @type {TemplateElementInfo | null} */
  let current = null;
  let attrPart = "";
  /** @type {'"' | "'" | null} */
  let quoteChar = null;
  let inComment = false;

  /**
   * @param {string} rawName
   * @param {unknown} val
   */
  const recordBinding = (rawName, val) => {
    if (!current || !rawName) return;
    if (rawName.startsWith("@")) {
      if (typeof val === "function") {
        current.handlers[rawName.slice(1)] =
          /** @type {(...args: unknown[]) => unknown} */ (val);
      }
      return;
    }
    if (rawName.startsWith("?")) {
      if (val && val !== LIT_NOTHING) {
        current.attrs[rawName.slice(1)] = true;
      }
      return;
    }
    if (rawName.startsWith(".")) {
      current.attrs[rawName.slice(1)] = val;
      return;
    }
    if (val !== LIT_NOTHING) {
      current.attrs[rawName] = val;
    }
  };

  const flushStaticAttrs = (/** @type {string} */ segment) => {
    if (!current) return;
    const re =
      /([@?.]?[A-Za-z0-9_:-]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
    let m;
    while ((m = re.exec(segment)) !== null) {
      const name = m[1];
      if (name === "/") continue;
      const val = m[2] ?? m[3] ?? m[4];
      if (val !== undefined) {
        recordBinding(name, val);
      } else {
        recordBinding(name, true);
      }
    }
  };

  for (let i = 0; i < node.strings.length; i++) {
    const s = node.strings[i];
    let pos = 0;

    while (pos < s.length) {
      if (inComment) {
        const end = s.indexOf("-->", pos);
        if (end < 0) {
          break;
        }
        inComment = false;
        pos = end + 3;
        continue;
      }

      if (!current) {
        const lt = s.indexOf("<", pos);
        if (lt < 0) {
          break;
        }
        if (s.startsWith("<!--", lt)) {
          inComment = true;
          pos = lt + 4;
          continue;
        }
        const tagMatch = /^<([A-Za-z][A-Za-z0-9-]*)(?=[\s/>]|$)/.exec(
          s.slice(lt)
        );
        if (!tagMatch) {
          pos = lt + 1;
          continue;
        }
        current = {
          tagName: tagMatch[1].toLowerCase(),
          attrs: Object.create(null),
          handlers: Object.create(null),
        };
        attrPart = "";
        quoteChar = null;
        pos = lt + 1 + tagMatch[1].length;
        continue;
      }

      const ch = s[pos];
      if (quoteChar !== null) {
        attrPart += ch;
        if (ch === quoteChar) {
          quoteChar = null;
        }
        pos++;
        continue;
      }
      if (ch === '"' || ch === "'") {
        quoteChar = ch;
        attrPart += ch;
        pos++;
        continue;
      }
      if (ch === ">") {
        flushStaticAttrs(attrPart);
        out.push(current);
        current = null;
        attrPart = "";
        pos++;
        continue;
      }
      attrPart += ch;
      pos++;
    }

    if (i < node.values.length) {
      const val = node.values[i];
      if (current) {
        if (quoteChar !== null) {
          attrPart += val === LIT_NOTHING || val == null ? "" : String(val);
        } else {
          const eqMatch = /([@?.]?[A-Za-z0-9_:-]+)\s*=\s*$/.exec(attrPart);
          if (eqMatch) {
            flushStaticAttrs(attrPart.slice(0, eqMatch.index));
            recordBinding(eqMatch[1], val);
            attrPart = "";
          }
        }
      } else {
        collectElements(val, out);
      }
    }
  }

  return out;
}

/**
 * Finds all elements matching `predicate`.
 * @param {unknown} node
 * @param {(el: TemplateElementInfo) => boolean} predicate
 * @returns {TemplateElementInfo[]}
 */
export function findAllElements(node, predicate) {
  return collectElements(node).filter(predicate);
}

/**
 * Finds the first element matching `predicate`.
 * @param {unknown} node
 * @param {(el: TemplateElementInfo) => boolean} predicate
 * @returns {TemplateElementInfo | null}
 */
export function findElement(node, predicate) {
  return collectElements(node).find(predicate) ?? null;
}

/**
 * Finds the first element with matching `id` attribute.
 * @param {unknown} node
 * @param {string} id
 * @returns {TemplateElementInfo | null}
 */
export function findElementById(node, id) {
  return findElement(node, (el) => el.attrs.id === id);
}

/**
 * Finds the first element whose `class` attribute includes `classSubstr`.
 * @param {unknown} node
 * @param {string} classSubstr
 * @returns {TemplateElementInfo | null}
 */
export function findElementByClass(node, classSubstr) {
  return findElement(
    node,
    (el) => typeof el.attrs.class === "string" && el.attrs.class.includes(classSubstr)
  );
}

/**
 * Finds the first element with exact `aria-label` (optionally constrained by `tagName`).
 * @param {unknown} node
 * @param {string} ariaLabel
 * @param {string} [tagName]
 * @returns {TemplateElementInfo | null}
 */
export function findElementByAriaLabel(node, ariaLabel, tagName) {
  return findElement(
    node,
    (el) =>
      el.attrs["aria-label"] === ariaLabel &&
      (!tagName || el.tagName === tagName.toLowerCase())
  );
}

/**
 * Finds a `<select>` element by its `aria-label`.
 * @param {unknown} node
 * @param {string} ariaLabel
 * @returns {TemplateElementInfo | null}
 */
export function findSelectByAriaLabel(node, ariaLabel) {
  return findElementByAriaLabel(node, ariaLabel, "select");
}

/**
 * Finds an `<input>` or `<textarea>` element whose `placeholder` or `aria-label` contains `substr`.
 * @param {unknown} node
 * @param {string} substr
 * @returns {TemplateElementInfo | null}
 */
export function findInputByPlaceholder(node, substr) {
  return findElement(
    node,
    (el) =>
      (el.tagName === "input" || el.tagName === "textarea") &&
      ((typeof el.attrs.placeholder === "string" &&
        el.attrs.placeholder.includes(substr)) ||
        (typeof el.attrs["aria-label"] === "string" &&
          el.attrs["aria-label"].includes(substr)))
  );
}

/**
 * Finds an `<input>` or `<textarea>` element by exact `aria-label`.
 * @param {unknown} node
 * @param {string} ariaLabel
 * @returns {TemplateElementInfo | null}
 */
export function findInputByAriaLabel(node, ariaLabel) {
  return findElement(
    node,
    (el) =>
      (el.tagName === "input" || el.tagName === "textarea") &&
      el.attrs["aria-label"] === ariaLabel
  );
}

/**
 * Finds the `@click` handler of a `<button>` or `<li>` whose rendered inner text contains `labelText`.
 * @param {unknown} node
 * @param {string} labelText
 * @returns {((...args: unknown[]) => unknown) | null}
 */
export function findClickHandler(node, labelText) {
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {
    for (const item of node) {
      const hit = findClickHandler(item, labelText);
      if (hit) return hit;
    }
    return null;
  }
  if (isTemplateResult(node)) {
    for (let i = 0; i < node.values.length; i++) {
      const v = node.values[i];
      if (typeof v === "function" && /@click=\s*$/.test(node.strings[i])) {
        let tagBefore = "";
        for (let k = i; k >= 0; k--) {
          tagBefore = node.strings[k] + tagBefore;
          const lastLt = tagBefore.lastIndexOf("<");
          if (lastLt >= 0) {
            tagBefore = tagBefore.slice(lastLt);
            break;
          }
        }
        const tagMatch = /^<(button|li)\b/.exec(tagBefore);
        if (tagMatch) {
          const closeTag = `</${tagMatch[1]}>`;
          let inner = "";
          for (let j = i + 1; j < node.strings.length; j++) {
            const s = node.strings[j];
            const closeIdx = s.indexOf(closeTag);
            if (closeIdx >= 0) {
              inner += s.slice(0, closeIdx);
              break;
            }
            inner += s;
            if (j < node.values.length) {
              inner += flattenText(node.values[j]);
            }
          }
          if (inner.includes(labelText)) {
            return /** @type {(...args: unknown[]) => unknown} */ (v);
          }
        }
      }
      const nested = findClickHandler(v, labelText);
      if (nested) return nested;
    }
  }
  return null;
}
