// @ts-check

/**
 * Returns the Firestore root documents path for a project/database pair, or `null` if unset.
 * @param {string | null} projectId
 * @param {string | null} databaseId
 * @returns {string | null}
 */
export function getActiveRootPath(projectId, databaseId) {
  return projectId && databaseId
    ? `projects/${projectId}/databases/${databaseId}/documents`
    : null;
}

/**
 * Returns true if `seg` is a non-empty, non-dot Firestore path segment without `/`.
 * @param {string} seg
 * @returns {boolean}
 */
export function isValidPathSegment(seg) {
  return Boolean(seg) && seg !== "." && seg !== ".." && !seg.includes("/");
}

/**
 * Extracts path segments relative to `rootPath`, or `null` if `resourcePath` is outside `rootPath`.
 * @param {string} rootPath
 * @param {string} resourcePath
 * @returns {string[] | null}
 */
export function relativeSegments(rootPath, resourcePath) {
  if (!resourcePath.startsWith(`${rootPath}/`)) return null;
  return resourcePath.slice(rootPath.length + 1).split("/");
}

/**
 * Checks whether `full` begins with the exact segment sequence `prefix`.
 * @param {string[]} full
 * @param {string[]} prefix
 * @returns {boolean}
 */
export function startsWithSegments(full, prefix) {
  return (
    prefix.length <= full.length &&
    prefix.every((s, idx) => full[idx] === s)
  );
}

/**
 * Parses `/projects/{p}/databases/{d}/documents[/...]` from a browser pathname.
 * Returns `null` for non-matching routes, malformed percent-encoding, or invalid segments.
 * @param {string} pathname
 * @returns {{ projectId: string, databaseId: string, segments: string[] } | null}
 */
export function parseBrowserRoute(pathname) {
  const stripped = pathname.replace(/\/+$/, "");
  if (!stripped) return null;
  const m =
    /^\/projects\/([^/]+)\/databases\/([^/]+)\/documents(?:\/(.*))?$/.exec(
      stripped
    );
  if (!m) return null;
  try {
    const projectId = decodeURIComponent(m[1]);
    const databaseId = decodeURIComponent(m[2]);
    if (!isValidPathSegment(projectId) || !isValidPathSegment(databaseId)) {
      return null;
    }
    /** @type {string[]} */
    const tail = [];
    if (m[3] !== undefined) {
      for (const rawSeg of m[3].split("/")) {
        const seg = decodeURIComponent(rawSeg);
        if (!isValidPathSegment(seg)) {
          return null;
        }
        tail.push(seg);
      }
    }
    return { projectId, databaseId, segments: tail };
  } catch {
    return null;
  }
}

/**
 * Syncs the browser address bar with the given active database and segments.
 * @param {string | null} projectId
 * @param {string | null} databaseId
 * @param {string[]} segments
 * @param {boolean} [replace]
 */
export function syncBrowserUrl(
  projectId,
  databaseId,
  segments,
  replace = false
) {
  let nextPath = "/";
  if (projectId && databaseId) {
    const base = `/projects/${encodeURIComponent(projectId)}/databases/${encodeURIComponent(databaseId)}/documents`;
    const tail = segments.map((s) => encodeURIComponent(s)).join("/");
    nextPath = tail ? `${base}/${tail}` : base;
  }
  if (window.location.pathname !== nextPath) {
    if (replace) {
      window.history.replaceState(null, "", nextPath);
    } else {
      window.history.pushState(null, "", nextPath);
    }
  }
}
