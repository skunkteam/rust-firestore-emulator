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
 * Decodes and validates project and database path segments.
 * @param {string} rawProject
 * @param {string} rawDatabase
 * @returns {{ projectId: string, databaseId: string } | null}
 */
function decodeProjectAndDatabase(rawProject, rawDatabase) {
  const projectId = decodeURIComponent(rawProject);
  const databaseId = decodeURIComponent(rawDatabase);
  if (!isValidPathSegment(projectId) || !isValidPathSegment(databaseId)) {
    return null;
  }
  return { projectId, databaseId };
}

/**
 * Parses `/projects/{p}/databases/{d}/documents[/...]` or `/projects/{p}/databases/{d}/contention`
 * from a browser pathname.
 * Returns `null` for non-matching routes, malformed percent-encoding, or invalid segments.
 * @param {string} pathname
 * @returns {{ projectId: string, databaseId: string, segments: string[], view?: 'contention' } | null}
 */
export function parseBrowserRoute(pathname) {
  const stripped = pathname.replace(/\/+$/, "");
  if (!stripped) return null;
  const contentionMatch =
    /^\/projects\/([^/]+)\/databases\/([^/]+)\/contention$/.exec(stripped);
  if (contentionMatch) {
    try {
      const ids = decodeProjectAndDatabase(
        contentionMatch[1],
        contentionMatch[2]
      );
      if (!ids) return null;
      return { ...ids, segments: [], view: "contention" };
    } catch {
      return null;
    }
  }
  const m =
    /^\/projects\/([^/]+)\/databases\/([^/]+)\/documents(?:\/(.*))?$/.exec(
      stripped
    );
  if (!m) return null;
  try {
    const ids = decodeProjectAndDatabase(m[1], m[2]);
    if (!ids) return null;
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
    return { ...ids, segments: tail };
  } catch {
    return null;
  }
}

/**
 * Syncs the browser address bar with the given active database, segments, and view.
 * @param {string | null} projectId
 * @param {string | null} databaseId
 * @param {string[]} segments
 * @param {boolean} [replace]
 * @param {'explorer' | 'query' | 'contention'} [view]
 */
export function syncBrowserUrl(
  projectId,
  databaseId,
  segments,
  replace = false,
  view = "explorer"
) {
  let nextPath = "/";
  if (projectId && databaseId) {
    const dbPrefix = `/projects/${encodeURIComponent(projectId)}/databases/${encodeURIComponent(databaseId)}`;
    if (view === "contention") {
      nextPath = `${dbPrefix}/contention`;
    } else {
      const base = `${dbPrefix}/documents`;
      const tail = segments.map((s) => encodeURIComponent(s)).join("/");
      nextPath = tail ? `${base}/${tail}` : base;
    }
  }
  if (window.location.pathname !== nextPath) {
    if (replace) {
      window.history.replaceState(null, "", nextPath);
    } else {
      window.history.pushState(null, "", nextPath);
    }
  }
}
