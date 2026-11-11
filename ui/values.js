// @ts-check

/**
 * @typedef {'stringValue' | 'integerValue' | 'doubleValue' | 'booleanValue' | 'nullValue' | 'timestampValue' | 'referenceValue' | 'bytesValue' | 'geoPointValue' | 'arrayValue' | 'mapValue'} ValueTypeKey
 */

/**
 * @typedef {{
 *   stringValue?: string,
 *   integerValue?: string,
 *   doubleValue?: number | 'NaN' | 'Infinity' | '-Infinity',
 *   booleanValue?: boolean,
 *   nullValue?: null,
 *   timestampValue?: string,
 *   referenceValue?: string,
 *   bytesValue?: string,
 *   geoPointValue?: { latitude?: number, longitude?: number },
 *   arrayValue?: { values?: FirestoreValue[] },
 *   mapValue?: { fields?: Record<string, FirestoreValue> },
 * }} FirestoreValue
 */

/**
 * @typedef {{
 *   name?: string,
 *   fields?: Record<string, FirestoreValue>,
 *   createTime?: string,
 *   updateTime?: string,
 * }} FirestoreDocument
 */

const AUTO_ID_ALPHABET =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

const I64_MIN = -9223372036854775808n;
const I64_MAX = 9223372036854775807n;

/** @type { ReadonlyArray<{ key: ValueTypeKey, label: string, shortLabel: string }> } */
export const VALUE_TYPES = [
  { key: "stringValue", label: "string", shortLabel: "str" },
  { key: "integerValue", label: "int64", shortLabel: "i64" },
  { key: "doubleValue", label: "double", shortLabel: "f64" },
  { key: "booleanValue", label: "boolean", shortLabel: "bool" },
  { key: "nullValue", label: "null", shortLabel: "null" },
  { key: "timestampValue", label: "timestamp", shortLabel: "time" },
  { key: "referenceValue", label: "reference", shortLabel: "ref" },
  { key: "bytesValue", label: "bytes", shortLabel: "bytes" },
  { key: "geoPointValue", label: "geopoint", shortLabel: "geo" },
  { key: "arrayValue", label: "array", shortLabel: "arr" },
  { key: "mapValue", label: "map", shortLabel: "map" },
];

const VALID_TYPE_SET = new Set(VALUE_TYPES.map((t) => t.key));

/**
 * Generates a 20-character alphanumeric Firestore document ID client-side
 * without modulo bias.
 * @returns {string}
 */
export function generateAutoId() {
  let out = "";
  const buf = new Uint8Array(40);
  while (out.length < 20) {
    crypto.getRandomValues(buf);
    for (let i = 0; i < buf.length && out.length < 20; i++) {
      const b = buf[i];
      // 248 = 62 * 4 (largest multiple of 62 < 256)
      if (b < 248) {
        out += AUTO_ID_ALPHABET[b % 62];
      }
    }
  }
  return out;
}

/**
 * Extracts the active ValueTypeKey of a Firestore ProtoJSON Value object.
 * @param {FirestoreValue | undefined | null} val
 * @returns {ValueTypeKey}
 */
export function getValueType(val) {
  if (!val || typeof val !== "object") {
    return "nullValue";
  }
  for (const item of VALUE_TYPES) {
    if (Object.prototype.hasOwnProperty.call(val, item.key)) {
      return item.key;
    }
  }
  return "nullValue";
}

/**
 * Creates a default FirestoreValue for the given ValueTypeKey.
 * @param {ValueTypeKey} typeKey
 * @param {string} projectId
 * @param {string} databaseId
 * @returns {FirestoreValue}
 */
export function createDefaultValue(typeKey, projectId, databaseId) {
  switch (typeKey) {
    case "stringValue":
      return { stringValue: "" };
    case "integerValue":
      return { integerValue: "0" };
    case "doubleValue":
      return { doubleValue: 0 };
    case "booleanValue":
      return { booleanValue: false };
    case "nullValue":
      return { nullValue: null };
    case "timestampValue":
      return { timestampValue: new Date().toISOString() };
    case "referenceValue":
      return {
        referenceValue: `projects/${projectId}/databases/${databaseId}/documents/collection/doc`,
      };
    case "bytesValue":
      return { bytesValue: "" };
    case "geoPointValue":
      return { geoPointValue: { latitude: 0, longitude: 0 } };
    case "arrayValue":
      return { arrayValue: { values: [] } };
    case "mapValue":
      return { mapValue: { fields: {} } };
  }
}

/**
 * Validates a signed 64-bit integer decimal string using BigInt.
 * @param {string} str
 * @returns {{ valid: boolean, canonical?: string, error?: string }}
 */
export function validateInteger64(str) {
  const trimmed = str.trim();
  if (!/^-?\d+$/.test(trimmed)) {
    return { valid: false, error: "Must be a whole 64-bit integer (e.g. 42 or -10)" };
  }
  try {
    const n = BigInt(trimmed);
    if (n < I64_MIN || n > I64_MAX) {
      return {
        valid: false,
        error: "Out of int64 range (-9223372036854775808 .. 9223372036854775807)",
      };
    }
    return { valid: true, canonical: n.toString() };
  } catch {
    return { valid: false, error: "Invalid 64-bit integer" };
  }
}

/**
 * Formats a doubleValue from ProtoJSON into an editable string, preserving -0.0 and non-finite values.
 * @param {number | string | undefined} raw
 * @returns {string}
 */
export function formatDoubleInput(raw) {
  if (raw === "NaN" || raw === "Infinity" || raw === "-Infinity") {
    return raw;
  }
  if (typeof raw === "number") {
    if (Number.isNaN(raw)) return "NaN";
    if (raw === Infinity) return "Infinity";
    if (raw === -Infinity) return "-Infinity";
    if (Object.is(raw, -0)) return "-0.0";
    return String(raw);
  }
  return "0";
}

/**
 * Validates and parses a doubleValue input string into its ProtoJSON value.
 * @param {string} str
 * @returns {{ valid: boolean, value?: number | 'NaN' | 'Infinity' | '-Infinity', error?: string }}
 */
export function validateDouble(str) {
  const trimmed = str.trim();
  if (!trimmed) {
    return { valid: false, error: "Double value is required" };
  }
  if (trimmed === "NaN" || trimmed === "Infinity" || trimmed === "-Infinity") {
    return { valid: true, value: trimmed };
  }
  if (/^-0(\.0+)?$/.test(trimmed)) {
    return { valid: true, value: -0 };
  }
  if (!/^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/.test(trimmed)) {
    return {
      valid: false,
      error: "Invalid float (use number, NaN, Infinity, or -Infinity)",
    };
  }
  const n = Number(trimmed);
  if (Number.isNaN(n)) {
    return { valid: false, error: "Invalid float literal" };
  }
  if (n === Infinity) return { valid: true, value: "Infinity" };
  if (n === -Infinity) return { valid: true, value: "-Infinity" };
  return { valid: true, value: n };
}

/**
 * Validates an RFC 3339 timestamp string with up to 9 fractional nanosecond digits.
 * @param {string} str
 * @returns {{ valid: boolean, error?: string }}
 */
export function validateTimestamp(str) {
  const trimmed = str.trim();
  const rfc3339 =
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;
  if (!rfc3339.test(trimmed)) {
    return {
      valid: false,
      error: "Expected RFC 3339 timestamp (e.g. 2026-11-10T15:30:00Z)",
    };
  }
  const coarseIso = trimmed.replace(/(\.\d{3})\d+/, "$1");
  const ms = Date.parse(coarseIso);
  if (Number.isNaN(ms)) {
    return { valid: false, error: "Invalid calendar date/time in timestamp" };
  }
  return { valid: true };
}

/**
 * Validates a geographic coordinate pair.
 * @param {string | number} latInput
 * @param {string | number} lngInput
 * @returns {{ valid: boolean, latitude?: number, longitude?: number, error?: string }}
 */
export function validateGeoPoint(latInput, lngInput) {
  const latStr = String(latInput).trim();
  const lngStr = String(lngInput).trim();
  if (!latStr || !lngStr) {
    return { valid: false, error: "Latitude and longitude are required" };
  }
  const lat = Number(latStr);
  const lng = Number(lngStr);
  if (!Number.isFinite(lat) || lat < -90 || lat > 90) {
    return { valid: false, error: "Latitude must be between -90 and 90" };
  }
  if (!Number.isFinite(lng) || lng < -180 || lng > 180) {
    return { valid: false, error: "Longitude must be between -180 and 180" };
  }
  return { valid: true, latitude: lat, longitude: lng };
}

/**
 * Expands a relative document path (`users/alice`) into a full resource reference
 * and validates its structure.
 * @param {string} input
 * @param {string} projectId
 * @param {string} databaseId
 * @returns {{ valid: boolean, fullPath?: string, error?: string }}
 */
export function normalizeReference(input, projectId, databaseId) {
  const trimmed = input.trim().replace(/^\/+/, "");
  if (!trimmed) {
    return { valid: false, error: "Document reference path is required" };
  }
  const fullPath = trimmed.startsWith("projects/")
    ? trimmed
    : `projects/${projectId}/databases/${databaseId}/documents/${trimmed}`;

  const match =
    /^projects\/([^/]+)\/databases\/([^/]+)\/documents\/(.+)$/.exec(fullPath);
  if (!match) {
    return {
      valid: false,
      error:
        "Reference must be projects/{p}/databases/{d}/documents/{col}/{doc} or relative {col}/{doc}",
    };
  }
  const tailSegments = match[3].split("/");
  if (
    tailSegments.length < 2 ||
    tailSegments.length % 2 !== 0 ||
    tailSegments.some((s) => !s || s === "." || s === "..")
  ) {
    return {
      valid: false,
      error: "Document reference must have an even number of collection/document segments",
    };
  }
  return { valid: true, fullPath };
}

/**
 * Encodes user input ('base64' | 'utf8' | 'hex') into standard padded Base64.
 * @param {string} input
 * @param {'base64' | 'utf8' | 'hex'} mode
 * @returns {{ valid: boolean, base64?: string, error?: string }}
 */
export function encodeBytes(input, mode) {
  if (mode === "utf8") {
    const bytes = new TextEncoder().encode(input);
    return { valid: true, base64: uint8ToBase64(bytes) };
  }
  if (mode === "hex") {
    const clean = input.replace(/\s+/g, "");
    if (clean.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(clean)) {
      return { valid: false, error: "Hex string must have an even number of hex digits" };
    }
    const bytes = new Uint8Array(clean.length / 2);
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16);
    }
    return { valid: true, base64: uint8ToBase64(bytes) };
  }
  const cleanB64 = input.trim();
  const decoded = base64ToUint8(cleanB64);
  if (!decoded) {
    return { valid: false, error: "Invalid Base64 byte string" };
  }
  return { valid: true, base64: uint8ToBase64(decoded) };
}

/**
 * Decodes a Base64 string into the requested display mode ('base64' | 'utf8' | 'hex').
 * @param {string} base64Str
 * @param {'base64' | 'utf8' | 'hex'} mode
 * @returns {{ ok: boolean, text: string }}
 */
export function decodeBytes(base64Str, mode) {
  if (mode === "base64") {
    return { ok: true, text: base64Str };
  }
  const bytes = base64ToUint8(base64Str);
  if (!bytes) {
    return { ok: false, text: base64Str };
  }
  if (mode === "hex") {
    const hex = Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    return { ok: true, text: hex };
  }
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    return { ok: true, text };
  } catch {
    return { ok: false, text: base64Str };
  }
}

/**
 * @param {Uint8Array} bytes
 * @returns {string}
 */
function uint8ToBase64(bytes) {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

/**
 * @param {string} b64
 * @returns {Uint8Array | null}
 */
function base64ToUint8(b64) {
  try {
    const normalized = b64.replace(/-/g, "+").replace(/_/g, "/");
    const padded =
      normalized.length % 4 === 0
        ? normalized
        : normalized + "=".repeat(4 - (normalized.length % 4));
    const bin = atob(padded);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) {
      out[i] = bin.charCodeAt(i);
    }
    return out;
  } catch {
    return null;
  }
}

/**
 * Escapes a single Firestore field path segment for DocumentMask.fieldPaths.
 * @param {string} segment
 * @returns {string}
 */
export function escapeFieldPathSegment(segment) {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(segment)) {
    return segment;
  }
  const escaped = segment.replace(/\\/g, "\\\\").replace(/`/g, "\\`");
  return `\`${escaped}\``;
}

/**
 * Formats a compact, single-line human-readable summary of a FirestoreValue.
 * @param {FirestoreValue | undefined} val
 * @returns {string}
 */
export function formatValuePreview(val) {
  if (!val) return "null";
  const t = getValueType(val);
  switch (t) {
    case "nullValue":
      return "null";
    case "booleanValue":
      return String(Boolean(val.booleanValue));
    case "integerValue":
      return String(val.integerValue ?? "0");
    case "doubleValue":
      return formatDoubleInput(val.doubleValue);
    case "stringValue":
      return JSON.stringify(val.stringValue ?? "");
    case "timestampValue":
      return val.timestampValue ?? "";
    case "referenceValue":
      return val.referenceValue ?? "";
    case "bytesValue":
      return `bytes(${val.bytesValue ?? ""})`;
    case "geoPointValue":
      return `(${val.geoPointValue?.latitude ?? 0}, ${val.geoPointValue?.longitude ?? 0})`;
    case "arrayValue":
      return `[${(val.arrayValue?.values ?? []).length} items]`;
    case "mapValue":
      return `{${Object.keys(val.mapValue?.fields ?? {}).length} fields}`;
  }
}

/**
 * Scans raw JSON text outside string literals to detect unquoted integer literals
 * exceeding Number.MAX_SAFE_INTEGER before JSON.parse silently truncates them.
 * @param {string} rawText
 * @returns {string | null}
 */
function findUnsafeUnquotedInteger(rawText) {
  let inString = false;
  let escaped = false;
  for (let i = 0; i < rawText.length; i++) {
    const ch = rawText[i];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === "-" || (ch >= "0" && ch <= "9")) {
      let j = i;
      if (rawText[j] === "-") j++;
      const digitStart = j;
      while (j < rawText.length && rawText[j] >= "0" && rawText[j] <= "9") {
        j++;
      }
      const isFloat =
        j < rawText.length &&
        (rawText[j] === "." || rawText[j] === "e" || rawText[j] === "E");
      const digits = rawText.slice(digitStart, j);
      if (!isFloat && digits.length >= 16) {
        try {
          const n = BigInt(rawText.slice(i, j));
          if (n > 9007199254740991n || n < -9007199254740991n) {
            return rawText.slice(i, j);
          }
        } catch {
          // Ignore malformed token here; JSON.parse will catch syntax errors.
        }
      }
      if (isFloat) {
        while (
          j < rawText.length &&
          /[0-9.eE+-]/.test(rawText[j])
        ) {
          j++;
        }
      }
      i = Math.max(i, j - 1);
    }
  }
  return null;
}

/**
 * Validates a raw ProtoJSON fields editor payload (`{ "fields": { ... } }` or `{ ... }`).
 * @param {string} rawText
 * @returns {{ valid: boolean, fields?: Record<string, FirestoreValue>, error?: string }}
 */
export function validateRawProtoJsonFields(rawText) {
  const unsafeLiteral = findUnsafeUnquotedInteger(rawText);
  if (unsafeLiteral) {
    return {
      valid: false,
      error: `Unquoted 64-bit integer ${unsafeLiteral} loses precision in JSON.parse; wrap integerValue in quotes: "${unsafeLiteral}"`,
    };
  }

  /** @type {unknown} */
  let parsed;
  try {
    parsed = JSON.parse(rawText);
  } catch (err) {
    return {
      valid: false,
      error: `JSON syntax error: ${err instanceof Error ? err.message : String(err)}`,
    };
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return {
      valid: false,
      error: 'Root JSON must be an object of the form { "fields": { ... } }',
    };
  }

  const rootObj = /** @type {Record<string, unknown>} */ (parsed);
  const candidateFields =
    Object.prototype.hasOwnProperty.call(rootObj, "fields") &&
    Object.keys(rootObj).every(
      (k) =>
        k === "fields" ||
        k === "name" ||
        k === "createTime" ||
        k === "updateTime"
    )
      ? rootObj.fields
      : rootObj;

  if (
    !candidateFields ||
    typeof candidateFields !== "object" ||
    Array.isArray(candidateFields)
  ) {
    return { valid: false, error: '"fields" must be a JSON object map' };
  }

  return normalizeFieldsMap(
    /** @type {Record<string, unknown>} */ (candidateFields),
    "fields"
  );
}

/**
 * @param {Record<string, unknown>} mapObj
 * @param {string} pathLabel
 * @returns {{ valid: boolean, fields?: Record<string, FirestoreValue>, error?: string }}
 */
function normalizeFieldsMap(mapObj, pathLabel) {
  /** @type {Record<string, FirestoreValue>} */
  const normalized = {};
  for (const [key, rawVal] of Object.entries(mapObj)) {
    if (!key) {
      return { valid: false, error: `${pathLabel}: field name cannot be empty` };
    }
    const res = normalizeProtoValue(rawVal, `${pathLabel}.${key}`, false);
    if (!res.valid || !res.value) {
      return { valid: false, error: res.error };
    }
    normalized[key] = res.value;
  }
  return { valid: true, fields: normalized };
}

/**
 * @param {unknown} rawVal
 * @param {string} pathLabel
 * @param {boolean} inArray
 * @returns {{ valid: boolean, value?: FirestoreValue, error?: string }}
 */
function normalizeProtoValue(rawVal, pathLabel, inArray) {
  if (!rawVal || typeof rawVal !== "object" || Array.isArray(rawVal)) {
    return {
      valid: false,
      error: `${pathLabel}: must be a Firestore Value object (e.g. { "stringValue": "..." })`,
    };
  }
  const keys = Object.keys(rawVal);
  if (keys.length !== 1) {
    return {
      valid: false,
      error: `${pathLabel}: Value object must have exactly one type key (found ${keys.length})`,
    };
  }
  const typeKey = /** @type {ValueTypeKey} */ (keys[0]);
  if (!VALID_TYPE_SET.has(typeKey)) {
    return {
      valid: false,
      error: `${pathLabel}: unsupported Value key "${keys[0]}"`,
    };
  }
  const inner = /** @type {Record<string, unknown>} */ (rawVal)[typeKey];

  switch (typeKey) {
    case "nullValue":
      if (inner !== null && inner !== "NULL_VALUE" && inner !== 0) {
        return { valid: false, error: `${pathLabel}.nullValue must be null` };
      }
      return { valid: true, value: { nullValue: null } };

    case "booleanValue":
      if (typeof inner !== "boolean") {
        return { valid: false, error: `${pathLabel}.booleanValue must be true or false` };
      }
      return { valid: true, value: { booleanValue: inner } };

    case "integerValue": {
      if (typeof inner === "number") {
        if (!Number.isSafeInteger(inner)) {
          return {
            valid: false,
            error: `${pathLabel}.integerValue must be a decimal string`,
          };
        }
        return { valid: true, value: { integerValue: String(inner) } };
      }
      if (typeof inner !== "string") {
        return {
          valid: false,
          error: `${pathLabel}.integerValue must be a decimal string`,
        };
      }
      const check = validateInteger64(inner);
      if (!check.valid || !check.canonical) {
        return { valid: false, error: `${pathLabel}: ${check.error}` };
      }
      return { valid: true, value: { integerValue: check.canonical } };
    }

    case "doubleValue": {
      if (typeof inner === "number") {
        return { valid: true, value: { doubleValue: inner } };
      }
      if (inner === "NaN" || inner === "Infinity" || inner === "-Infinity") {
        return { valid: true, value: { doubleValue: inner } };
      }
      return {
        valid: false,
        error: `${pathLabel}.doubleValue must be a number or "NaN"/"Infinity"/"-Infinity"`,
      };
    }

    case "stringValue":
      if (typeof inner !== "string") {
        return { valid: false, error: `${pathLabel}.stringValue must be a string` };
      }
      return { valid: true, value: { stringValue: inner } };

    case "timestampValue": {
      if (typeof inner !== "string") {
        return {
          valid: false,
          error: `${pathLabel}.timestampValue must be an RFC 3339 string`,
        };
      }
      const tsCheck = validateTimestamp(inner);
      if (!tsCheck.valid) {
        return { valid: false, error: `${pathLabel}: ${tsCheck.error}` };
      }
      return { valid: true, value: { timestampValue: inner.trim() } };
    }

    case "referenceValue":
      if (typeof inner !== "string" || !inner.startsWith("projects/")) {
        return {
          valid: false,
          error: `${pathLabel}.referenceValue must be "projects/{p}/databases/{d}/documents/..."`,
        };
      }
      return { valid: true, value: { referenceValue: inner } };

    case "bytesValue": {
      if (typeof inner !== "string") {
        return {
          valid: false,
          error: `${pathLabel}.bytesValue must be a Base64 string`,
        };
      }
      const b64 = encodeBytes(inner, "base64");
      if (!b64.valid || b64.base64 === undefined) {
        return { valid: false, error: `${pathLabel}: ${b64.error}` };
      }
      return { valid: true, value: { bytesValue: b64.base64 } };
    }

    case "geoPointValue": {
      if (!inner || typeof inner !== "object" || Array.isArray(inner)) {
        return {
          valid: false,
          error: `${pathLabel}.geoPointValue must be { latitude, longitude }`,
        };
      }
      const geoObj = /** @type {Record<string, unknown>} */ (inner);
      const geoCheck = validateGeoPoint(
        /** @type {number | string} */ (geoObj.latitude ?? 0),
        /** @type {number | string} */ (geoObj.longitude ?? 0)
      );
      if (!geoCheck.valid) {
        return { valid: false, error: `${pathLabel}: ${geoCheck.error}` };
      }
      return {
        valid: true,
        value: {
          geoPointValue: {
            latitude: geoCheck.latitude,
            longitude: geoCheck.longitude,
          },
        },
      };
    }

    case "arrayValue": {
      if (inArray) {
        return {
          valid: false,
          error: `${pathLabel}: nested arrays inside arrayValue are not supported in Firestore`,
        };
      }
      if (!inner || typeof inner !== "object" || Array.isArray(inner)) {
        return {
          valid: false,
          error: `${pathLabel}.arrayValue must be { "values": [...] }`,
        };
      }
      const arrObj = /** @type {Record<string, unknown>} */ (inner);
      const rawValues = arrObj.values ?? [];
      if (!Array.isArray(rawValues)) {
        return { valid: false, error: `${pathLabel}.arrayValue.values must be an array` };
      }
      /** @type {FirestoreValue[]} */
      const values = [];
      for (let i = 0; i < rawValues.length; i++) {
        const itemRes = normalizeProtoValue(rawValues[i], `${pathLabel}[${i}]`, true);
        if (!itemRes.valid || !itemRes.value) {
          return { valid: false, error: itemRes.error };
        }
        values.push(itemRes.value);
      }
      return { valid: true, value: { arrayValue: { values } } };
    }

    case "mapValue": {
      if (!inner || typeof inner !== "object" || Array.isArray(inner)) {
        return {
          valid: false,
          error: `${pathLabel}.mapValue must be { "fields": { ... } }`,
        };
      }
      const mapWrapper = /** @type {Record<string, unknown>} */ (inner);
      const rawFields = mapWrapper.fields ?? {};
      if (!rawFields || typeof rawFields !== "object" || Array.isArray(rawFields)) {
        return { valid: false, error: `${pathLabel}.mapValue.fields must be an object` };
      }
      const mapRes = normalizeFieldsMap(
        /** @type {Record<string, unknown>} */ (rawFields),
        pathLabel
      );
      if (!mapRes.valid || !mapRes.fields) {
        return { valid: false, error: mapRes.error };
      }
      return { valid: true, value: { mapValue: { fields: mapRes.fields } } };
    }
  }
}
