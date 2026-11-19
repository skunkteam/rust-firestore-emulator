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

const MIN_TIMESTAMP_SECONDS = -62135596800; // 0001-01-01T00:00:00Z
const MAX_TIMESTAMP_SECONDS = 253402300799; // 9999-12-31T23:59:59Z
const DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/**
 * @param {number} year
 * @param {number} month
 * @returns {number}
 */
function daysInGregorianMonth(year, month) {
  if (month === 2) {
    const isLeap = (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
    return isLeap ? 29 : 28;
  }
  return DAYS_IN_MONTH[month - 1];
}

/**
 * Validates an RFC 3339 timestamp string with up to 9 fractional nanosecond digits.
 * @param {string} str
 * @returns {{ valid: boolean, error?: string }}
 */
export function validateTimestamp(str) {
  const trimmed = str.trim();
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(Z|([+-])(\d{2}):(\d{2}))$/.exec(
      trimmed
    );
  if (!match) {
    return {
      valid: false,
      error: "Expected RFC 3339 timestamp (e.g. 2026-11-10T15:30:00Z)",
    };
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);

  if (year < 1 || year > 9999 || month < 1 || month > 12) {
    return { valid: false, error: "Invalid calendar date/time in timestamp" };
  }
  if (day < 1 || day > daysInGregorianMonth(year, month)) {
    return { valid: false, error: "Invalid calendar date/time in timestamp" };
  }
  if (hour > 23 || minute > 59 || second > 59) {
    return { valid: false, error: "Invalid calendar date/time in timestamp" };
  }

  let offsetSeconds = 0;
  if (match[7] !== "Z") {
    const sign = match[8] === "-" ? -1 : 1;
    const offHour = Number(match[9]);
    const offMinute = Number(match[10]);
    if (offHour > 23 || offMinute > 59) {
      return { valid: false, error: "Invalid timezone offset in timestamp" };
    }
    offsetSeconds = sign * (offHour * 3600 + offMinute * 60);
  }

  // Use setUTCFullYear so years 0001..0099 are not shifted to 1901..1999 by Date.UTC.
  const utcDate = new Date(0);
  utcDate.setUTCFullYear(year, month - 1, day);
  utcDate.setUTCHours(hour, minute, second, 0);
  const utcSeconds = Math.floor(utcDate.getTime() / 1000) - offsetSeconds;
  if (
    Number.isNaN(utcSeconds) ||
    utcSeconds < MIN_TIMESTAMP_SECONDS ||
    utcSeconds > MAX_TIMESTAMP_SECONDS
  ) {
    return {
      valid: false,
      error:
        "Timestamp must be between 0001-01-01T00:00:00Z and 9999-12-31T23:59:59.999999999Z",
    };
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
  const latStr =
    typeof latInput === "number" && Object.is(latInput, -0)
      ? "-0.0"
      : String(latInput).trim();
  const lngStr =
    typeof lngInput === "number" && Object.is(lngInput, -0)
      ? "-0.0"
      : String(lngInput).trim();
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
 * Returns true if `val` is a Cloud Firestore vector sentinel map
 * (`{ mapValue: { fields: { __type__: { stringValue: "__vector__" }, value: { arrayValue: { values: [...] } } } } }`)
 * with 1..2048 non-NaN double coordinates.
 * @param {FirestoreValue | null | undefined} val
 * @returns {boolean}
 */
export function isVectorMapValue(val) {
  const fields = val?.mapValue?.fields;
  if (!fields) return false;
  if (Object.keys(fields).length !== 2) return false;
  if (fields.__type__?.stringValue !== "__vector__") return false;
  const values = fields.value?.arrayValue?.values;
  if (!Array.isArray(values) || values.length < 1 || values.length > 2048) {
    return false;
  }
  return values.every((el) => {
    const d = el?.doubleValue;
    return (
      (typeof d === "number" && !Number.isNaN(d)) ||
      d === "Infinity" ||
      d === "-Infinity"
    );
  });
}

/**
 * Formats a compact one-line summary for a vector sentinel map value.
 * @param {FirestoreValue | null | undefined} val
 * @returns {string}
 */
export function formatVectorSummary(val) {
  const values = val?.mapValue?.fields?.value?.arrayValue?.values ?? [];
  const n = values.length;
  const head = values
    .slice(0, 3)
    .map((el) => formatDoubleInput(el?.doubleValue))
    .join(", ");
  return `vector (${n} dims) [${head}${n > 3 ? ", …" : ""}]`;
}

/**
 * Checks structural equality between two FirestoreValue ProtoJSON objects.
 * Uses Object.is for numeric doubleValue comparison (preserving -0 vs +0) and
 * compares mapValue fields independently of key insertion order.
 * @param {FirestoreValue | null | undefined} a
 * @param {FirestoreValue | null | undefined} b
 * @returns {boolean}
 */
export function firestoreValueEquals(a, b) {
  if (a === b) return true;
  if (!a || !b) return false;
  const ta = getValueType(a);
  const tb = getValueType(b);
  if (ta !== tb) return false;
  switch (ta) {
    case "nullValue":
      return true;
    case "booleanValue":
      return Boolean(a.booleanValue) === Boolean(b.booleanValue);
    case "integerValue":
      return (a.integerValue ?? "0") === (b.integerValue ?? "0");
    case "doubleValue":
      return typeof a.doubleValue === "number" && typeof b.doubleValue === "number"
        ? Object.is(a.doubleValue, b.doubleValue)
        : a.doubleValue === b.doubleValue;
    case "stringValue":
      return (a.stringValue ?? "") === (b.stringValue ?? "");
    case "timestampValue":
      return (a.timestampValue ?? "") === (b.timestampValue ?? "");
    case "referenceValue":
      return (a.referenceValue ?? "") === (b.referenceValue ?? "");
    case "bytesValue":
      return (a.bytesValue ?? "") === (b.bytesValue ?? "");
    case "geoPointValue": {
      const ga = a.geoPointValue;
      const gb = b.geoPointValue;
      return (
        Object.is(ga?.latitude ?? 0, gb?.latitude ?? 0) &&
        Object.is(ga?.longitude ?? 0, gb?.longitude ?? 0)
      );
    }
    case "arrayValue": {
      const va = a.arrayValue?.values ?? [];
      const vb = b.arrayValue?.values ?? [];
      if (va.length !== vb.length) return false;
      for (let i = 0; i < va.length; i++) {
        if (!firestoreValueEquals(va[i], vb[i])) return false;
      }
      return true;
    }
    case "mapValue": {
      const fa = a.mapValue?.fields ?? {};
      const fb = b.mapValue?.fields ?? {};
      const ka = Object.keys(fa);
      const kb = Object.keys(fb);
      if (ka.length !== kb.length) return false;
      for (const k of ka) {
        if (!Object.prototype.hasOwnProperty.call(fb, k)) return false;
        if (!firestoreValueEquals(fa[k], fb[k])) return false;
      }
      return true;
    }
  }
}

/**
 * Formats a compact, single-line human-readable summary of a FirestoreValue.
 * @param {FirestoreValue | undefined} val
 * @returns {string}
 */
export function formatValuePreview(val) {
  if (!val) return "null";
  if (isVectorMapValue(val)) {
    return formatVectorSummary(val);
  }
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
    setFieldProperty(normalized, key, res.value);
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
        if (!Number.isFinite(inner)) {
          return {
            valid: false,
            error: `${pathLabel}.doubleValue overflows IEEE-754 double; use "Infinity" or "-Infinity" as a string`,
          };
        }
        return { valid: true, value: { doubleValue: inner } };
      }
      if (inner === "NaN" || inner === "Infinity" || inner === "-Infinity") {
        return { valid: true, value: { doubleValue: inner } };
      }
      return {
        valid: false,
        error: `${pathLabel}.doubleValue must be a finite number or "NaN"/"Infinity"/"-Infinity"`,
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

/**
 * Defines an own enumerable field property safely, including `"__proto__"`.
 * @template T
 * @param {Record<string, T>} target
 * @param {string} key
 * @param {T} value
 */
export function setFieldProperty(target, key, value) {
  Object.defineProperty(target, key, {
    value,
    enumerable: true,
    writable: true,
    configurable: true,
  });
}

/**
 * Serializes a value to JSON while preserving `-0` as `-0.0` (which `JSON.stringify`
 * would otherwise normalize to `0`).
 * @param {unknown} value
 * @param {number} [indent]
 * @returns {string}
 */
export function stringifyProtoJson(value, indent = 0) {
  return formatProtoJsonValue(value, indent, 0) ?? "null";
}

/**
 * @param {unknown} value
 * @param {number} indent
 * @param {number} depth
 * @returns {string | undefined}
 */
function formatProtoJsonValue(value, indent, depth) {
  if (
    value !== null &&
    typeof value === "object" &&
    "toJSON" in value &&
    typeof /** @type {{ toJSON?: unknown }} */ (value).toJSON === "function"
  ) {
    value = /** @type {{ toJSON: () => unknown }} */ (value).toJSON();
  }
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") {
    if (Object.is(value, -0)) return "-0.0";
    return Number.isFinite(value) ? JSON.stringify(value) : "null";
  }
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value !== "object") return undefined;

  const step = indent > 0 ? " ".repeat(indent) : "";
  const nl = indent > 0 ? "\n" : "";
  const childPad = indent > 0 ? step.repeat(depth + 1) : "";
  const closePad = indent > 0 ? step.repeat(depth) : "";
  const sep = indent > 0 ? ": " : ":";

  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    const items = value.map(
      (item) =>
        `${childPad}${formatProtoJsonValue(item, indent, depth + 1) ?? "null"}`
    );
    return `[${nl}${items.join(`,${nl}`)}${nl}${closePad}]`;
  }

  const obj = /** @type {Record<string, unknown>} */ (value);
  /** @type {string[]} */
  const entries = [];
  for (const key of Object.keys(obj)) {
    const rendered = formatProtoJsonValue(obj[key], indent, depth + 1);
    if (rendered !== undefined) {
      entries.push(`${childPad}${JSON.stringify(key)}${sep}${rendered}`);
    }
  }
  if (entries.length === 0) return "{}";
  return `{${nl}${entries.join(`,${nl}`)}${nl}${closePad}}`;
}
