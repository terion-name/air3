import { createHmac, timingSafeEqual } from 'node:crypto';
export class EdgeSignError extends Error {
    constructor(message) {
        super(message);
        this.name = 'EdgeSignError';
    }
}
export class InvalidSignatureError extends EdgeSignError {
    constructor() {
        super('invalid signature');
        this.name = 'InvalidSignatureError';
    }
}
export class ExpiredSignatureError extends EdgeSignError {
    constructor() {
        super('signature expired');
        this.name = 'ExpiredSignatureError';
    }
}
export class UnsignedRangeError extends EdgeSignError {
    constructor() {
        super('range header is not signed');
        this.name = 'UnsignedRangeError';
    }
}
export class RangeMismatchError extends EdgeSignError {
    constructor() {
        super('range header does not match signed range');
        this.name = 'RangeMismatchError';
    }
}
export function signUrl(input) {
    if (input.secret === '') {
        throw new EdgeSignError('signing secret is required');
    }
    const claims = claimsFromInput(input);
    const parsed = parseUrl(input.baseUrl, 'base url');
    if (parsed.protocol === 'edge-relative:') {
        throw new EdgeSignError('base url must be absolute');
    }
    const objectPath = appendObjectPath(decodePathname(parsed.pathname), claims.server ?? '', input.bucket, input.key);
    const query = collectQuery(parsed.searchParams);
    setQueryValue(query, 'expires', String(toUnixSeconds(input.expires)));
    if (input.range)
        setQueryValue(query, 'range', input.range);
    if (input.responseContentType)
        setQueryValue(query, 'response-content-type', input.responseContentType);
    if (input.responseContentDisposition) {
        setQueryValue(query, 'response-content-disposition', input.responseContentDisposition);
    }
    setQueryValue(query, 'sig', signatureHex(claims, input.secret));
    const origin = `${parsed.protocol}//${parsed.host}`;
    const encodedPath = escapeURLPath(objectPath);
    const encodedQuery = encodeQuery(query);
    return `${origin}${encodedPath}${encodedQuery ? `?${encodedQuery}` : ''}`;
}
export function verifyUrl(input) {
    const { claims, sig } = claimsFromUrl(input.method, input.url, input.server ?? '');
    if (input.secret === '') {
        throw new EdgeSignError('signing secret is required');
    }
    if (sig === '') {
        throw new InvalidSignatureError();
    }
    if (claims.expires.getTime() <= toUnixSeconds(input.now) * 1000) {
        throw new ExpiredSignatureError();
    }
    if (!constantTimeHexEqual(sig, signatureHex(claims, input.secret))) {
        throw new InvalidSignatureError();
    }
    const rangeHeader = (input.range ?? '').trim();
    if (rangeHeader !== '') {
        if (claims.range === '') {
            throw new UnsignedRangeError();
        }
        if (claims.range !== rangeHeader) {
            throw new RangeMismatchError();
        }
    }
    return claims;
}
export function canonicalString(claims) {
    const fields = [claims.method.toUpperCase()];
    if ((claims.server ?? '') !== '') {
        fields.push(claims.server ?? '');
    }
    fields.push(claims.bucket, claims.key, String(toUnixSeconds(claims.expires)), claims.range, claims.responseContentType, claims.responseContentDisposition);
    return fields.join('\n');
}
function claimsFromInput(input) {
    const method = normalizeMethod(input.method);
    const server = input.server ?? '';
    if (server !== '') {
        validateServerAlias(server);
    }
    validateBucket(input.bucket);
    validateKey(input.key);
    const expires = toUnixSeconds(input.expires);
    if (expires <= 0) {
        throw new EdgeSignError('expiration is required');
    }
    return {
        method,
        ...(server !== '' ? { server } : {}),
        bucket: input.bucket,
        key: input.key,
        expires: new Date(expires * 1000),
        range: input.range ?? '',
        responseContentType: input.responseContentType ?? '',
        responseContentDisposition: input.responseContentDisposition ?? '',
    };
}
function claimsFromUrl(method, rawUrl, expectedServer) {
    if (expectedServer !== '') {
        validateServerAlias(expectedServer);
    }
    const parsed = parseUrl(rawUrl, 'signed url');
    const [server, bucket, key] = expectedServer === ''
        ? ['', ...objectFromPath(parsed.pathname)]
        : objectFromServerPath(parsed.pathname);
    if (expectedServer !== '' && server !== expectedServer) {
        throw new InvalidSignatureError();
    }
    const expiresText = parsed.searchParams.get('expires') ?? '';
    if (expiresText === '') {
        throw new EdgeSignError('expires query parameter is required');
    }
    const expires = parseUnixSeconds(expiresText);
    const claims = claimsFromInput({
        method,
        baseUrl: 'https://edge.invalid',
        server,
        bucket,
        key,
        expires,
        secret: 'unused',
        range: parsed.searchParams.get('range') ?? '',
        responseContentType: parsed.searchParams.get('response-content-type') ?? '',
        responseContentDisposition: parsed.searchParams.get('response-content-disposition') ?? '',
    });
    return { claims, sig: parsed.searchParams.get('sig') ?? '' };
}
function normalizeMethod(method) {
    const normalized = method.trim().toUpperCase();
    if (normalized !== 'GET' && normalized !== 'HEAD') {
        throw new EdgeSignError('method must be GET or HEAD');
    }
    return normalized;
}
function signatureHex(claims, secret) {
    return createHmac('sha256', Buffer.from(secret, 'utf8')).update(canonicalString(claims), 'utf8').digest('hex');
}
function constantTimeHexEqual(suppliedHex, expectedHex) {
    if (!/^[0-9a-fA-F]*$/.test(suppliedHex)) {
        return false;
    }
    const supplied = Buffer.from(suppliedHex, 'hex');
    const expected = Buffer.from(expectedHex, 'hex');
    return supplied.length === expected.length && timingSafeEqual(supplied, expected);
}
function appendObjectPath(basePath, server, bucket, key) {
    const parts = [
        trimSlashes(basePath),
        ...(server !== '' ? [goPathEscape(server)] : []),
        goPathEscape(bucket),
        ...key.split('/').map(goPathEscape),
    ].filter((part) => part !== '');
    return `/${parts.join('/')}`;
}
function objectFromPath(escapedPath) {
    const cleaned = cleanPath(`/${escapedPath}`).replace(/^\/+/, '');
    if (cleaned === '' || cleaned === '.') {
        throw new EdgeSignError('signed url path must include bucket and key');
    }
    const slash = cleaned.indexOf('/');
    if (slash <= 0 || slash === cleaned.length - 1) {
        throw new EdgeSignError('signed url path must include bucket and key');
    }
    const bucket = pathUnescape(cleaned.slice(0, slash), 'decode bucket path');
    const key = pathUnescape(cleaned.slice(slash + 1), 'decode key path');
    return [bucket, key];
}
function objectFromServerPath(escapedPath) {
    const cleaned = cleanPath(`/${escapedPath}`).replace(/^\/+/, '');
    const firstSlash = cleaned.indexOf('/');
    const secondSlash = firstSlash < 0 ? -1 : cleaned.indexOf('/', firstSlash + 1);
    if (cleaned === '' || cleaned === '.' || firstSlash <= 0 || secondSlash <= firstSlash + 1 || secondSlash === cleaned.length - 1) {
        throw new EdgeSignError('signed url path must include server, bucket and key');
    }
    const server = pathUnescape(cleaned.slice(0, firstSlash), 'decode server path');
    validateServerAlias(server);
    const bucket = pathUnescape(cleaned.slice(firstSlash + 1, secondSlash), 'decode bucket path');
    const key = pathUnescape(cleaned.slice(secondSlash + 1), 'decode key path');
    return [server, bucket, key];
}
function cleanPath(pathname) {
    const rooted = pathname.startsWith('/') ? pathname : `/${pathname}`;
    const out = [];
    for (const part of rooted.split('/')) {
        if (part === '' || part === '.')
            continue;
        if (part === '..') {
            out.pop();
            continue;
        }
        out.push(part);
    }
    return `/${out.join('/')}`;
}
function goPathEscape(value) {
    return percentEncode(value, isGoPathSegmentSafe, '%20');
}
function escapeURLPath(value) {
    return percentEncode(value, isGoURLPathSafe, '%20');
}
function encodeQuery(values) {
    const parts = [];
    for (const key of [...values.keys()].sort()) {
        for (const value of values.get(key) ?? []) {
            parts.push(`${goQueryEscape(key)}=${goQueryEscape(value)}`);
        }
    }
    return parts.join('&');
}
function goQueryEscape(value) {
    return percentEncode(value, isGoQuerySafe, '+');
}
function percentEncode(value, isSafe, encodedSpace) {
    const bytes = Buffer.from(value, 'utf8');
    let out = '';
    for (const byte of bytes) {
        if (byte === 0x20) {
            out += encodedSpace;
        }
        else if (isSafe(byte)) {
            out += String.fromCharCode(byte);
        }
        else {
            out += `%${byte.toString(16).toUpperCase().padStart(2, '0')}`;
        }
    }
    return out;
}
function isAlphaNum(byte) {
    return (byte >= 0x30 && byte <= 0x39) || (byte >= 0x41 && byte <= 0x5a) || (byte >= 0x61 && byte <= 0x7a);
}
function isGoPathSegmentSafe(byte) {
    return isAlphaNum(byte) || '-._~$&+=:@'.includes(String.fromCharCode(byte));
}
function isGoURLPathSafe(byte) {
    return isAlphaNum(byte) || '-._~$&+,;=:@/'.includes(String.fromCharCode(byte));
}
function isGoQuerySafe(byte) {
    return isAlphaNum(byte) || '-._~'.includes(String.fromCharCode(byte));
}
function parseUrl(rawUrl, context) {
    try {
        if (/^[A-Za-z][A-Za-z0-9+.-]*:/.test(rawUrl)) {
            return new URL(rawUrl);
        }
        return new URL(rawUrl, 'edge-relative://edge.invalid');
    }
    catch (error) {
        throw new EdgeSignError(`parse ${context}: ${String(error)}`);
    }
}
function decodePathname(pathname) {
    return pathUnescape(pathname, 'decode base path');
}
function pathUnescape(value, context) {
    try {
        return decodeURIComponent(value);
    }
    catch (error) {
        throw new EdgeSignError(`${context}: ${String(error)}`);
    }
}
function collectQuery(searchParams) {
    const values = new Map();
    for (const [key, value] of searchParams) {
        const existing = values.get(key);
        if (existing)
            existing.push(value);
        else
            values.set(key, [value]);
    }
    return values;
}
function setQueryValue(values, key, value) {
    values.set(key, [value]);
}
function trimSlashes(value) {
    return value.replace(/^\/+|\/+$/g, '');
}
function toUnixSeconds(value) {
    return value instanceof Date ? Math.trunc(value.getTime() / 1000) : Math.trunc(value);
}
function parseUnixSeconds(text) {
    if (!/^[0-9]+$/.test(text)) {
        throw new EdgeSignError('expires query parameter must be unix seconds');
    }
    const n = Number(text);
    if (!Number.isSafeInteger(n) || n <= 0) {
        throw new EdgeSignError('expires query parameter must be positive');
    }
    return n;
}
function validateServerAlias(alias) {
    const bytes = Buffer.from(alias, 'utf8');
    if (alias === '') {
        throw new EdgeSignError('server alias: alias is empty');
    }
    if (bytes.length > 63) {
        throw new EdgeSignError('server alias: alias is too long');
    }
    if (!isAliasAlphaNum(bytes[0] ?? 0)) {
        throw new EdgeSignError('server alias: alias must start with an ASCII letter or digit');
    }
    for (let i = 1; i < bytes.length; i += 1) {
        const byte = bytes[i] ?? 0;
        if (!isAliasChar(byte)) {
            throw new EdgeSignError(`server alias: alias contains invalid character ${JSON.stringify(String.fromCharCode(byte))}`);
        }
    }
}
function isAliasChar(byte) {
    return isAliasAlphaNum(byte) || byte === 0x5f || byte === 0x2d;
}
function isAliasAlphaNum(byte) {
    return isAlphaNum(byte);
}
function validateBucket(bucket) {
    if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket)) {
        throw new EdgeSignError('invalid ticket: bucket must be a valid DNS-style S3 bucket name');
    }
    if (bucket.includes('..') || bucket.includes('.-') || bucket.includes('-.')) {
        throw new EdgeSignError('invalid ticket: bucket contains invalid dot/hyphen placement');
    }
    if (isIPv4Address(bucket) || isIPv6Address(bucket)) {
        throw new EdgeSignError('invalid ticket: bucket must not look like an IP address');
    }
}
function validateKey(key) {
    if (key === '') {
        throw new EdgeSignError('invalid ticket: key is required');
    }
    if (key.length > 1024) {
        throw new EdgeSignError('invalid ticket: key is too long');
    }
    if (key.startsWith('/') || key.endsWith('/')) {
        throw new EdgeSignError('invalid ticket: key must not start or end with slash');
    }
    for (const part of key.split('/')) {
        if (part === '' || part === '.' || part === '..') {
            throw new EdgeSignError('invalid ticket: key must not contain empty or traversal path segments');
        }
    }
    for (const ch of key) {
        const code = ch.codePointAt(0) ?? 0;
        if (code === 0 || (code < 32) || (code >= 127 && code <= 159)) {
            throw new EdgeSignError('invalid ticket: key must not contain control characters');
        }
    }
}
function isIPv4Address(value) {
    const parts = value.split('.');
    return parts.length === 4 && parts.every((part) => /^\d+$/.test(part) && Number(part) >= 0 && Number(part) <= 255);
}
function isIPv6Address(value) {
    return value.includes(':');
}
