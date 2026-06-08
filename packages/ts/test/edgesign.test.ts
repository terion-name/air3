import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import test from 'node:test';

import {
  canonicalString,
  RangeMismatchError,
  signUrl,
  UnsignedRangeError,
  verifyUrl,
} from '../src/index.ts';

interface Vector {
  name: string;
  input: {
    method: string;
    baseUrl: string;
    bucket: string;
    key: string;
    secret: string;
    expires: number;
    range?: string;
    responseContentType?: string;
    responseContentDisposition?: string;
  };
  now: number;
  canonical: string;
  url: string;
  verifyRange?: string;
}

const here = dirname(fileURLToPath(import.meta.url));
const vectors = JSON.parse(readFileSync(resolve(here, '../../testdata/edge-signing-vectors.json'), 'utf8')) as Vector[];

test('shared edge signing vectors', () => {
  for (const vector of vectors) {
    const raw = signUrl({
      method: vector.input.method,
      baseUrl: vector.input.baseUrl,
      bucket: vector.input.bucket,
      key: vector.input.key,
      secret: vector.input.secret,
      expires: vector.input.expires,
      range: vector.input.range,
      responseContentType: vector.input.responseContentType,
      responseContentDisposition: vector.input.responseContentDisposition,
    });
    assert.equal(raw, vector.url, vector.name);

    const claims = verifyUrl({
      method: vector.input.method,
      url: raw,
      secret: vector.input.secret,
      now: vector.now,
      range: vector.verifyRange,
    });
    assert.equal(canonicalString(claims), vector.canonical, vector.name);
  }
});

test('range header must be covered by the signature', () => {
  const raw = signUrl({
    method: 'GET',
    baseUrl: 'https://files.example.com',
    bucket: 'demo-bucket',
    key: 'object.txt',
    secret: 'secret',
    expires: 1780938000,
  });
  assert.throws(
    () => verifyUrl({ method: 'GET', url: raw, secret: 'secret', now: 1780934400, range: 'bytes=0-9' }),
    UnsignedRangeError,
  );

  const ranged = signUrl({
    method: 'GET',
    baseUrl: 'https://files.example.com',
    bucket: 'demo-bucket',
    key: 'object.txt',
    secret: 'secret',
    expires: 1780938000,
    range: 'bytes=0-9',
  });
  assert.throws(
    () => verifyUrl({ method: 'GET', url: ranged, secret: 'secret', now: 1780934400, range: 'bytes=1-9' }),
    RangeMismatchError,
  );
});
