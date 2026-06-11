import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import test from 'node:test';

import {
  canonicalString,
  EdgeSignError,
  InvalidSignatureError,
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

test('multi-server signing inserts server after base path prefix', () => {
  const raw = signUrl({
    method: 'GET',
    baseUrl: 'https://files.example.com/public/root?existing=1',
    server: 'media_1',
    bucket: 'demo-bucket',
    key: 'dir/object.txt',
    secret: 'secret',
    expires: 1780938000,
  });

  const parsed = new URL(raw);
  assert.equal(parsed.pathname, '/public/root/media_1/demo-bucket/dir/object.txt');
  assert.equal(parsed.searchParams.get('existing'), '1');
});

test('multi-server verification returns server claim and canonical string includes it', () => {
  const raw = signUrl({
    method: 'GET',
    baseUrl: 'https://files.example.com',
    server: 'media-1',
    bucket: 'demo-bucket',
    key: 'dir/object.txt',
    secret: 'secret',
    expires: 1780938000,
  });

  const claims = verifyUrl({ method: 'GET', url: raw, server: 'media-1', secret: 'secret', now: 1780934400 });

  assert.equal(claims.server, 'media-1');
  assert.equal(claims.bucket, 'demo-bucket');
  assert.equal(claims.key, 'dir/object.txt');
  assert.equal(
    canonicalString(claims),
    ['GET', 'media-1', 'demo-bucket', 'dir/object.txt', '1780938000', '', '', ''].join('\n'),
  );
});

test('multi-server verification rejects tampered server path', () => {
  const raw = signUrl({
    method: 'GET',
    baseUrl: 'https://files.example.com',
    server: 'media-1',
    bucket: 'demo-bucket',
    key: 'object.txt',
    secret: 'secret',
    expires: 1780938000,
  });
  const tampered = raw.replace('/media-1/', '/media-2/');

  assert.throws(
    () => verifyUrl({ method: 'GET', url: tampered, server: 'media-2', secret: 'secret', now: 1780934400 }),
    InvalidSignatureError,
  );
});

test('multi-server verification rejects expected-server mismatch', () => {
  const raw = signUrl({
    method: 'GET',
    baseUrl: 'https://files.example.com',
    server: 'media-1',
    bucket: 'demo-bucket',
    key: 'object.txt',
    secret: 'secret',
    expires: 1780938000,
  });

  assert.throws(
    () => verifyUrl({ method: 'GET', url: raw, server: 'media-2', secret: 'secret', now: 1780934400 }),
    InvalidSignatureError,
  );
});

test('default bucket short path signs real bucket and verifies explicitly', () => {
  const raw = signUrl({
    method: 'GET',
    baseUrl: 'https://files.example.com',
    server: 'blue',
    bucket: 'demo-bucket',
    key: 'archive/object.txt',
    secret: 'secret',
    expires: 1780938000,
    defaultBucketPath: true,
  });

  const parsed = new URL(raw);
  assert.equal(parsed.pathname, '/blue/archive/object.txt');

  assert.throws(
    () => verifyUrl({ method: 'GET', url: raw, server: 'blue', secret: 'secret', now: 1780934400 }),
    InvalidSignatureError,
  );

  const claims = verifyUrl({
    method: 'GET',
    url: raw,
    server: 'blue',
    secret: 'secret',
    now: 1780934400,
    defaultBucket: 'demo-bucket',
  });
  assert.equal(claims.server, 'blue');
  assert.equal(claims.bucket, 'demo-bucket');
  assert.equal(claims.key, 'archive/object.txt');
  assert.equal(
    canonicalString(claims),
    ['GET', 'blue', 'demo-bucket', 'archive/object.txt', '1780938000', '', '', ''].join('\n'),
  );
});

test('default bucket path requires server', () => {
  assert.throws(
    () => signUrl({
      method: 'GET',
      baseUrl: 'https://files.example.com',
      bucket: 'demo-bucket',
      key: 'object.txt',
      secret: 'secret',
      expires: 1780938000,
      defaultBucketPath: true,
    }),
    EdgeSignError,
  );
});

test('empty server keeps legacy signing and verification behavior', () => {
  const input = {
    method: 'GET',
    baseUrl: 'https://files.example.com',
    bucket: 'demo-bucket',
    key: 'object.txt',
    secret: 'secret',
    expires: 1780938000,
  };
  const legacy = signUrl(input);
  const explicitEmpty = signUrl({ ...input, server: '' });

  assert.equal(explicitEmpty, legacy);

  const claims = verifyUrl({ method: 'GET', url: explicitEmpty, server: '', secret: 'secret', now: 1780934400 });
  assert.equal(claims.server, undefined);
  assert.equal(canonicalString(claims), ['GET', 'demo-bucket', 'object.txt', '1780938000', '', '', ''].join('\n'));
});
