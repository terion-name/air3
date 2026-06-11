from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import unittest
from urllib.parse import urlsplit

from edgesign import (
    EdgeSignError,
    InvalidSignatureError,
    RangeMismatchError,
    UnsignedRangeError,
    canonical_string,
    sign_url,
    verify_url,
)


class EdgeSignVectorTests(unittest.TestCase):
    def test_shared_vectors(self) -> None:
        for vector in _vectors():
            with self.subTest(vector=vector["name"]):
                raw = sign_url(
                    method=vector["input"]["method"],
                    base_url=vector["input"]["baseUrl"],
                    bucket=vector["input"]["bucket"],
                    key=vector["input"]["key"],
                    secret=vector["input"]["secret"],
                    expires=vector["input"]["expires"],
                    range=vector["input"].get("range", ""),
                    response_content_type=vector["input"].get("responseContentType", ""),
                    response_content_disposition=vector["input"].get("responseContentDisposition", ""),
                )
                self.assertEqual(raw, vector["url"])

                claims = verify_url(
                    method=vector["input"]["method"],
                    url=raw,
                    secret=vector["input"]["secret"],
                    now=vector["now"],
                    range=vector.get("verifyRange", ""),
                )
                self.assertEqual(canonical_string(claims), vector["canonical"])

    def test_range_header_must_be_covered_by_signature(self) -> None:
        now = datetime.fromtimestamp(1780934400, tz=timezone.utc)
        raw = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo-bucket",
            key="object.txt",
            secret="secret",
            expires=now + timedelta(minutes=1),
        )
        with self.assertRaises(UnsignedRangeError):
            verify_url(method="GET", url=raw, secret="secret", now=now, range="bytes=0-9")

        ranged = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo-bucket",
            key="object.txt",
            secret="secret",
            expires=now + timedelta(minutes=1),
            range="bytes=0-9",
        )
        with self.assertRaises(RangeMismatchError):
            verify_url(method="GET", url=ranged, secret="secret", now=now, range="bytes=1-9")

    def test_multi_server_url_includes_server_claim_and_canonical_string(self) -> None:
        now = datetime.fromtimestamp(1780934400, tz=timezone.utc)
        prefixed_raw = sign_url(
            method="GET",
            base_url="https://files.example.com/prefix/api?tenant=acme",
            bucket="demo-bucket",
            key="dir/object.txt",
            server="origin-a",
            secret="secret",
            expires=now + timedelta(minutes=1),
        )

        parsed = urlsplit(prefixed_raw)
        self.assertEqual(parsed.path, "/prefix/api/origin-a/demo-bucket/dir/object.txt")
        self.assertIn("tenant=acme", parsed.query)

        raw = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo-bucket",
            key="dir/object.txt",
            server="origin-a",
            secret="secret",
            expires=now + timedelta(minutes=1),
        )
        claims = verify_url(method="GET", url=raw, secret="secret", now=now, server="origin-a")
        self.assertEqual(claims.server, "origin-a")
        self.assertEqual(
            canonical_string(claims),
            "GET\norigin-a\ndemo-bucket\ndir/object.txt\n1780934460\n\n\n",
        )

    def test_multi_server_default_bucket_short_path_signs_real_bucket(self) -> None:
        now = datetime.fromtimestamp(1780934400, tz=timezone.utc)
        raw = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo",
            key="file.txt",
            server="blue",
            secret="secret",
            expires=now + timedelta(minutes=1),
            default_bucket_path=True,
        )

        self.assertEqual(urlsplit(raw).path, "/blue/file.txt")
        claims = verify_url(
            method="GET",
            url=raw,
            secret="secret",
            now=now,
            server="blue",
            default_bucket="demo",
        )
        self.assertEqual(claims.server, "blue")
        self.assertEqual(claims.bucket, "demo")
        self.assertEqual(claims.key, "file.txt")
        self.assertEqual(
            canonical_string(claims),
            "GET\nblue\ndemo\nfile.txt\n1780934460\n\n\n",
        )

        with self.assertRaises(EdgeSignError):
            verify_url(method="GET", url=raw, secret="secret", now=now, server="blue")

    def test_single_server_default_bucket_short_path_signs_real_bucket(self) -> None:
        now = datetime.fromtimestamp(1780934400, tz=timezone.utc)
        raw = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo",
            key="file.txt",
            secret="secret",
            expires=now + timedelta(minutes=1),
            default_bucket_path=True,
        )

        self.assertEqual(urlsplit(raw).path, "/file.txt")
        claims = verify_url(
            method="GET",
            url=raw,
            secret="secret",
            now=now,
            default_bucket="demo",
        )
        self.assertEqual(claims.server, "")
        self.assertEqual(claims.bucket, "demo")
        self.assertEqual(claims.key, "file.txt")
        self.assertEqual(
            canonical_string(claims),
            "GET\ndemo\nfile.txt\n1780934460\n\n\n",
        )

        with self.assertRaises(EdgeSignError):
            verify_url(method="GET", url=raw, secret="secret", now=now)

    def test_single_server_default_bucket_treats_whole_path_as_key(self) -> None:
        now = datetime.fromtimestamp(1780934400, tz=timezone.utc)
        raw = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo",
            key="demo/file.txt",
            secret="secret",
            expires=now + timedelta(minutes=1),
            default_bucket_path=True,
        )

        self.assertEqual(urlsplit(raw).path, "/demo/file.txt")
        claims = verify_url(
            method="GET",
            url=raw,
            secret="secret",
            now=now,
            default_bucket="demo",
        )
        self.assertEqual(claims.bucket, "demo")
        self.assertEqual(claims.key, "demo/file.txt")

    def test_multi_server_tamper_fails_signature_validation(self) -> None:
        now = datetime.fromtimestamp(1780934400, tz=timezone.utc)
        raw = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo-bucket",
            key="object.txt",
            server="origin-a",
            secret="secret",
            expires=now + timedelta(minutes=1),
        )
        tampered = raw.replace("/origin-a/", "/origin-b/", 1)

        with self.assertRaises(InvalidSignatureError):
            verify_url(method="GET", url=tampered, secret="secret", now=now, server="origin-b")

    def test_multi_server_expected_server_mismatch_fails_signature_validation(self) -> None:
        now = datetime.fromtimestamp(1780934400, tz=timezone.utc)
        raw = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo-bucket",
            key="object.txt",
            server="origin-a",
            secret="secret",
            expires=now + timedelta(minutes=1),
        )

        with self.assertRaises(InvalidSignatureError):
            verify_url(method="GET", url=raw, secret="secret", now=now, server="origin-b")

    def test_no_server_uses_legacy_canonical_string(self) -> None:
        now = datetime.fromtimestamp(1780934400, tz=timezone.utc)
        raw = sign_url(
            method="GET",
            base_url="https://files.example.com",
            bucket="demo-bucket",
            key="object.txt",
            secret="secret",
            expires=now + timedelta(minutes=1),
        )

        self.assertEqual(urlsplit(raw).path, "/demo-bucket/object.txt")
        claims = verify_url(method="GET", url=raw, secret="secret", now=now)
        self.assertEqual(claims.server, "")
        self.assertEqual(canonical_string(claims), "GET\ndemo-bucket\nobject.txt\n1780934460\n\n\n")


def _vectors() -> list[dict]:
    path = Path(__file__).resolve().parents[2] / "testdata" / "edge-signing-vectors.json"
    return json.loads(path.read_text())


if __name__ == "__main__":
    unittest.main()
