from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import unittest

from edgesign import (
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


def _vectors() -> list[dict]:
    path = Path(__file__).resolve().parents[2] / "testdata" / "edge-signing-vectors.json"
    return json.loads(path.read_text())


if __name__ == "__main__":
    unittest.main()
