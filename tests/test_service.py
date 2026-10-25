import unittest
from datetime import datetime

from pydantic import ValidationError

from app.schemas import CreateLinkRequest


class CreateLinkRequestTests(unittest.TestCase):
    def test_valid_http_url_and_custom_code(self):
        request = CreateLinkRequest(url="https://example.com/path", custom_code="hello_42")
        self.assertEqual(str(request.url), "https://example.com/path")
        self.assertEqual(request.custom_code, "hello_42")

    def test_rejects_custom_code_with_path_characters(self):
        with self.assertRaises(ValidationError):
            CreateLinkRequest(url="https://example.com", custom_code="../admin")

    def test_rejects_timezone_naive_expiration(self):
        with self.assertRaises(ValidationError):
            CreateLinkRequest(
                url="https://example.com",
                expires_at=datetime(2030, 1, 1),
            )

    def test_rejects_non_http_url(self):
        with self.assertRaises(ValidationError):
            CreateLinkRequest(url="javascript:alert(1)")

    def test_rejects_credentials_embedded_in_destination(self):
        with self.assertRaises(ValidationError):
            CreateLinkRequest(url="https://user:password@example.com/private")


if __name__ == "__main__":
    unittest.main()
