import json
import unittest
from datetime import UTC, datetime, timedelta

from aiokafka.errors import KafkaError
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from app.main import redirect


class FakeRedis:
    def __init__(self, value):
        self.value = value

    async def get(self, _):
        return self.value


class FakeDatabase:
    def __init__(self, row=None):
        self.row = row

    async def fetchrow(self, *_):
        return self.row


class FakeProducer:
    def __init__(self, error=None):
        self.error = error
        self.events = []

    async def send_and_wait(self, topic, event):
        if self.error:
            raise self.error
        self.events.append((topic, event))


def make_request(*, cached=None, row=None, producer=None):
    app = FastAPI()
    app.state.redis = FakeRedis(cached)
    app.state.db = FakeDatabase(row)
    app.state.producer = producer or FakeProducer()
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/short",
        "raw_path": b"/short",
        "query_string": b"",
        "headers": [(b"host", b"testserver")],
        "server": ("testserver", 80),
        "client": ("test-client", 12345),
        "root_path": "",
        "app": app,
    }
    return Request(scope), app.state.producer


class RedirectTests(unittest.IsolatedAsyncioTestCase):
    async def test_publishes_durable_event_before_redirect(self):
        cached = json.dumps({"destination": "https://destination.test/path", "expires_at": None})
        producer = FakeProducer()
        request, _ = make_request(cached=cached, producer=producer)
        response = await redirect("abc123", request)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["location"], "https://destination.test/path")
        self.assertEqual(len(producer.events), 1)
        self.assertEqual(producer.events[0][0], "redirect-events")
        self.assertEqual(producer.events[0][1]["code"], "abc123")

    async def test_returns_503_instead_of_untracked_redirect_when_kafka_fails(self):
        cached = json.dumps({"destination": "https://destination.test", "expires_at": None})
        producer = FakeProducer(KafkaError("broker unavailable"))
        request, _ = make_request(cached=cached, producer=producer)
        with self.assertRaises(HTTPException) as error:
            await redirect("abc123", request)
        self.assertEqual(error.exception.status_code, 503)

    async def test_expired_link_is_gone_and_does_not_publish(self):
        expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        cached = json.dumps({"destination": "https://destination.test", "expires_at": expired})
        producer = FakeProducer()
        request, _ = make_request(cached=cached, producer=producer)
        with self.assertRaises(HTTPException) as error:
            await redirect("abc123", request)
        self.assertEqual(error.exception.status_code, 410)
        self.assertEqual(producer.events, [])


if __name__ == "__main__":
    unittest.main()
