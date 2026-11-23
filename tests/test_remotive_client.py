import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import httpx

from app.clients.remotive_client import RemotiveAPIError, RemotiveClient


FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "remotive_feed.json"
)


class RemotiveClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.payload = json.loads(
            FIXTURE_PATH.read_text(encoding="utf-8")
        )

    async def test_fetch_jobs_uses_public_endpoint_and_category_parameter(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, json=self.payload)

        http_client = httpx.AsyncClient(
            base_url=RemotiveClient.BASE_URL,
            transport=httpx.MockTransport(handler),
        )
        client = RemotiveClient(http_client=http_client)

        try:
            payload = await client.fetch_jobs()
        finally:
            await http_client.aclose()

        self.assertEqual(payload, self.payload)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].method, "GET")
        self.assertEqual(
            str(requests[0].url),
            "https://remotive.com/api/remote-jobs?category=software-dev",
        )

    async def test_http_error_is_reported_clearly(self):
        def handler(request):
            return httpx.Response(503, request=request)

        http_client = httpx.AsyncClient(
            base_url=RemotiveClient.BASE_URL,
            transport=httpx.MockTransport(handler),
        )
        client = RemotiveClient(http_client=http_client)

        try:
            with self.assertRaisesRegex(RemotiveAPIError, "HTTP 503"):
                await client.fetch_jobs()
        finally:
            await http_client.aclose()

    async def test_invalid_json_is_reported_clearly(self):
        def handler(request):
            return httpx.Response(
                200,
                content=b"not-json",
                headers={"content-type": "application/json"},
                request=request,
            )

        http_client = httpx.AsyncClient(
            base_url=RemotiveClient.BASE_URL,
            transport=httpx.MockTransport(handler),
        )
        client = RemotiveClient(http_client=http_client)

        try:
            with self.assertRaisesRegex(RemotiveAPIError, "invalid JSON"):
                await client.fetch_jobs()
        finally:
            await http_client.aclose()

    async def test_injected_http_client_is_not_closed_by_wrapper(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = self.payload
        http_client = Mock()
        http_client.get = AsyncMock(return_value=response)
        http_client.aclose = AsyncMock()

        client = RemotiveClient(http_client=http_client)
        await client.fetch_jobs()
        await client.aclose()

        http_client.aclose.assert_not_awaited()

    async def test_wrapper_closes_http_client_it_created(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = self.payload
        http_client = Mock()
        http_client.get = AsyncMock(return_value=response)
        http_client.aclose = AsyncMock()

        with patch(
            "app.clients.remotive_client.httpx.AsyncClient",
            return_value=http_client,
        ):
            client = RemotiveClient()
            await client.fetch_jobs()
            await client.aclose()

        http_client.aclose.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
