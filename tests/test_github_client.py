import os
import unittest
from unittest.mock import AsyncMock,Mock,patch
from app.clients.github_client import GitHubClient,GitHubAPIError,GitHubRateLimitError
class R:
 def __init__(self,s,h=None,p=None):self.status_code=s;self.headers=h or {};self.p=p;self.request=Mock();self.request.url="https://api.github.com/x"
 def json(self):
  if isinstance(self.p,Exception):raise self.p
  return self.p
class H:
 def __init__(self,r):self.r=r;self.get=AsyncMock(return_value=r);self.aclose=AsyncMock()
class Tests(unittest.IsolatedAsyncioTestCase):
 async def test_304_etag(self):
  h=H(R(304,{"ETag":"x"}));g=GitHubClient("test",h);s,headers,p=await g.get("/x",etag="x")
  self.assertEqual(s,304);self.assertIsNone(p);self.assertEqual(h.get.await_args.kwargs["headers"],{"If-None-Match":"x"})
 async def test_json(self):
  g=GitHubClient("t",H(R(200,{"X-RateLimit-Remaining":"500"},{"id":1})))
  self.assertEqual((await g.get("/x"))[2],{"id":1})
 async def test_quota_floor(self):
  g=GitHubClient("t",H(R(200,{"X-RateLimit-Remaining":"99"},{})))
  with self.assertRaises(GitHubRateLimitError):await g.get("/x")
 async def test_403_rate_limit(self):
  g=GitHubClient("t",H(R(403,{"X-RateLimit-Remaining":"0"},{})))
  with self.assertRaises(GitHubRateLimitError):await g.get("/x")
 async def test_429(self):
  g=GitHubClient("t",H(R(429,{},{})))
  with self.assertRaises(GitHubRateLimitError):await g.get("/x")
 async def test_404(self):
  g=GitHubClient("t",H(R(404,{},{"message":"not found"})))
  with self.assertRaisesRegex(GitHubAPIError,"404"):await g.get("/x")
 async def test_bad_json(self):
  g=GitHubClient("t",H(R(200,{},ValueError())))
  with self.assertRaisesRegex(GitHubAPIError,"invalid JSON"):await g.get("/x")
 async def test_pat_is_read_from_dedicated_environment_variable(self):
  token="test-pat-value"
  with patch.dict(os.environ,{"SIGNAL_GITHUB_PAT":token,"GITHUB_TOKEN":"wrong-token"}):
   with patch("app.clients.github_client.httpx.AsyncClient") as factory:
    GitHubClient()
  headers=factory.call_args.kwargs["headers"]
  self.assertEqual(headers["Authorization"],f"Bearer {token}")

 async def test_token_is_redacted_from_provider_error_message(self):
  token="sensitive-pat-value"
  g=GitHubClient(token,H(R(403,{"X-RateLimit-Remaining":"500"},{"message":f"rejected {token}"})))
  with self.assertRaises(GitHubAPIError) as raised:
   await g.get("/x")
  self.assertNotIn(token,str(raised.exception))
  self.assertIn("[REDACTED]",str(raised.exception))

 async def test_injected_client_not_closed(self):
  h=H(R(200,{},{}));g=GitHubClient("t",h);await g.aclose();h.aclose.assert_not_awaited()
if __name__=="__main__":unittest.main()
