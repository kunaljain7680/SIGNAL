import json,tempfile,unittest
from unittest.mock import patch
from datetime import datetime,timezone
from pathlib import Path
from app.ingestion.connectors.github import GitHubConnector
from app.workers.run_once import run_source_once
from tests.test_ingestion_runner import _FakeStore

NOW=datetime(2026,11,24,12,tzinfo=timezone.utc)
class FakeClient:
 def __init__(self,fn):self.fn=fn;self.calls=[];self.closed=False
 async def get(self,path,params=None,etag=None):
  c={"path":path,"params":params,"etag":etag};self.calls.append(c);return self.fn(c)
 async def aclose(self):self.closed=True
def inv(status="approved",topic=None):
 return {"approval_status":status,"owner_review_required":status != "approved","technologies":[{"repositories":[{"owner":"python","name":"cpython"}],"topic":topic}]}
class Tests(unittest.IsolatedAsyncioTestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.p=Path(self.tmp.name)/"inv.json"
 def tearDown(self):self.tmp.cleanup()
 def make(self,cl,data=None):
  self.p.write_text(json.dumps(data or inv()),encoding="utf8")
  return GitHubConnector(self.p,lambda:cl,lambda:NOW)
 async def test_draft_inventory_refused(self):
  c=FakeClient(lambda _:None);g=self.make(c,inv("draft"))
  with self.assertRaisesRegex(RuntimeError,"draft"):await g.scopes()
  self.assertFalse(c.calls)
 async def test_draft_inventory_fails_runner_preflight_without_http(self):
  c=FakeClient(lambda _:self.fail("HTTP client should not be used for a draft inventory"))
  g=self.make(c,inv("draft"))
  store=_FakeStore(source_name="GitHub",source_type="github")
  with patch("app.workers.run_once.settings.SIGNAL_GITHUB_PAT","test-only-placeholder"):
   outcome=await run_source_once(
    "github",
    session_factory=store.factory,
    env=None,
    now_factory=lambda:NOW,
    connector=g,
    source_identity=("GitHub","github"),
   )
  self.assertEqual(outcome.status,"FAILED")
  self.assertIn("draft",outcome.error_message)
  run=store.runs[outcome.run_id]
  self.assertEqual(run.current_step,"PREFLIGHT")
  self.assertEqual(run.status,"FAILED")
  self.assertEqual(c.calls,[])
  self.assertEqual(store.events,{})
  self.assertEqual(store.source.cursor_state,{})

 async def test_missing_custom_pat_fails_preflight_before_inventory_or_http(self):
  c=FakeClient(lambda _:self.fail("HTTP client should not be used without a PAT"))
  g=self.make(c,inv("draft"))
  store=_FakeStore(source_name="GitHub",source_type="github")
  outcome=await run_source_once(
   "github",
   session_factory=store.factory,
   env={"GITHUB_TOKEN":"legacy-token-must-not-be-used"},
   now_factory=lambda:NOW,
   connector=g,
   source_identity=("GitHub","github"),
  )
  self.assertEqual(outcome.status,"FAILED")
  self.assertIn("SIGNAL_GITHUB_PAT",outcome.error_message)
  self.assertNotIn("legacy-token-must-not-be-used",outcome.error_message)
  self.assertEqual(store.runs[outcome.run_id].current_step,"PREFLIGHT")
  self.assertEqual(c.calls,[])

 async def test_connector_requires_dedicated_pat_variable(self):
  self.assertEqual(GitHubConnector.auth.required_env,("SIGNAL_GITHUB_PAT",))

 async def test_approved_inventory_still_fails_while_owner_review_required(self):
  d=inv("approved");d["owner_review_required"]=True
  c=FakeClient(lambda _:self.fail("HTTP must not be used for an unapproved inventory"))
  g=self.make(c,d)
  with self.assertRaisesRegex(RuntimeError,"owner-approved"):
   await g.scopes()
  self.assertEqual(c.calls,[])

 async def test_repositories_before_topics(self):
  d=inv(topic="python");d["technologies"].append({"repositories":[{"owner":"fastapi","name":"fastapi"}],"topic":"fastapi"})
  g=self.make(FakeClient(lambda _:None),d)
  self.assertEqual(await g.scopes(),["repo:python/cpython","repo:fastapi/fastapi","topic:python","topic:fastapi"])
 async def test_repo_snapshot_and_release_raw_events(self):
  raw={"id":12,"full_name":"python/cpython"};rel={"id":3,"body":"raw"}
  c=FakeClient(lambda q:(200,{"etag":"e1"},raw) if q["path"].endswith("/cpython") else (200,{"etag":"e2"},[rel]))
  g=self.make(c);await g.scopes();r=await g.fetch_since("repo:python/cpython",{})
  self.assertEqual([e.source_native_id for e in r.events],["snapshot:12:2026-11-24","release:3"])
  self.assertIs(r.events[0].raw_payload,raw);self.assertIs(r.events[1].raw_payload,rel)
  self.assertEqual(r.next_cursor["repository_etag"],"e1");self.assertEqual(r.next_cursor["releases_etag"],"e2");self.assertTrue(c.closed)
 async def test_304_returns_no_events(self):
  c=FakeClient(lambda q:(304,{},None));g=self.make(c);await g.scopes()
  r=await g.fetch_since("repo:python/cpython",{"repository_etag":"r","releases_etag":"l"})
  self.assertEqual(r.events,[])
  self.assertEqual(len(c.calls),2)
  self.assertEqual([call["etag"] for call in c.calls],["r","l"])
  self.assertEqual(r.next_cursor["repository_etag"],"r")
  self.assertEqual(r.next_cursor["releases_etag"],"l")
  self.assertEqual(r.next_cursor["last_checked_at"],NOW.isoformat())
 async def test_approved_seed_inventory_resolves_five_repo_scopes_and_no_topics(self):
  inventory_path=Path(__file__).resolve().parents[1]/"backend"/"app"/"ingestion"/"scopes"/"github_scopes.json"
  data=json.loads(inventory_path.read_text(encoding="utf-8"))
  data["approval_status"]="approved"
  data["owner_review_required"]=False
  client=FakeClient(lambda _:self.fail("Scope discovery must not make HTTP requests"))
  g=self.make(client,data)
  scopes=await g.scopes()
  self.assertEqual(len(scopes),5)
  self.assertTrue(all(scope.startswith("repo:") for scope in scopes))
  self.assertEqual(
   set(scopes),
   {
    "repo:python/cpython",
    "repo:fastapi/fastapi",
    "repo:django/django",
    "repo:pallets/flask",
    "repo:pytorch/pytorch",
   },
  )
  self.assertEqual(client.calls,[])

 async def test_empty_release_list_is_a_successful_repo_fetch(self):
  def response(call):
   if call["path"]=="/repos/python/cpython":
    return 200,{"etag":"repo-etag"},{"id":12,"full_name":"python/cpython"}
   return 200,{"etag":"release-etag"},[]
  client=FakeClient(response)
  g=self.make(client)
  await g.scopes()
  result=await g.fetch_since("repo:python/cpython",{})
  self.assertEqual([event.source_native_id for event in result.events],["snapshot:12:2026-11-24"])
  self.assertEqual(result.next_cursor["releases_etag"],"release-etag")

 async def test_moved_repository_fails_scope_without_fetching_releases(self):
  client=FakeClient(lambda _: (301,{},{"message":"repository moved"}))
  g=self.make(client)
  await g.scopes()
  with self.assertRaisesRegex(Exception,"301"):
   await g.fetch_since("repo:python/cpython",{})
  self.assertEqual(len(client.calls),1)

 async def test_topic_result(self):
  c=FakeClient(lambda q:(200,{},{"total_count":8,"incomplete_results":False,"items":[]}))
  g=self.make(c,inv(topic="python"));await g.scopes();r=await g.fetch_since("topic:python",{})
  self.assertEqual(r.events[0].source_native_id,"topic:python:2026-11-24")
  self.assertEqual(r.events[0].raw_payload,{"topic":"python","total_count":8})
 async def test_incomplete_topic_fails(self):
  c=FakeClient(lambda q:(200,{},{"total_count":1,"incomplete_results":True}))
  g=self.make(c,inv(topic="python"));await g.scopes()
  with self.assertRaisesRegex(RuntimeError,"incomplete"):await g.fetch_since("topic:python",{})
 async def test_invalid_topic_count_fails(self):
  c=FakeClient(lambda q:(200,{},{"total_count":"bad"}));g=self.make(c,inv(topic="python"));await g.scopes()
  with self.assertRaisesRegex(ValueError,"total_count"):await g.fetch_since("topic:python",{})
 async def test_scope_caps(self):
  d={"approval_status":"approved","owner_review_required":False,"technologies":[{"repositories":[],"topic":str(i)} for i in range(29)]}
  g=self.make(FakeClient(lambda _:None),d)
  with self.assertRaisesRegex(ValueError,"caps"):await g.scopes()
 async def test_unknown_scope(self):
  g=self.make(FakeClient(lambda _:None));await g.scopes()
  with self.assertRaisesRegex(ValueError,"Call scopes"):await g.fetch_since("repo:no/repo",{})
 async def test_release_shape_rejected(self):
  c=FakeClient(lambda q:(200,{},{"bad":1}) if q["path"].endswith("/releases") else (200,{},{"id":1}))
  g=self.make(c);await g.scopes()
  with self.assertRaisesRegex(ValueError,"list"):await g.fetch_since("repo:python/cpython",{})
if __name__=="__main__":unittest.main()
