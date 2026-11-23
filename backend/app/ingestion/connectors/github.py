import json, logging
from datetime import datetime,timezone
from pathlib import Path
from typing import Any,Callable
from app.clients.github_client import GitHubAPIError,GitHubClient
from app.ingestion.contracts import AuthConfig,Cursor,FetchResult,RateLimitConfig,RawEvent

logger=logging.getLogger(__name__)
INVENTORY_PATH=Path(__file__).resolve().parents[1]/"scopes"/"github_scopes.json"

class GitHubConnector:
    name="github"
    rate_limit=RateLimitConfig(min_run_interval_s=86400,min_request_interval_s=2.0)
    auth=AuthConfig(required_env=("GITHUB_TOKEN",))
    def __init__(self,inventory_path:Path=INVENTORY_PATH,client_factory:Callable[[],GitHubClient]=GitHubClient,now_factory:Callable[[],datetime]|None=None):
        self.path=inventory_path;self.client_factory=client_factory;self.now=now_factory or (lambda:datetime.now(timezone.utc))
        self.repositories={};self.topics={}
    def _inventory(self):
        try: data=json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError,json.JSONDecodeError) as e: raise RuntimeError(f"Cannot load GitHub scope inventory: {e}") from e
        if not isinstance(data,dict) or data.get("approval_status")!="approved":
            raise RuntimeError("GitHub scope inventory is still a draft; owner review is required before live fetching.")
        techs=data.get("technologies")
        if not isinstance(techs,list): raise ValueError("GitHub technologies must be a list.")
        repos={};topics={}
        for t in techs:
            if not isinstance(t,dict): raise ValueError("Technology scope must be an object.")
            for r in t.get("repositories",[]):
                if not isinstance(r,dict) or not r.get("owner") or not r.get("name"): raise ValueError("Invalid repository scope.")
                key=f"repo:{r['owner']}/{r['name']}";repos[key]={"owner":r["owner"],"name":r["name"]}
            topic=t.get("topic")
            if topic: topics[f"topic:{topic}"]=topic
        if len(repos)>60 or len(topics)>28: raise ValueError("GitHub inventory exceeds architecture caps.")
        return repos,topics
    async def scopes(self):
        self.repositories,self.topics=self._inventory()
        return list(self.repositories)+list(self.topics)
    async def fetch_since(self,scope_key:str,cursor:Cursor)->FetchResult:
        if not isinstance(cursor,dict): raise ValueError("Cursor must be a JSON object.")
        if scope_key not in self.repositories and scope_key not in self.topics: raise ValueError(f"Unknown GitHub scope '{scope_key}'. Call scopes() first.")
        now=self.now()
        if now.tzinfo is None or now.utcoffset() is None: raise ValueError("Clock must be timezone-aware.")
        date=now.date().isoformat();checked=now.isoformat();client=self.client_factory()
        try:
            if scope_key in self.repositories: return await self._repo(client,scope_key,cursor,date,checked)
            return await self._topic(client,scope_key,cursor,date,checked)
        finally: await client.aclose()
    async def _repo(self,client,scope,cursor,date,checked):
        repo=self.repositories[scope];path=f"/repos/{repo['owner']}/{repo['name']}";events=[];nextc=dict(cursor)
        status,headers,payload=await client.get(path,etag=cursor.get("repository_etag"))
        if status==200:
            if not isinstance(payload,dict) or not isinstance(payload.get("id"),int): raise ValueError("Malformed GitHub repository response.")
            events.append(RawEvent(f"snapshot:{payload['id']}:{date}",payload));nextc["repository_etag"]=headers.get("etag")
        elif status!=304: raise GitHubAPIError(f"Unexpected repository status {status}")
        status,rheaders,rpayload=await client.get(path+"/releases",params={"per_page":100},etag=cursor.get("releases_etag"))
        if status==200:
            if not isinstance(rpayload,list): raise ValueError("GitHub releases response must be a list.")
            for release in rpayload:
                if isinstance(release,dict) and isinstance(release.get("id"),int): events.append(RawEvent(f"release:{release['id']}",release))
                else: logger.warning("Skipping malformed release for %s",scope)
            nextc["releases_etag"]=rheaders.get("etag")
        elif status!=304: raise GitHubAPIError(f"Unexpected releases status {status}")
        nextc["last_checked_at"]=checked
        return FetchResult(events,nextc)
    async def _topic(self,client,scope,cursor,date,checked):
        topic=self.topics[scope];status,headers,payload=await client.get("/search/repositories",params={"q":f"topic:{topic}","per_page":1})
        if status!=200 or not isinstance(payload,dict): raise GitHubAPIError("Unexpected topic search response.")
        if payload.get("incomplete_results") is True: raise RuntimeError(f"Topic '{topic}' search was incomplete.")
        count=payload.get("total_count")
        if not isinstance(count,int) or isinstance(count,bool): raise ValueError("Invalid topic total_count.")
        return FetchResult([RawEvent(f"topic:{topic}:{date}",{"topic":topic,"total_count":count})],{**cursor,"last_checked_at":checked})
