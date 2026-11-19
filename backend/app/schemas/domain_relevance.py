import uuid

from pydantic import BaseModel, ConfigDict, Field


class DomainRelevanceRequest(BaseModel):
    target_domains: list[str] = Field(default_factory=list)
    interests: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class TechnologyDomainRelevanceResponse(BaseModel):
    technology_id: uuid.UUID
    canonical_name: str
    domain_tags: list[str]
    domain_relevance: float
