from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, List, Literal
import uuid
from datetime import datetime

class TechnologyCreate(BaseModel):
    canonical_name: str
    slug: str
    description: Optional[str] = None
    domain_tags: List[str] = Field(default_factory=list)
    learning_effort_hours_estimate: Optional[int] = None
    status: Literal["active", "candidate", "archived"] = "active"

    # Explicitly reject any field not defined above (e.g., an injected 'id')
    model_config = ConfigDict(extra="forbid")

class TechnologyResponse(BaseModel):
    id: uuid.UUID
    canonical_name: str
    slug: str
    description: Optional[str] = None
    domain_tags: List[str]
    learning_effort_hours_estimate: Optional[int] = None
    status: str
    created_at: datetime

    # Allow Pydantic to read data directly from the SQLAlchemy model attributes
    model_config = ConfigDict(from_attributes=True)