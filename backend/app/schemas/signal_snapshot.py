from datetime import datetime
from typing import Any
import uuid

from pydantic import BaseModel, ConfigDict


class TechnologySignalResponse(BaseModel):
    technology_id: uuid.UUID
    canonical_name: str
    final_score: float
    formula_version: str
    raw_features: dict[str, Any]
    component_scores: dict[str, Any]
    window_start: datetime
    window_end: datetime
    computed_at: datetime

    model_config = ConfigDict(from_attributes=True)