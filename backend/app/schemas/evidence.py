from datetime import datetime
import uuid

from pydantic import BaseModel


class TechnologyEvidenceResponse(BaseModel):
    normalized_event_id: uuid.UUID
    source_name: str
    source_type: str
    source_native_id: str
    summary_text: str
    source_url: str | None = None
    occurred_at: datetime
    fetched_at: datetime
