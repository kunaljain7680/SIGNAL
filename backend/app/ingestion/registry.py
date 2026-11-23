from app.ingestion.connectors.hn_algolia import HNAlgoliaConnector
from app.ingestion.connectors.remotive import RemotiveConnector
from app.ingestion.contracts import SourceConnector


# Keys are connector names, not database source names.
# Keep source identity mapping here so connector names never create new source rows.
CONNECTORS: dict[str, SourceConnector] = {
    "hn_algolia": HNAlgoliaConnector(),
    "remotive": RemotiveConnector(),
}

CONNECTOR_SOURCE_IDENTITY: dict[str, tuple[str, str]] = {
    "hn_algolia": ("Hacker News", "hn"),
    "remotive": ("Remotive", "jobs"),
}


def get_connector(name: str) -> SourceConnector:
    try:
        return CONNECTORS[name]
    except KeyError as exc:
        raise KeyError(f"Unknown source connector: {name}") from exc
