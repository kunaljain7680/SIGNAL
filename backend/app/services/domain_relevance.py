from collections.abc import Iterable


def calculate_domain_relevance(
    technology_domain_tags: Iterable[str],
    target_domains: Iterable[str],
    interests: Iterable[str],
) -> float:
    """Return the Phase 1 domain-only relevance score in the range 0.0–1.0.

    This implements the domain_match term from the SIGNAL architecture:
    |technology.domain_tags ∩ (target_domains ∪ interests)|
    --------------------------------------------------------
                    |technology.domain_tags|

    Tags are compared case-insensitively after trimming whitespace. Empty
    technology tags produce 0.0 because there is no evidence of a domain match.
    """
    technology_tags = {
        tag.strip().casefold()
        for tag in technology_domain_tags
        if tag and tag.strip()
    }
    if not technology_tags:
        return 0.0

    profile_domains = {
        tag.strip().casefold()
        for tag in (*target_domains, *interests)
        if tag and tag.strip()
    }

    return len(technology_tags & profile_domains) / len(technology_tags)
