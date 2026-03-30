"""Нормализация URL и доменов для дедупликации."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlparse, urlunparse


_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")


def normalize_url(raw: str) -> str:
    """Приводит URL к каноническому виду (https, без фрагментов)."""
    value = (raw or "").strip()
    if not value:
        return ""

    if value.lower().startswith(("mailto:", "tel:", "javascript:", "data:", "#")):
        return ""

    if not _SCHEME_RE.match(value):
        value = f"https://{value}"

    parsed = urlparse(value)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ""

    if not netloc:
        return ""

    host = netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    try:
        port = parsed.port
    except ValueError:
        return ""

    if port and port not in (80, 443):
        host = f"{host.split(':', 1)[0]}:{port}"
    else:
        host = host.split(":", 1)[0]

    clean_path = re.sub(r"/{2,}", "/", path)
    if not clean_path:
        clean_path = "/"

    normalized = urlunparse(
        (
            scheme.lower(),
            host,
            clean_path.rstrip("/") or "/",
            "",
            parsed.query,
            "",
        )
    )
    return normalized


def normalize_domain(value: str) -> str:
    """Выделяет и нормализует домен (punycode, нижний регистр)."""
    candidate = (value or "").strip()
    if not candidate:
        return ""

    if "/" in candidate or _SCHEME_RE.match(candidate):
        candidate = normalize_url(candidate)
        parsed = urlparse(candidate)
        domain = parsed.netloc
    else:
        domain = candidate

    domain = domain.lower()
    if domain.startswith("www."):
        domain = domain[4:]

    try:
        domain = domain.encode("idna").decode("ascii")
    except UnicodeError:
        pass

    return domain


def build_company_dedupe_key(name: str | None, domain: str | None) -> str:
    """Строит детерминированный ключ дедупликации компании."""
    canonical_domain = normalize_domain(domain)
    payload = canonical_domain or (name or "").strip().lower()
    digest = hashlib.sha1(payload.encode("utf-8"), usedforsecurity=False)
    return digest.hexdigest()


def clean_snippet(text: str | None) -> str:
    """Очищает сниппет от лишних пробелов и переносов."""
    if not text:
        return ""
    compact = re.sub(r"\s+", " ", text)
    return compact.strip()
