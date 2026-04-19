from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

import requests


class MagaluClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class MagaluSearchPage:
    query: str
    page: int
    url: str
    html: str


class MagaluClient:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        timeout_seconds: float = 20.0,
        user_agent: str = "gocase-eng-dados-magalu-collector/1.0",
    ) -> None:
        self._session = session or requests.Session()
        self._timeout_seconds = timeout_seconds
        self._session.headers.setdefault("User-Agent", user_agent)
        self._session.headers.setdefault(
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        )
        self._session.headers.setdefault("Accept-Language", "pt-BR,pt;q=0.9,en-US;q=0.8")

    def search(self, *, query: str, page: int) -> MagaluSearchPage:
        slug = quote(query.strip())
        url = f"https://www.magazineluiza.com.br/busca/{slug}/"
        if page > 1:
            url = f"{url}?page={page}"

        try:
            resp = self._session.get(url, timeout=self._timeout_seconds)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise MagaluClientError(f"Falha ao buscar Magalu url={url}: {e}") from e

        return MagaluSearchPage(query=query, page=page, url=url, html=resp.text)

