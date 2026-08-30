import asyncio
import base64
import time
from datetime import UTC, datetime, timedelta

import httpx

from .base import (
    Article,
    ArticleUnavailable,
    Connector,
    Folder,
    UpstreamError,
    decode_json,
    parse_dt,
    raise_for_upstream,
    retry_after_seconds,
)

PER_PAGE = 100
LAST_DAY = "last-24-hours"


def _category_id(name: str) -> str:
    encoded = base64.urlsafe_b64encode(name.encode()).decode().rstrip("=")
    return f"category-{encoded}"


def _category_name(category_id: str) -> str | None:
    if not category_id.startswith("category-"):
        return None
    encoded = category_id.removeprefix("category-")
    try:
        return base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode()
    except (UnicodeDecodeError, ValueError):
        return None


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, UTC).replace(tzinfo=None)
    if isinstance(value, str):
        return parse_dt(value)
    return None


def _item_content(item: dict) -> str:
    for field in ("content", "summary"):
        value = item.get(field)
        if isinstance(value, dict) and value.get("content"):
            return str(value["content"])
        if isinstance(value, str) and value:
            return value
    return ""


def _item_url(item: dict) -> str | None:
    canonical = item.get("canonical") or item.get("alternate") or []
    if isinstance(canonical, list) and canonical:
        first = canonical[0]
        if isinstance(first, dict) and first.get("href"):
            return str(first["href"])
    origin = item.get("origin")
    if isinstance(origin, dict) and origin.get("htmlUrl"):
        return str(origin["htmlUrl"])
    return None


def _article_from_item(item: dict) -> Article:
    published = _timestamp(item.get("published"))
    author = item.get("author") or None
    return Article(
        # FreshRSS uses a Google Reader tag ID whose final hex component is a
        # stable, URL-safe identifier. The full tag contains slashes, which
        # would be decoded as path separators by the OPDS download route.
        id=str(item["id"]).rsplit("/", 1)[-1],
        title=item.get("title") or "Untitled",
        author=str(author) if author else None,
        summary=_item_content(item)[:280] or None,
        url=_item_url(item),
        updated=published or datetime.now(UTC).replace(tzinfo=None),
        content_date=published,
        category="article",
    )


class FreshRSSConnector(Connector):
    name = "freshrss"
    description = "FreshRSS"

    def __init__(
        self,
        url: str,
        username: str,
        api_password: str,
        categories: tuple[str, ...] = (),
        client: httpx.AsyncClient | None = None,
    ):
        self._username = username
        self._api_password = api_password
        self._categories = {name.casefold(): name for name in categories if name.strip()}
        self._auth: str | None = None
        self._client = client or httpx.AsyncClient(base_url=url.rstrip("/"), timeout=30.0)
        self._auth_lock = asyncio.Lock()

    async def _login(self) -> None:
        try:
            resp = await self._client.post(
                "/accounts/ClientLogin",
                data={"Email": self._username, "Passwd": self._api_password},
            )
        except httpx.HTTPError as e:
            raise UpstreamError(f"Could not reach FreshRSS: {type(e).__name__}") from e
        raise_for_upstream(resp, "FreshRSS")
        lines = dict(
            line.split("=", 1) for line in resp.text.splitlines() if "=" in line
        )
        auth = lines.get("Auth")
        if not auth:
            raise UpstreamError("FreshRSS returned an unexpected login response")
        self._auth = auth

    async def _get(self, path: str, params: dict[str, str]) -> dict:
        for attempt in (0, 1):
            async with self._auth_lock:
                if self._auth is None:
                    await self._login()
                auth = self._auth
            try:
                resp = await self._client.get(
                    path,
                    params={**params, "output": "json"},
                    headers={"Authorization": f"GoogleLogin auth={auth}"},
                )
            except httpx.HTTPError as e:
                raise UpstreamError(f"Could not reach FreshRSS: {type(e).__name__}") from e
            if resp.status_code == 401 and attempt == 0:
                self._auth = None
                continue
            if resp.status_code == 429 and attempt == 0:
                await asyncio.sleep(retry_after_seconds(resp))
                continue
            break
        raise_for_upstream(resp, "FreshRSS")
        return decode_json(resp, "FreshRSS")

    async def _post(self, path: str, data: dict[str, str]) -> dict:
        for attempt in (0, 1):
            async with self._auth_lock:
                if self._auth is None:
                    await self._login()
                auth = self._auth
            try:
                resp = await self._client.post(
                    path,
                    data=data,
                    headers={"Authorization": f"GoogleLogin auth={auth}"},
                )
            except httpx.HTTPError as e:
                raise UpstreamError(f"Could not reach FreshRSS: {type(e).__name__}") from e
            if resp.status_code == 401 and attempt == 0:
                self._auth = None
                continue
            if resp.status_code == 429 and attempt == 0:
                await asyncio.sleep(retry_after_seconds(resp))
                continue
            break
        raise_for_upstream(resp, "FreshRSS")
        return decode_json(resp, "FreshRSS")

    async def list_folders(self) -> list[Folder]:
        data = await self._get("/reader/api/0/tag/list", {})
        names = [
            str(tag["id"]).removeprefix("user/-/label/")
            for tag in data.get("tags", [])
            if (
                isinstance(tag, dict)
                and tag.get("type") == "folder"
                and str(tag.get("id", "")).startswith("user/-/label/")
            )
        ]
        if self._categories:
            names = [name for name in names if name.casefold() in self._categories]
        return [Folder(_category_id(name), name, "FreshRSS category") for name in names]

    async def list_views(self) -> list[Folder]:
        return [Folder(LAST_DAY, "Last 24 hours", "Published in the last 24 hours")]

    async def _list_stream(
        self, stream: str, cursor: str | None = None, *, since: int | None = None
    ) -> tuple[list[Article], str | None]:
        params = {"s": stream, "n": str(PER_PAGE), "r": "d"}
        if since is not None:
            params["ot"] = str(since)
        if cursor:
            params["c"] = cursor
        data = await self._get("/reader/api/0/stream/contents", params)
        articles = [_article_from_item(item) for item in data.get("items", [])]
        articles.sort(key=lambda article: article.content_date or datetime.min, reverse=True)
        return articles, data.get("continuation")

    async def list_articles(
        self, folder_id: str, cursor: str | None = None
    ) -> tuple[list[Article], str | None]:
        name = _category_name(folder_id)
        if name is None:
            raise KeyError(folder_id)
        # httpx encodes query parameters. Pre-quoting the category would turn
        # spaces into %2520 and make FreshRSS look up a different category.
        stream = f"user/-/label/{name}"
        return await self._list_stream(stream, cursor)

    async def list_view_articles(
        self, view_id: str, cursor: str | None = None
    ) -> tuple[list[Article], str | None]:
        if view_id != LAST_DAY:
            raise KeyError(view_id)
        since = int(time.time() - timedelta(days=1).total_seconds())
        articles, next_cursor = await self._list_stream(
            "user/-/state/com.google/reading-list", cursor, since=since
        )
        cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=1)
        return [a for a in articles if a.content_date and a.content_date >= cutoff], next_cursor

    async def get_article_html(self, article_id: str) -> tuple[Article, str]:
        data = await self._post(
            "/reader/api/0/stream/items/contents", {"i": article_id}
        )
        items = data.get("items", [])
        if not items:
            raise ArticleUnavailable("This article is no longer in FreshRSS.", status=404)
        article = _article_from_item(items[0])
        html = _item_content(items[0])
        if not html:
            raise ArticleUnavailable("This item has no readable article text.", status=422)
        return article, html

    async def close(self):
        await self._client.aclose()
