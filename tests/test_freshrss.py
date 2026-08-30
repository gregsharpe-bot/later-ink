import asyncio
from datetime import datetime

import httpx
import pytest

from later_ink.connectors.base import ArticleUnavailable, UpstreamError
from later_ink.connectors.freshrss import (
    LAST_DAY,
    FreshRSSConnector,
    _category_id,
    _item_url,
)


def _connector(handler):
    return FreshRSSConnector(
        "https://freshrss.example.com/api/greader.php",
        "greg",
        "api-pass",
        categories=("News",),
        client=httpx.AsyncClient(
            base_url="https://freshrss.example.com/api/greader.php",
            transport=httpx.MockTransport(handler),
        ),
    )


def _run(conn, call):
    async def run():
        try:
            return await call()
        finally:
            await conn.close()

    return asyncio.run(run())


def _item(item_id="1", published=1_774_000_000, content="<p>Body</p>"):
    return {
        "id": f"tag:google.com,2005:reader/item/{item_id}",
        "title": f"Article {item_id}",
        "author": "Author",
        "published": published,
        "canonical": [{"href": "https://example.com/article"}],
        "content": {"content": content},
    }


def test_categories_are_discovered_and_filtered():
    def handler(request):
        if request.url.path.endswith("/accounts/ClientLogin"):
            return httpx.Response(200, text="Auth=greg/token\n")
        return httpx.Response(
            200,
            json={
                "tags": [
                    {"id": "user/-/label/News", "type": "folder"},
                    {"id": "user/-/label/Other", "type": "folder"},
                ]
            },
        )

    conn = _connector(handler)
    folders = _run(conn, lambda: conn.list_folders())
    assert [(folder.id, folder.title) for folder in folders] == [(_category_id("News"), "News")]


def test_category_articles_are_published_newest_first():
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path.endswith("/accounts/ClientLogin"):
            return httpx.Response(200, text="Auth=greg/token\n")
        return httpx.Response(200, json={"items": [_item("old", 100), _item("new", 200)]})

    conn = _connector(handler)
    articles, cursor = _run(conn, lambda: conn.list_articles(_category_id("News")))
    assert [article.id for article in articles] == [
        "new",
        "old",
    ]
    assert cursor is None
    request = seen[-1]
    assert request.url.params["s"] == "user/-/label/News"
    assert request.url.params["r"] == "d"


def test_last_24_hours_filters_published_time_only():
    now = int(datetime.now().timestamp())

    def handler(request):
        if request.url.path.endswith("/accounts/ClientLogin"):
            return httpx.Response(200, text="Auth=greg/token\n")
        return httpx.Response(
            200,
            json={"items": [_item("old", now - 86_401), _item("new", now - 10)]},
        )

    conn = _connector(handler)
    articles, _ = _run(conn, lambda: conn.list_view_articles(LAST_DAY))
    assert [article.id for article in articles] == ["new"]


def test_get_article_html_uses_post_item_endpoint():
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path.endswith("/accounts/ClientLogin"):
            return httpx.Response(200, text="Auth=greg/token\n")
        return httpx.Response(200, json={"items": [_item()]})

    conn = _connector(handler)
    article, html = _run(conn, lambda: conn.get_article_html("1"))
    assert article.title == "Article 1"
    assert html == "<p>Body</p>"
    assert seen[-1].method == "POST"
    assert seen[-1].content == b"i=1"


def test_missing_article_is_unavailable():
    def handler(request):
        if request.url.path.endswith("/accounts/ClientLogin"):
            return httpx.Response(200, text="Auth=greg/token\n")
        return httpx.Response(200, json={"items": []})

    conn = _connector(handler)
    with pytest.raises(ArticleUnavailable):
        _run(conn, lambda: conn.get_article_html("missing"))


def test_auth_failure_is_upstream_error():
    def handler(request):
        return httpx.Response(401)

    conn = _connector(handler)
    with pytest.raises(UpstreamError):
        _run(conn, lambda: conn.list_folders())


def test_url_falls_back_to_origin():
    assert _item_url({"origin": {"htmlUrl": "https://example.com/feed"}}) == "https://example.com/feed"
