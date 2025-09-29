import arxiv
from typing import Any, Optional, Union, Mapping, Tuple, List
from datetime import datetime, timezone

def _lower(s: Optional[str]) -> str:
    return (s or "").lower()

def _extract_text_fields(
    item: Union[arxiv.Result, Mapping[str, Any]]
) -> Tuple[str, str, str, str]:
    if isinstance(item, arxiv.Result):
        title = getattr(item, "title", "") or ""
        summary = getattr(item, "summary", "") or ""
        authors_list = getattr(item, "authors", []) or []
        authors = ", ".join(getattr(a, "name", str(a)) for a in authors_list)
        cats = ", ".join(getattr(item, "categories", []) or [])
        return title, summary, authors, cats

    title = item.get("title", "")
    if isinstance(title, dict):
        title = title.get("_", "") or ""

    summary = item.get("summary", "")
    if isinstance(summary, dict):
        summary = summary.get("_", "") or ""

    if "author" in item:
        auth = item["author"]
        if isinstance(auth, list):
            names = []
            for a in auth:
                if isinstance(a, dict):
                    names.append(a.get("name", ""))
                else:
                    names.append(str(a))
            authors = ", ".join(filter(None, names))
        else:
            authors = str(auth)
    else:
        authors = item.get("authors", "") or ""

    cats_list: List[str] = []
    cat = item.get("category")
    if cat is not None:
        if isinstance(cat, list):
            for c in cat:
                if isinstance(c, dict):
                    cats_list.append(c.get("term", ""))
                else:
                    cats_list.append(str(c))
        else:
            if isinstance(cat, dict):
                cats_list.append(cat.get("term", ""))
            else:
                cats_list.append(str(cat))

    return title or "", summary or "", authors or "", ", ".join(filter(None, cats_list))

def _published_at(item: Union[arxiv.Result, Mapping[str, Any]]) -> datetime:
    if isinstance(item, arxiv.Result):
        dt = getattr(item, "published", None) or getattr(item, "updated", None)
        if isinstance(dt, datetime):
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return datetime.fromtimestamp(0, tz=timezone.utc)

    p = item.get("published")
    if isinstance(p, dict):
        p = p.get("_")
    u = item.get("updated")
    if isinstance(u, dict):
        u = u.get("_")

    def to_dt(x: Optional[str]) -> Optional[datetime]:
        if not x:
            return None
        try:
            dt = datetime.fromisoformat(x.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    return to_dt(p) or to_dt(u) or datetime.fromtimestamp(0, tz=timezone.utc)

def _categories(item: Union[arxiv.Result, Mapping[str, Any]]) -> List[str]:
    if isinstance(item, arxiv.Result):
        return list(getattr(item, "categories", []) or [])

    out: List[str] = []
    cat = item.get("category")
    if cat is None:
        return out
    if isinstance(cat, list):
        for c in cat:
            if isinstance(c, dict):
                term = c.get("term")
                if term:
                    out.append(term)
            elif isinstance(c, str):
                out.append(c)
    elif isinstance(cat, dict):
        term = cat.get("term")
        if term:
            out.append(term)
    elif isinstance(cat, str):
        out.append(cat)
    return [c for c in out if c]


def _pick_links(item: Union[arxiv.Result, Mapping[str, Any]]) -> Tuple[str, str]:
    """
    Returns (html_href, pdf_href). Works for arxiv.Result and Atom-like dicts.
    Handles link entries that are dicts or arxiv.Result.Link objects.
    """
    # Case 1: arxiv.Result instance
    if isinstance(item, arxiv.Result):
        html = item.entry_id or ""
        pdf = ""

        links = getattr(item, "links", []) or []
        for l in links:
            # Support both dicts and arxiv.Result.Link objects
            if isinstance(l, dict):
                href = l.get("href", "") or ""
                title = l.get("title")
                rel = l.get("rel")
            else:
                # arxiv.Result.Link has attributes
                href = getattr(l, "href", "") or ""
                title = getattr(l, "title", None)
                rel = getattr(l, "rel", None)

            if not html and (rel == "alternate" or ("abs" in href)):
                html = href
            if not pdf and href and ((title == "pdf") or ("/pdf/" in href)):
                pdf = href

        # Fallbacks
        if not pdf and html and "/abs/" in html:
            pdf = html.replace("/abs/", "/pdf/")
        return html or "", pdf or ""

    # Case 2: Atom-like dict
    links = item.get("link")
    links_list = links if isinstance(links, list) else ([links] if links else [])
    html = ""
    pdf = ""

    for l in links_list:
        if isinstance(l, dict):
            href = l.get("href", "") or ""
            title = l.get("title")
            rel = l.get("rel")
        else:
            # Unexpected shape; skip
            continue

        if not html and (rel == "alternate" or ("abs" in href)):
            html = href
        if not pdf and href and ((title == "pdf") or ("/pdf/" in href)):
            pdf = href

    # Final fallback: derive pdf from id/entry if possible
    if not html:
        html = str(item.get("id", "")) or ""
    if not pdf and html and "/abs/" in html:
        pdf = html.replace("/abs/", "/pdf/")

    return html or "", pdf or ""

def _item_id(item: Union[arxiv.Result, Mapping[str, Any]]) -> str:
    if isinstance(item, arxiv.Result):
        return getattr(item, "entry_id", "") or ""
    return str(item.get("id", "") or "")
