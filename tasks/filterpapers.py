from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import arxiv
from prefect import task, get_run_logger
from tools.arxivhelpers import _lower, _published_at, _extract_text_fields, _categories, _pick_links, _item_id

# -----------------------------
# Scoring and filtering
# -----------------------------
def _same_family(cat: str, white: str) -> bool:
    return (cat.split(".")[0] or "") == (white.split(".")[0] or "")


def _score_item(
    item: Union[arxiv.Result, Mapping[str, Any]],
    cfg: RelevanceConfig,
) -> int:
    title, summary, authors, cats = _extract_text_fields(item)
    full = f"{title} {summary} {authors} {cats}".lower()

    # negative keyword filter
    if any(k in full for k in cfg.neg_keywords):
        return float("-inf")  # sentinel for reject

    # category whitelist with cross-list family acceptance
    item_cats = _categories(item)
    # logger = get_run_logger()
    # logger.info(f"Item categories: {item_cats}")
    if cfg.cat_whitelist:
        wl = set(cfg.cat_whitelist)
        ok_cat = any(c in wl for c in item_cats) or any(
            any(_same_family(c, w) for w in wl) for c in item_cats
        )
        if not ok_cat:
            return float("-inf")

    # keyword scoring (title boosted)
    score = 0
    t_low = _lower(title)
    s_low = _lower(summary)
    a_low = _lower(authors)
    for k in cfg.keywords:
        if k in t_low:
            score += cfg.title_boost
        if k in s_low or k in a_low:
            score += 1
    return score


def _shape_record(
    item: Union[arxiv.Result, Mapping[str, Any]],
    score: int,
    published_dt: datetime,
) -> Mapping[str, Any]:
    title, summary, authors, _ = _extract_text_fields(item)
    link_html, link_pdf = _pick_links(item)
    cats = ", ".join(_categories(item))
    return {
        "id": _item_id(item),
        "title": title,
        "authors": authors,
        "categories": cats,
        "published": published_dt.astimezone(timezone.utc).isoformat(),
        "linkHtml": link_html,
        "linkPdf": link_pdf,
        "summary": summary,
        "score": score,
    }


@dataclass(frozen=True)
class RelevanceConfig:
    days: int = 30
    min_score: int = 2
    title_boost: int = 2
    keywords: Tuple[str, ...] = field(
        default_factory=lambda: (
            "LLM","agent","RAG","retrieval","multimodal","foundation model",
            "embedding","vector","self-play","planning","MCTS","AEC","BIM",
            "construction","digital twin","robotics","supply chain",
            "protein","genomic","single-cell","drug discovery","biology",
            "biodesign","diffusion","fintech","credit","fraud","risk",
            "payments","markets","regtech"
        )
    )
    neg_keywords: Tuple[str, ...] = field(
        default_factory=lambda: (
            "quantum gravity","black hole","cosmology",
            "algebraic topology","category theory","number theory"
        )
    )
    cat_whitelist: Tuple[str, ...] = field(
        default_factory=lambda: (
            "cs.LG","cs.AI","cs.CL","cs.CV", "cs.CE", 
            "cs.CR", "cs.CY", "cs.ET", "cs.GL", "cs.LG", 
            "cs.MA", "cs.SE", "econ.GN", "q-fin.EC", "stat.ML"
        )
    )

    queryString="cat:cs.AI OR cat:cs.CE OR cat:cs.CR OR cat:cs.CY OR cat:cs.ET OR cat:cs.GL OR cat:cs.LG OR cat:cs.MA OR cat:cs.SE OR cat:econ.GN OR cat:q-fin.EC OR cat:stat.ML"


@task
def filter_papers(papers: list[arxiv.Result], config: RelevanceConfig | None = None) -> list[arxiv.Result]:
    """
    Filters and scores arXiv results, returning a list of shaped dicts:
      { id, title, authors, categories, published, linkHtml, linkPdf, summary, score }

    Sort order: score desc, then recency desc.
    """

    logger = get_run_logger()
    # logger.info(f"Filtering papers task started...")
    # logger.info(f"Number of papers: {len(papers)}")

    cfg = config or RelevanceConfig()
    # normalize keywords once
    cfg = RelevanceConfig(
        days=cfg.days,
        min_score=cfg.min_score,
        title_boost=cfg.title_boost,
        keywords=tuple(_lower(k) for k in cfg.keywords),
        neg_keywords=tuple(_lower(k) for k in cfg.neg_keywords),
        cat_whitelist=tuple(cfg.cat_whitelist),
    )

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=cfg.days)
    logger = get_run_logger()
    output: List[Mapping[str, Any]] = []
    for item in papers:
        d = _published_at(item)
        if d < cutoff:
            # logger.info(f"Skipping paper {item.title} because it is older than {cfg.days} days")
            continue

        sc = _score_item(item, cfg)
        if sc == float("-inf") or sc < cfg.min_score:
            # logger.info(f"Skipping paper {item.title} because it has a score of {sc}")
            continue
        
        # logger.info(f"Adding paper {item.title} to output with score {sc}")
        output.append(_shape_record(item, sc, d))

    # Sort by score desc, then published desc
    output.sort(key=lambda r: (r["score"], r["published"]), reverse=True)
    logger.info(f"Number of papers after filtering: {len(output)}")
    return output


# -----------------------------
# Example usage
# -----------------------------
# results = list(arxiv.Search(query="large language model", max_results=100).results())
# selected = filter_papers.submit(
#     results,
#     config=RelevanceConfig(
#         days=30,
#         min_score=2,
#         title_boost=2,
#         keywords=("llm", "retrieval", "alignment"),
#         neg_keywords=("survey",),
#         cat_whitelist=("cs.CL", "cs.LG"),
#     ),
# ).result()
# print(len(selected), "papers")
# for r in selected[:5]:
#     print(r["score"], r["published"], r["title"])
