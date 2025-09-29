from prefect import flow, task, get_run_logger
from prefect.logging.loggers import task_run_logger
from tasks import getpapers, filterpapers, sendemails
import arxiv    

# helper
def log_top_titles(records, n=10):
    logger = get_run_logger()
    if not records:
        logger.info("No filtered papers")
        return
    for i, rec in enumerate(records[:n], start=1):
        title = rec.get("title") if isinstance(rec, dict) else getattr(rec, "title", "")
        logger.info(f"{i:02d}. {title}")

@task
def get_papers(query: str, max_results: int, sort_by: arxiv.SortCriterion, sort_order: arxiv.SortOrder):
    logger = get_run_logger()
    logger.info("Fetching papers from arXiv...")
    papers = getpapers.get_list_of_papers(query, max_results, sort_by, sort_order)
    logger.info(f"Got {len(papers)} papers")
    return papers

@task
def filter_papers(papers: list[arxiv.Result]):
    logger = get_run_logger()
    logger.info("Filtering papers...")
    return filterpapers.filter_papers(papers)   # call your existing filtering logic

@task
def send_email(filtered_papers: list[arxiv.Result]):
    logger = get_run_logger()
    logger.info("Sending email...")
    sendemails.send_email(filtered_papers, ["chet.kumar@argonauticventures.com"])
    logger.info("Email sent!")

@flow
def main():
    logger = get_run_logger()

    query = (
        "cat:cs.AI OR cat:cs.CE OR cat:cs.CR OR cat:cs.CY OR cat:cs.ET "
        "OR cat:cs.GL OR cat:cs.LG OR cat:cs.MA OR cat:cs.SE OR "
        "cat:econ.GN OR cat:q-fin.EC OR cat:stat.ML"
    )

    # run tasks as subtasks
    papers_future = get_papers.submit(
        query, 
        max_results=10, 
        sort_by=arxiv.SortCriterion.LastUpdatedDate, 
        sort_order=arxiv.SortOrder.Descending
    )
    papers = papers_future.result()

    filtered_future = filter_papers.submit(papers)
    filtered_papers = filtered_future.result()

    logger.info(f"Filtered to {len(filtered_papers)} papers")
    # log_top_titles(filtered_papers, n=10)

    send_email.submit(filtered_papers)


if __name__ == "__main__":
    main()
