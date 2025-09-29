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
def get_papers(queryString: str, max_results: int, sort_by: arxiv.SortCriterion, sort_order: arxiv.SortOrder):
    papers = getpapers.get_list_of_papers(queryString, max_results, sort_by, sort_order)
    return papers

@task
def filter_papers(papers: list[arxiv.Result]):
    task_run_logger = get_run_logger()
    task_run_logger.info(f"Filtering papers")
    pass

@task
def send_email(filtered_papers: list[arxiv.Result]):
    task_run_logger = get_run_logger()
    task_run_logger.info(f"Sending email")
    sendemails.send_email(filtered_papers, ["chet.kumar@argonauticventures.com"])
    pass

@flow
def main():
    task_run_logger = get_run_logger()
    # task_run_logger.info(f"Getting list of papers...")
    queryString="cat:cs.AI OR cat:cs.CE OR cat:cs.CR OR cat:cs.CY OR cat:cs.ET OR cat:cs.GL OR cat:cs.LG OR cat:cs.MA OR cat:cs.SE OR cat:econ.GN OR cat:q-fin.EC OR cat:stat.ML"
    papers = getpapers.get_list_of_papers(queryString, max_results=10, sort_by=arxiv.SortCriterion.LastUpdatedDate, sort_order=arxiv.SortOrder.Descending)
    task_run_logger.info(f"Found {len(papers)} papers")

    filtered_papers = filterpapers.filter_papers(papers)
    task_run_logger.info(f"Filtered to {len(filtered_papers)} papers")
    send_email(filtered_papers)


if __name__ == "__main__":
    main()
