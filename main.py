from prefect import flow, task, get_run_logger
from prefect.logging.loggers import task_run_logger
from tasks import getpapers, filterpapers
import arxiv    

@task
def filter_papers(papers: list[arxiv.Result]):
    task_run_logger = get_run_logger()
    task_run_logger.info(f"Filtering papers")
    pass

@task
def send_email():
    task_run_logger = get_run_logger()
    task_run_logger.info(f"Sending email")
    pass

@flow
def main():
    task_run_logger = get_run_logger()
    task_run_logger.info(f"Getting list of papers")
    papers = getpapers.get_list_of_papers()
    # task_run_logger.info(papers)
    task_run_logger.info(f"Found {len(papers)} papers")
    filter_papers(papers)
    send_email()


if __name__ == "__main__":
    main()
