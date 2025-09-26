from prefect import flow, task, get_run_logger
from prefect.logging.loggers import task_run_logger
from config import ARXIV_URL, DEFAULT_NUM_PAPERS


@task
def get_list_of_papers():
    task_run_logger = get_run_logger()
    task_run_logger.info(f"Getting list of papers from {ARXIV_URL}")
    return 

@task
def filter_papers():
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
    get_list_of_papers()
    filter_papers()
    send_email()


if __name__ == "__main__":
    main()
