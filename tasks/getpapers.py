from prefect import task   
import arxiv

@task
def get_list_of_papers(queryString: str, max_results: int, sort_by: arxiv.SortCriterion, sort_order: arxiv.SortOrder) -> list[arxiv.Result]:
    client = arxiv.Client()
    query = arxiv.Search(
        query=queryString,
        max_results=max_results,
        sort_by=sort_by,
        sort_order=sort_order,
        )
    return list(client.results(query))