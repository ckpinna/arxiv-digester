from prefect import task
from config import  MAX_RESULTS   
import arxiv

@task
def get_list_of_papers() -> list[arxiv.Result]:
    client = arxiv.Client()
    query = arxiv.Search(
        query= "cat:cs.AI OR cat:cs.CE OR cat:cs.CR OR cat:cs.CY OR cat:cs.ET OR cat:cs.GL OR cat:cs.LG OR cat:cs.MA OR cat:cs.SE OR cat:econ.GN OR cat:q-fin.EC OR cat:stat.ML", #https://arxiv.org/category_taxonomy
        max_results=MAX_RESULTS,
        sort_by=arxiv.SortCriterion.LastUpdatedDate,
        sort_order=arxiv.SortOrder.Descending,
    )
    return list(client.results(query))