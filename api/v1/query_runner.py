from fastapi import APIRouter


router = APIRouter()


@router.get("/run_query_basic")
def run_query_basic():
    return "Query runner is set up"
