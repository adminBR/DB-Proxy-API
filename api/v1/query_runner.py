from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from util.db_connectors import get_connection
from sqlalchemy import text
from time import time

router = APIRouter()


class queryRequest(BaseModel):
    query: str
    database: str
    database_name: str


class queryResponse(BaseModel):
    time_taken: float
    headers: list[str]
    data: list[dict]


@router.post("/run_query", response_model=queryResponse)
def run_query_basic(request: queryRequest):
    if request.query.strip() == "":
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    try:
        conn = get_connection(request.database, request.database_name)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to acquire connection: {e}"
        )

    try:
        with conn.connect() as connection:
            start_time = time()

            result = connection.execute(text(request.query))
            headers = list(result.keys())
            rows = [dict(row) for row in result.mappings()]
            time_taken = time() - start_time

            response = queryResponse(time_taken=time_taken, headers=headers, data=rows)
            return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")
