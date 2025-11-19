from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from util.db_connectors import get_connection
from sqlalchemy import text
from time import time


router = APIRouter()


class queryRequest(BaseModel):
    query: str
    database: str


class queryResponse(BaseModel):
    time_taken: float
    headers: list[str]
    data: list[dict]


@router.post("/run_query", response_model=queryResponse)
def run_query_basic(request: queryRequest):
    start_time = time()
    conn = get_connection(request.database)
    with conn.connect() as connection:
        result = connection.execute(text(request.query))
        rows = [dict(row) for row in result]
        headers = list(result.keys())
        time_taken = time() - start_time

        response = queryResponse(time_taken=time_taken, headers=headers, data=rows)
        return response
    raise HTTPException(status_code=500, detail="Query execution failed")
