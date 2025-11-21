from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from util.db_connectors import get_connection, get_connection_async
from sqlalchemy import text
from time import time


router = APIRouter()


class QueryRequest(BaseModel):
    """
    Represents a query request.

    database: The type or engine of the database (e.g., 'postgres', 'mysql').
    database_name: The specific database/schema name to connect to.
    """

    query: str
    database: str
    database_name: str


class QueryResponse(BaseModel):
    time_taken: float
    headers: list[str]
    data: list[dict]


# Endpoint for query execution with improved I/O handling
@router.post("/run_query_async", response_model=QueryResponse)
async def run_query_async(request: QueryRequest):
    if request.query.strip() == "":
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    try:
        conn = await get_connection_async(request.database, request.database_name)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to acquire connection: {e}"
        )

    try:
        start_time = time()
        async with conn.connect() as connection:
            result = await connection.execute(text(request.query))
            headers = list(result.mappings().keys())
            rows = [dict(row) for row in result.mappings().all()]
            time_taken = time() - start_time

            response = QueryResponse(time_taken=time_taken, headers=headers, data=rows)
            return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")


@router.post("/run_query", response_model=QueryResponse)
def run_query_basic(request: QueryRequest):
    if request.query.strip() == "":
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    try:
        conn = get_connection(request.database, request.database_name)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to acquire connection: {e}"
        )

    try:
        start_time = time()
        with conn.connect() as connection:
            result = connection.execute(text(request.query))
            headers = list(result.keys())
            rows = [dict(row) for row in result.mappings()]
            time_taken = time() - start_time

            response = QueryResponse(time_taken=time_taken, headers=headers, data=rows)
            return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")
