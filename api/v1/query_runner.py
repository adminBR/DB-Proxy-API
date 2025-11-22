from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from util.db_connectors import get_db_engine, get_redis_client
from sqlalchemy import text
import asyncio
from time import time
from typing import Optional
import json
from pydantic import BaseModel, Field


router = APIRouter()

r = get_redis_client()


class QueryRequest(BaseModel):
    """
    Represents a query request.

    database: The type or engine of the database (e.g., 'postgres', 'mysql').
    database_name: The specific database/schema name to connect to.
    """

    query: str
    database: str
    database_name: str
    cache: Optional[bool] = False
    cache_duration_seconds: Optional[int] = Field(300, ge=10, le=3600)


class QueryResponse(BaseModel):
    time_taken: float
    headers: list[str]
    data: list[dict]


async def run_blocking_query(engine, query):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: engine.execute(query))


# Endpoint for query execution with improved I/O handling
@router.post("/run_query_async", response_model=QueryResponse)
async def run_query_async(request: QueryRequest):
    if request.query.strip() == "":
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    try:
        engine = get_db_engine(request.database, request.database_name)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to acquire connection: {e}"
        )

    if request.cache:
        cached_result = r.get(request.query)
        if cached_result and type(cached_result) == bytes:
            cached_data = json.loads(cached_result)
            return QueryResponse(
                time_taken=0.0, headers=cached_data["headers"], data=cached_data["data"]
            )

    try:

        def execute_query():
            with engine.connect() as connection:
                result = connection.execute(text(request.query))
                rows = [dict(row) for row in result.mappings().all()]
                headers = list(rows[0].keys()) if rows else []
                return rows, headers

        loop = asyncio.get_running_loop()

        start_time = time()
        rows, headers = await loop.run_in_executor(None, execute_query)
        time_taken = time() - start_time

        if request.cache:
            r.set(
                request.query,
                json.dumps(
                    {"headers": headers, "data": rows},
                    default=str,
                ),
                ex=request.cache_duration_seconds,
            )

        return QueryResponse(time_taken=time_taken, headers=headers, data=rows)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")


@router.post("/run_query", response_model=QueryResponse)
def run_query_basic(request: QueryRequest):
    if request.query.strip() == "":
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    try:
        engine = get_db_engine(request.database, request.database_name)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to acquire connection: {e}"
        )

    if request.cache:
        cached_result = r.get(request.query)
        if cached_result and type(cached_result) == bytes:
            cached_data = json.loads(cached_result)
            return QueryResponse(
                time_taken=0.0, headers=cached_data["headers"], data=cached_data["data"]
            )

    try:
        start_time = time()
        with engine.connect() as connection:
            result = connection.execute(text(request.query))
            rows = [dict(row) for row in result.mappings().all()]
            headers = list(rows[0].keys()) if rows else []
            time_taken = time() - start_time

            if request.cache:
                r.set(
                    request.query,
                    json.dumps(
                        {"headers": headers, "data": rows},
                        default=str,
                    ),
                    ex=request.cache_duration_seconds,
                )

            response = QueryResponse(time_taken=time_taken, headers=headers, data=rows)
            return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")
