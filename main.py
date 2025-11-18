from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()


@app.get("/test")
def test():
    return "Test successful"


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
