from fastapi import FastAPI

from .sd_changed_at import changed_at


app = FastAPI()


@app.get("/")
async def index() -> dict[str, str]:
    return {"name": "sdlon"}


@app.get("/trigger")
def trigger() -> dict[str, str]:
    changed_at(False, False)
    return {"status": "OK"}
