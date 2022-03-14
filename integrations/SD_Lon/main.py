from fastapi import FastAPI

app = FastAPI()


@app.post("/trigger")
def trigger():
    return {"status": "OK"}
