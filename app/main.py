from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def root():
    return {"FastAPI": "is awesome"}


@app.get("/dummy")
def dummy():
    return {"hello": "dummy endpoint"}
