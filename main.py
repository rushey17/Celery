from celery import Celery
from fastapi import FastAPI
import time
import tasks
import uvicorn as uvicorn

app = FastAPI()


@app.get("/")
async def root():
    task = tasks.print_result.delay()
    return {"task_id": task.id}

@app.post("/add/{x}/{y}", status_code=201)
def add(x: int, y: int):
    task = tasks.add.delay(x,y)
    return {"task_id": task.id}

@app.post("/divide/{x}/{y}", status_code=201)
def divide(x: int, y: int):
    task = tasks.divide.delay(x, y)
    return {"task_id": task.id}

@app.post("/multiply/{x}/{y}", status_code=201)
def divide(x: int, y: int):
    task = tasks.multiply.delay(x, y)
    return {"task_id": task.id}

@app.post("/schedule", status_code=201)
def schedule():
    task = tasks.schedule.delay()
    return {"task_id": task.id}


@app.get("/write", status_code=201)
async def write():
    task = tasks.write_file.delay()
    return {"task_id": task.id}


if __name__ == "__main__":
    uvicorn.run("main:app", port=9000, reload=True)