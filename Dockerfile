FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY ./app /app/app
COPY ./requirements.txt /app/

RUN cd /app/ && pip install --upgrade pip && pip install -r /app/requirements.txt
