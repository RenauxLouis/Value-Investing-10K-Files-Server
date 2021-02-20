FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY ./app/TickerServer/requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY ./app/TickerServer /app
