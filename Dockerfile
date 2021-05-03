FROM tiangolo/uvicorn-gunicorn:python3.7

ENV APP_PATH /app
COPY src/ /app
WORKDIR /app

ADD requirements.txt $APP_PATH
ADD .credentials $APP_PATH

WORKDIR /app

RUN pip install -r requirements.txt

ENV PORT 8080

CMD exec uvicorn app:app --host 0.0.0.0 --port $PORT
