FROM tiangolo/uvicorn-gunicorn-fastapi:python3.6

COPY ./app/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY ./app/ /app

# Add the credentials for AWS
RUN mkdir /root/.aws
ADD .credentials /app
RUN if [ -f "/app/.credentials" ]; then mv /app/.credentials /root/.aws/credentials ; else echo "pass" ; fi

WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 8080
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
