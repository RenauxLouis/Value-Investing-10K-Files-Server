FROM tiangolo/uvicorn-gunicorn-fastapi:python3.6

COPY ./app/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY ./app/ /app

# Add the credentials for AWS
RUN mkdir /root/.aws
ADD .credentials /app
RUN if [ -f "/app/.credentials" ]; then mv /app/.credentials /root/.aws/credentials ; else echo "pass" ; fi

#WORKDIR /app
#ENV PYTHONPATH "${PYTHONPATH}:/app"

#EXPOSE 8080
#CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]

#FROM python:3.6-slim-buster

#ENV WORKDIR=/usr/src/app
#ENV USER=app
#ENV APP_HOME=/home/app/web
#ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

#WORKDIR $WORKDIR

#RUN pip install --upgrade pip
#COPY app/requirements.txt $WORKDIR/requirements.txt
#RUN pip install -r requirements.txt

#RUN adduser --system --group $USER
#RUN mkdir $APP_HOME
#WORKDIR $APP_HOME

#COPY . $APP_HOME
#RUN chown -R $USER:$USER $APP_HOME
#USER $USER
