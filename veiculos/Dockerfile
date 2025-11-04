FROM python:3.10-alpine

WORKDIR /app

RUN apk add --no-cache build-base postgresql-dev libpq

COPY veiculos/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY veiculos/src/ .

COPY proto/ /app/proto

CMD [ "python", "server.py" ]