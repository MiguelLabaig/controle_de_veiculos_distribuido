FROM python:3.10-alpine 
WORKDIR /app

RUN apk add --no-cache build-base postgresql-dev libpq

COPY manutencoes/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY manutencoes/src/ .

CMD [ "python", "server.py" ]