FROM python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/

RUN pip install -r requirements.txt

WORKDIR /app/src/

COPY . /app/src

CMD ["python", "pg_datagen.py"]