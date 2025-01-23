FROM python:3.12.8

RUN apt-get install wget

COPY requirements.txt .
RUN pip install -r requirements.txt

VOLUME ["/data"]
WORKDIR /app
COPY ingest.py .

ENTRYPOINT [ "python", "ingest.py" ]

