FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml pyswarm.py ./
RUN pip install --no-cache-dir .

RUN mkdir -p /app/tmp
WORKDIR /app

EXPOSE 8000 8089 5557

ENTRYPOINT ["pyswarm"]