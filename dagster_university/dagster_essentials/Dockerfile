FROM python:3.11-slim

WORKDIR /opt/dagster/app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
COPY data/ ./data/

RUN uv sync --frozen --no-dev --no-editable

ENV PATH="/opt/dagster/app/.venv/bin:$PATH"
ENV PYTHONPATH="/opt/dagster/app/src"

EXPOSE 3030

CMD ["uv", "run", "dagster", "api", "grpc", \
     "-m", "dagster_essentials.definitions", \
     "-a", "defs", \
     "-h", "0.0.0.0", \
     "-p", "3030"]