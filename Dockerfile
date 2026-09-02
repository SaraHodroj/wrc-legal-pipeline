FROM python:3.12-slim

# System deps: lxml needs libxml2/libxslt headers; curl is used by healthchecks.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libxml2-dev libxslt1-dev curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency layer is copied first so code edits do not invalidate the pip
# cache -- a rebuild after a one-line change takes seconds, not minutes.
# Installing "." at this point would build the package without its sources,
# so we extract the dependency list from pyproject.toml and install only that.
COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
    && python -c "import tomllib; \
print('\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))" \
       > /tmp/requirements.txt \
    && pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/ ./src/
COPY scrapy.cfg ./
COPY scripts/ ./scripts/
RUN pip install --no-cache-dir -e .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DAGSTER_HOME=/opt/dagster
RUN mkdir -p /opt/dagster

# Run as a non-root user: a crawler parsing untrusted third-party documents is
# exactly the process you do not want owning the container.
RUN useradd --create-home --shell /bin/bash pipeline && chown -R pipeline /app /opt/dagster
USER pipeline

CMD ["python", "-m", "wrc_pipeline.scraping.runner"]
