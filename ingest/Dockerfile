# Digest-pinned per the repo's no-mutable-tags convention (see chart/values.yaml).
# Verified via: docker pull python:3.12-slim && docker inspect \
#   --format '{{index .RepoDigests 0}}' python:3.12-slim   (Plan 2 Task 2)
FROM python:3.12-slim@sha256:423ed6ab25b1921a477529254bfeeabf5855151dc2c3141699a1bfc852199fbf

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Only the package's build/runtime inputs -- tests/fixtures stay out of the
# shipped image (see .dockerignore); pytest runs on the host VM per Task 2.
COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir .

# Non-root runtime user (brief requirement, Step 5).
RUN groupadd --system --gid 10001 alice \
    && useradd --system --uid 10001 --gid 10001 --home-dir /home/alice --create-home alice \
    && chown -R alice:alice /app
USER alice

ENTRYPOINT ["alice-ingest"]
