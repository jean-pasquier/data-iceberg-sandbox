FROM ghcr.io/astral-sh/uv:python3.13-trixie AS builder

ENV UV_PYTHON_DOWNLOADS=0 UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_CACHE_DIR=$HOME/.cache/uv

WORKDIR /app

# Install ONLY dependencies using files: uv.lock & pyproject.toml
RUN --mount=type=cache,target=$UV_CACHE_DIR \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --group python-apps

# Get source code
COPY . /app/

# Install our app
RUN --mount=type=cache,target=$UV_CACHE_DIR \
    uv sync --locked --group python-apps

######## FINAL IMAGE ########
FROM python:3.13-slim-trixie

RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot


COPY --from=builder --chown=nonroot:nonroot /app /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

USER nonroot

WORKDIR /app

ENTRYPOINT ["python"]
CMD ["--version"]
