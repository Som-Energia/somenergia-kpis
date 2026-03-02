# ---------------------------------------------------------------------------- #
#                            global build arguments                            #
# ---------------------------------------------------------------------------- #

# Global ARG, available to all stages (if renewed)
ARG WORKDIR="/project"

# global username
ARG USERNAME=somenergia
ARG USER_UID=1000
ARG USER_GID=1000

# versions used across multiple stages, can be renewed in each stage if needed
ARG PYTHON_VERSION=3.10.13
ARG POETRY_VERSION=2.3.2
ARG PIPX_VERSION=1.7.0

# general variables
# don't use DBT_PROJECT_DIR as it is catched by dbt but not by re_data
ARG DBT_PROJECT_DIR_NAME=dbt_kpis
ARG DBT_PACKAGES_INSTALL_PATH=/home/${USERNAME}/.dbt/dbt_packages


# ---------------------------------------------------------------------------- #
#                                  build stage                                 #
# ---------------------------------------------------------------------------- #

FROM python:${PYTHON_VERSION}-slim AS builder

# Renew args
ARG WORKDIR
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG DBT_PROJECT_DIR_NAME
ARG DBT_PACKAGES_INSTALL_PATH
ARG POETRY_VERSION
ARG PIPX_VERSION

# prepare the $PATH
ENV PATH=/opt/pipx/bin:/opt/venv/bin:$PATH \
    PIPX_HOME=/opt/pipx \
    PIPX_BIN_DIR=/opt/pipx/bin \
    PYTHONPATH=${WORKDIR} \
    VIRTUAL_ENV=/opt/venv \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# ------------------------------ add user ----------------------------- #

RUN groupadd --gid $USER_GID "${USERNAME}" \
    && useradd --uid $USER_UID --gid $USER_GID -m "${USERNAME}"

# -------------------------- add python dependencies ------------------------- #

# Install Pipx using pip
RUN python -m pip install --no-cache-dir --upgrade pip pipx==${PIPX_VERSION}
RUN pipx ensurepath && pipx --version

# Install Poetry using pipx
RUN pipx install --force poetry==${POETRY_VERSION}

# ---------------------------- add code specifics ---------------------------- #

WORKDIR ${WORKDIR}

# make sure the user owns the workdir
RUN chown -R ${USER_UID}:${USER_GID} ${WORKDIR}

# Create virtualenv outside the project directory to avoid bind-mount conflicts
RUN python -m venv /opt/venv \
    && chown -R ${USER_UID}:${USER_GID} /opt/venv

# Copy only the files needed for installing dependencies
COPY --chown=${USER_UID}:${USER_GID} pyproject.toml poetry.lock ${WORKDIR}/

USER ${USERNAME}


# ---------------------------------------------------------------------------- #
#                              main-pre stage                                  #
# ---------------------------------------------------------------------------- #

FROM builder AS main-pre

# Install main dependencies into /opt/venv
RUN poetry install --no-root --only main


# ---------------------------------------------------------------------------- #
#                             runtime-base stage                               #
# ---------------------------------------------------------------------------- #
# Shared base for all final (non-builder) stages. Sets up the non-root user,
# common ENV vars, apt packages, and workdir so we don't repeat ourselves.

FROM python:${PYTHON_VERSION}-slim AS runtime-base

# Renew args
ARG WORKDIR
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG DBT_PROJECT_DIR_NAME
ARG DBT_PACKAGES_INSTALL_PATH

# Common ENV shared by all runtime stages
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONPATH=${WORKDIR} \
    VIRTUAL_ENV=/opt/venv \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# ------------------------------ user management ----------------------------- #

RUN groupadd --gid $USER_GID "${USERNAME}" \
    && useradd --uid $USER_UID --gid $USER_GID -m "${USERNAME}"

# Common runtime apt packages (git needed by dbt deps for git packages)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR ${WORKDIR}
RUN chown -R ${USER_UID}:${USER_GID} ${WORKDIR}


# ---------------------------------------------------------------------------- #
#                                  main stage                                  #
# ---------------------------------------------------------------------------- #

FROM runtime-base AS main

# Renew args
ARG WORKDIR
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG DBT_PROJECT_DIR_NAME
ARG DBT_PACKAGES_INSTALL_PATH

# Copy virtualenv from builder
COPY --from=main-pre --chown=${USER_UID}:${USER_GID} /opt/venv /opt/venv

# Run all commands as non-root user
USER ${USERNAME}

# ------------------------------- dbt specific ------------------------------- #

# Move profiles to .dbt
RUN mkdir -p "/home/${USERNAME}/.dbt"
COPY --chown=${USER_UID}:${USER_GID} ./${DBT_PROJECT_DIR_NAME}/config/profiles.yml /home/${USERNAME}/.dbt/profiles.yml

# Install dbt deps
RUN mkdir -p ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/
COPY --chown=${USER_UID}:${USER_GID} ./${DBT_PROJECT_DIR_NAME}/dbt_project.yml ./${DBT_PROJECT_DIR_NAME}/packages.yml ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/

RUN dbt deps --project-dir ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/ --profiles-dir /home/${USERNAME}/.dbt

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD ["dbt", "--version"]

# ---------------------------------------------------------------------------- #
#                                  dev stage                                   #
# ---------------------------------------------------------------------------- #
# Inherits from main so dev always has everything production has, plus
# dev-only tools and dependencies.

FROM main AS dev

# Renew args
ARG WORKDIR
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG DBT_PROJECT_DIR_NAME
ARG DBT_PACKAGES_INSTALL_PATH
ARG POETRY_VERSION
ARG PIPX_VERSION

# Need root to install dev tooling
USER root

ENV PATH=/opt/pipx/bin:$PATH \
    PIPX_HOME=/opt/pipx \
    PIPX_BIN_DIR=/opt/pipx/bin \
    POETRY_VIRTUALENVS_CREATE=false

# Install dev apt packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends sudo vim iputils-ping wget \
    && echo "${USERNAME} ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} \
    && chmod 0440 /etc/sudoers.d/${USERNAME} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Re-install pipx + poetry so we can run poetry install for dev deps
RUN python -m pip install --no-cache-dir --upgrade pip pipx==${PIPX_VERSION}
RUN pipx ensurepath && pipx --version
RUN pipx install --force poetry==${POETRY_VERSION}

# Copy lock files (needed because main stage didn't copy them)
COPY --chown=${USER_UID}:${USER_GID} pyproject.toml poetry.lock ${WORKDIR}/

# Materialize venv onto the current writable layer to avoid
# cross-device link errors when poetry updates existing packages
RUN cp -a /opt/venv /tmp/venv \
    && rm -rf /opt/venv \
    && mv /tmp/venv /opt/venv

RUN chown -R ${USER_UID}:${USER_GID} /opt/venv

USER ${USERNAME}

# Install all remaining dependency groups (dev, test, etc.)
RUN poetry install --no-root