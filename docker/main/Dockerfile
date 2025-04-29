# ---------------------------------------------------------------------------- #
#                            global build arguments                            #
# ---------------------------------------------------------------------------- #

# Global ARG, available to all stages (if renewed)
ARG WORKDIR="/repo"

# global username
ARG USERNAME=somenergia
ARG USER_UID=1000
ARG USER_GID=1000

# tag used in all images
ARG PYTHON_VERSION=3.10.13

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

# Poetry version
ARG POETRY_VERSION=1.8.4

# Pipx version
ARG PIPX_VERSION=1.7.0

# prepare the $PATH
ENV PATH=/opt/pipx/bin:${WORKDIR}/.venv/bin:$PATH \
	PIPX_BIN_DIR=/opt/pipx/bin \
	PIPX_HOME=/opt/pipx/home \
	PIPX_VERSION=$PIPX_VERSION \
	POETRY_VERSION=$POETRY_VERSION \
	PYTHONPATH=${WORKDIR} \
	# Don't buffer `stdout`
	PYTHONUNBUFFERED=1 \
	# Don't create `.pyc` files:
	PYTHONDONTWRITEBYTECODE=1 \
	# make poetry create a .venv folder in the project
	POETRY_VIRTUALENVS_IN_PROJECT=true

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

# Copy everything to the container, we filter out what we don't need using .dockerignore
WORKDIR ${WORKDIR}

# make sure the user owns /repo
RUN chown -R ${USER_UID}:${USER_GID} ${WORKDIR}

# Copy only the files needed for installing dependencies
COPY --chown=${USER_UID}:${USER_GID} pyproject.toml poetry.lock ${WORKDIR}/

USER ${USERNAME}


# ---------------------------------------------------------------------------- #
#                                 pre stage                                #
# ---------------------------------------------------------------------------- #

FROM builder AS main-pre


# Install dependencies and creates a a virtualenv at /repo/.venv
RUN poetry install --no-root --only main


# ---------------------------------------------------------------------------- #
#                                   main stage                                  #
# ---------------------------------------------------------------------------- #

# We don't want to use alpine because porting from debian is challenging
# https://stackoverflow.com/a/67695490/5819113
FROM python:${PYTHON_VERSION}-slim AS main

# refresh global arguments
ARG WORKDIR
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG DBT_PROJECT_DIR_NAME
ARG DBT_PACKAGES_INSTALL_PATH

# refresh PATH
ENV PATH=/opt/pipx/bin:${WORKDIR}/.venv/bin:$PATH \
	POETRY_VERSION=$POETRY_VERSION \
	PYTHONPATH=${WORKDIR} \
	# Don't buffer `stdout`
	PYTHONUNBUFFERED=1 \
	# Don't create `.pyc` files:
	PYTHONDONTWRITEBYTECODE=1

# ------------------------------ user management ----------------------------- #

RUN groupadd --gid $USER_GID "${USERNAME}" \
	&& useradd --uid $USER_UID --gid $USER_GID -m "${USERNAME}"

RUN apt-get update \
	&& apt-get install -y vim

# ------------------------------- dbt specific ------------------------------- #

# start from the workdir
WORKDIR ${WORKDIR}

# make the custom non-root user own the workdir
RUN chown -R ${USER_UID}:${USER_GID} ${WORKDIR}

# copy /repo/.venv from main-pre
COPY --from=main-pre --chown=${USER_UID}:${USER_GID} ${WORKDIR} ${WORKDIR}

# Run all commands as non-root user
USER ${USERNAME}

# Move profiles to .dbt
RUN mkdir -p "/home/${USERNAME}/.dbt"

COPY ./${DBT_PROJECT_DIR_NAME}/config/profiles.yml /home/${USERNAME}/.dbt/profiles.yml

# install deps
RUN mkdir -p ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/
COPY ./${DBT_PROJECT_DIR_NAME}/dbt_project.yml ./${DBT_PROJECT_DIR_NAME}/packages.yml ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/

# cd into the project dir and install deps since re_data does not understand --project-dir
RUN dbt deps --project-dir ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/ --profiles-dir /home/${USERNAME}/.dbt


# ---------------------------------------------------------------------------- #
#                                   dev stage                                  #
# ---------------------------------------------------------------------------- #

# This stage contains all the tools needed for development, with sudo user,
# pipx and poetry already installed.

FROM main-pre AS dev

# refresh global arguments
ARG WORKDIR
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG DBT_PROJECT_DIR_NAME
ARG DBT_PACKAGES_INSTALL_PATH

USER root

# Add USERNAME to sudoers. Omit if you don't need to install software after connecting.
RUN apt-get update \
	&& apt-get install -y sudo git iputils-ping wget \
	&& echo ${USERNAME} ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/${USERNAME} \
	&& chmod 0440 /etc/sudoers.d/${USERNAME}

USER ${USERNAME}

# install all remaning dependencies
RUN poetry install --no-root

# Move profiles to .dbt
RUN mkdir -p "/home/${USERNAME}/.dbt"
COPY --chown=${USER_UID}:${USER_GID} ./${DBT_PROJECT_DIR_NAME}/config/profiles.yml /home/${USERNAME}/.dbt/profiles.yml

# install dbt dependencies
RUN mkdir -p ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/
COPY --chown=${USER_UID}:${USER_GID} ./${DBT_PROJECT_DIR_NAME}/dbt_project.yml ./${DBT_PROJECT_DIR_NAME}/packages.yml ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/

# cd into the project dir and install deps since re_data does not understand --project-dir
RUN cd ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/ && dbt deps --profiles-dir /home/${USERNAME}/.dbt


USER ${USERNAME}

# ---------------------------------------------------------------------------- #
#                                dbt-deps stage                                #
# ---------------------------------------------------------------------------- #

FROM builder AS dbt-deps-pre

# install dependencies
RUN poetry install --no-root --only dbt-deps

# We don't want to use alpine because porting from debian is challenging
# https://stackoverflow.com/a/67695490/5819113
FROM python:${PYTHON_VERSION}-slim AS dbt-deps

# refresh global arguments
ARG WORKDIR
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG DBT_PROJECT_DIR_NAME
ARG DBT_PACKAGES_INSTALL_PATH

# refresh PATH
ENV PATH=/opt/pipx/bin:${WORKDIR}/.venv/bin:$PATH \
	POETRY_VERSION=$POETRY_VERSION \
	PYTHONPATH=${WORKDIR} \
	# Don't buffer `stdout`
	PYTHONUNBUFFERED=1 \
	# Don't create `.pyc` files:
	PYTHONDONTWRITEBYTECODE=1

# ------------------------------ user management ----------------------------- #

RUN groupadd --gid ${USER_GID} "${USERNAME}" \
	&& useradd --uid ${USER_UID} --gid ${USER_GID} -m "${USERNAME}"

# --------------------------- install dependencies --------------------------- #

RUN apt-get update \
	&& apt-get install -y git vim iputils-ping wget curl unzip file \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------- dbt specific ------------------------------- #

WORKDIR ${WORKDIR}

RUN chown -R ${USER_UID}:${USER_GID} ${WORKDIR}

COPY --from=dbt-deps-pre --chown=${USER_UID}:${USER_GID} ${WORKDIR} ${WORKDIR}

# Run all commands as non-root user
USER ${USERNAME}

# Move profiles to .dbt
RUN mkdir -p "/home/${USERNAME}/.dbt"

COPY --chown=${USER_UID}:${USER_GID} ./${DBT_PROJECT_DIR_NAME}/config/profiles.yml /home/${USERNAME}/.dbt/profiles.yml

# install deps
RUN mkdir -p ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/

COPY --chown=${USER_UID}:${USER_GID} ./${DBT_PROJECT_DIR_NAME}/dbt_project.yml ./${DBT_PROJECT_DIR_NAME}/packages.yml ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/

RUN dbt deps --project-dir ${WORKDIR}/${DBT_PROJECT_DIR_NAME}/ --profiles-dir /home/${USERNAME}/.dbt

EXPOSE 80
