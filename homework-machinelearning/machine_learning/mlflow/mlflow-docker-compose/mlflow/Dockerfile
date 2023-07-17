FROM python:3.10-slim-buster

RUN pip install --upgrade pip
# Install python packages
COPY requirements.txt /tmp

ENV HOME="/root"
WORKDIR ${HOME}

RUN apt-get -y update
RUN apt-get -y install git
RUN apt-get -y install curl
RUN apt-get -y install build-essential
RUN apt-get -y install libssl-dev libffi-dev libncurses5-dev zlib1g zlib1g-dev libreadline-dev libbz2-dev libsqlite3-dev make gcc

RUN git clone --depth=1 https://github.com/pyenv/pyenv.git .pyenv
# https://github.com/pyenv/pyenv#installation
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"


RUN pip install -r /tmp/requirements.txt
