# For more information, please refer to https://aka.ms/vscode-docker-python
FROM jupyter/datascience-notebook

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

USER root

RUN apt-get update && apt-get install -y \
    sqlite3 \
&& rm -rf /var/lib/apt/lists/*

#   python3-dev \
#   default-libmysqlclient-dev \
#   build-essential \

# Install pip requirements
ADD requirements.txt .
RUN python -m pip install -r requirements.txt

USER $NB_UID