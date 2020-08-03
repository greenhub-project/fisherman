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
ADD requirements/ ./requirements
RUN python -m pip install -r requirements/base.txt
RUN python -m pip install -r requirements/dev.txt

USER $NB_UID

RUN jupyter contrib nbextension install --user

RUN jupyter nbextension enable contrib_nbextensions_help_item/main
RUN jupyter nbextension enable autosavetime/main
RUN jupyter nbextension enable codefolding/main
RUN jupyter nbextension enable code_prettify/autopep8
RUN jupyter nbextension enable collapsible_headings/main
RUN jupyter nbextension enable comment-uncomment/main
RUN jupyter nbextension enable equation-numbering/main
RUN jupyter nbextension enable execute_time/ExecuteTime
RUN jupyter nbextension enable hide_input/main 
RUN jupyter nbextension enable spellchecker/main
RUN jupyter nbextension enable toc2/main
RUN jupyter nbextension enable toggle_all_line_numbers/main