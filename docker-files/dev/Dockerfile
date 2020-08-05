# For more information, please refer to https://aka.ms/vscode-docker-python
FROM jupyter/tensorflow-notebook

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

USER root

# Install pip requirements
ADD requirements/ ./requirements
RUN python -m pip install --no-cache-dir -r  requirements/base.txt && \
    python -m pip install --no-cache-dir -r  requirements/dev.txt

USER $NB_UID

RUN jupyter contrib nbextension install --user && \
    jupyter nbextension enable autosavetime/main && \
    jupyter nbextension enable code_prettify/autopep8 && \
    jupyter nbextension enable codefolding/main && \
    jupyter nbextension enable collapsible_headings/main && \
    jupyter nbextension enable comment-uncomment/main && \
    jupyter nbextension enable contrib_nbextensions_help_item/main && \
    jupyter nbextension enable equation-numbering/main && \
    jupyter nbextension enable execute_time/ExecuteTime && \
    jupyter nbextension enable hide_input/main  && \
    jupyter nbextension enable nbresuse && \
    jupyter nbextension enable spellchecker/main && \
    jupyter nbextension enable toc2/main && \
    jupyter nbextension enable toggle_all_line_numbers/main