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
    python -m pip install --no-cache-dir -r  requirements/dev.txt && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

# R pre-requisites
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    fonts-dejavu \
    unixodbc \
    unixodbc-dev \
    r-cran-rodbc \
    gfortran \
    gcc && \
    rm -rf /var/lib/apt/lists/*

# Fix for devtools https://github.com/conda-forge/r-devtools-feedstock/issues/4
RUN ln -s /bin/tar /bin/gtar

USER $NB_UID

# R packages
RUN conda install --quiet --yes \
    'r-base=4.0.0' \
    'r-caret=6.*' \
    'r-crayon=1.3*' \
    'r-devtools=2.3*' \
    'r-forecast=8.12*' \
    'r-hexbin=1.28*' \
    'r-htmltools=0.4*' \
    'r-htmlwidgets=1.5*' \
    'r-irkernel=1.1*' \
    'r-nycflights13=1.0*' \
    'r-randomforest=4.6*' \
    'r-rcurl=1.98*' \
    'r-rmarkdown=2.2*' \
    'r-rodbc=1.3*' \
    'r-shiny=1.4*' \
    'r-tidyverse=1.3*' \
    'unixodbc=2.3.*' \
    'r-tidymodels=0.1*' \
    && \
    conda clean --all -f -y && \
    fix-permissions "${CONDA_DIR}"

# Install e1071 R package (dependency of the caret R package)
RUN conda install --quiet --yes r-e1071

RUN jupyter labextension install jupyterlab-plotly@4.9.0 && \
    jupyter labextension install @jupyter-widgets/jupyterlab-manager plotlywidget@4.9.0 && \
    jupyter labextension install jupyterlab-execute-time

RUN jupyter contrib nbextension install --user && \
    jupyter nbextension enable autosavetime/main && \
    jupyter nbextension enable code_prettify/autopep8 && \
    jupyter nbextension enable codefolding/main && \
    jupyter nbextension enable collapsible_headings/main && \
    jupyter nbextension enable comment-uncomment/main && \
    jupyter nbextension enable contrib_nbextensions_help_item/main && \
    jupyter nbextension enable equation-numbering/main && \
    jupyter nbextension enable hide_input/main  && \
    jupyter nbextension enable nbresuse && \
    jupyter nbextension enable spellchecker/main && \
    jupyter nbextension enable toc2/main && \
    jupyter nbextension enable toggle_all_line_numbers/main