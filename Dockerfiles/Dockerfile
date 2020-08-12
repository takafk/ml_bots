FROM jupyter/datascience-notebook:latest

# Node js
USER root
RUN apt-get update && apt-get install -y sudo && \
    sudo apt install -y nodejs npm && conda update -n base conda

# Install additional packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

## JupyterLab
RUN jupyter labextension install @lckr/jupyterlab_variableinspector \
    && jupyter labextension install jupyterlab-flake8

RUN jupyter labextension install @ryantam626/jupyterlab_code_formatter@1.2.0 \
    && jupyter serverextension enable --py jupyterlab_code_formatter \
    && jupyter labextension install @jupyterlab/toc \
    && jupyter labextension install jupyterlab-flake8