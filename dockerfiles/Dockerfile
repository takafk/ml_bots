FROM jupyter/datascience-notebook:latest

# Node js
USER jovyan
RUN conda update -n base conda
#ENV PATH $PATH:/home/jovyan/work/src/
#RUN echo $PATH
ENV PYTHONPATH "${PYTONPATH}:/home/jovyan/work/src/"

# Install additional packages

COPY requirements.txt /tmp/requirements.txt
RUN  fix-permissions $CONDA_DIR && fix-permissions /home/$NB_USER
RUN pip install --requirement /tmp/requirements.txt

## JupyterLab
RUN jupyter labextension install @lckr/jupyterlab_variableinspector \
    && jupyter labextension install jupyterlab-flake8

RUN jupyter labextension install @ryantam626/jupyterlab_code_formatter@1.2.0 \
    && jupyter serverextension enable --py jupyterlab_code_formatter \
    && jupyter labextension install @jupyterlab/toc \
    && jupyter labextension install jupyterlab-flake8