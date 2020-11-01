FROM jupyter/datascience-notebook:latest

USER root
ENV PYTHONPATH "/home/jovyan/work/src/:/home/$NB_USER/work/local_disks/"
# apt-get with sudo. First line is to avoid the error: sudo: setrlimit(RLIMIT_CORE)
RUN echo "Set disable_coredump false" >> /etc/sudo.conf
RUN sudo apt-get update && sudo apt-get install graphviz -y wget \
    && sudo apt-get install vim -y wget

# Install additional packages (to store cache, we do it on $NB_USER.)
USER $NB_USER
COPY requirements.txt /tmp/requirements.txt
RUN pip install --requirement /tmp/requirements.txt \
    && fix-permissions $CONDA_DIR \
    && fix-permissions /home/$NB_USER

# Initialization of prefect. Unless chown, permission error arises in Prefecct.
COPY --chown=$NB_USER:users config.toml $HOME/.prefect/

# PREFECT__FLOWS__CHECKPOINTING="True"
## JupyterLab
#RUN jupyter labextension install jupyterlab-flake8

#RUN jupyter labextension install @ryantam626/jupyterlab_code_formatter@1.2.0 \
#    && jupyter serverextension enable --py jupyterlab_code_formatter \
#    && jupyter labextension install @jupyterlab/toc