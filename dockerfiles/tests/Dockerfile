FROM jupyter/datascience-notebook:latest

USER root
ENV PYTHONPATH "/home/jovyan/work/src/:/home/$NB_USER/work/local_disks/"
# apt-get with sudo. First line is to avoid the error: sudo: setrlimit(RLIMIT_CORE)
RUN echo "Set disable_coredump false" >> /etc/sudo.conf
RUN sudo apt-get update && sudo apt-get install graphviz -y wget \
    && sudo apt-get install vim -y wget

# Install additional packages (to store cache, we do it on $NB_USER.)
USER $NB_USER
RUN pip install --upgrade pip setuptools wheel
COPY ./dockerfiles/flake8 /home/$NB_USER/.config/flake8
COPY ./dockerfiles/requirements.txt /tmp/requirements.txt
RUN pip install --requirement /tmp/requirements.txt \
    && fix-permissions $CONDA_DIR \
    && fix-permissions /home/$NB_USER

# Initialization of prefect. Unless chown, permission error arises in Prefecct.
COPY --chown=$NB_USER:users ./dockerfiles/config.toml $HOME/.prefect/
