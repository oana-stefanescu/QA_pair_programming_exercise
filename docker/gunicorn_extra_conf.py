"""This file contains additional configuration required for gunicorn.

At the moment it only contains logic for prometheus-client multi process mode, i.e. it adds a child_exit method
that notifies prometheus_client about the exit to remove the files etc.

It is appended to the file /gunicorn_conf_extension.py (the configuration file used by the docker image).
"""

from prometheus_client import multiprocess


def child_exit(server, worker):
    multiprocess.mark_process_dead(worker.pid)
