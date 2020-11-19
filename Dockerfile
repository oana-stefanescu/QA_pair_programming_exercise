FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY ./app /app/app
COPY ./requirements.txt /app/

RUN cd /app/ && pip install --upgrade pip && pip install -r /app/requirements.txt

# copy prestart script
COPY docker/prestart.sh /app/prestart.sh
RUN chmod +x /app/prestart.sh
# set environment variable required for prometheus multiprocessing
ENV prometheus_multiproc_dir="/prometheus-tmp"
# create the directory
RUN mkdir -p /prometheus-tmp
# include the setup logic in the gunicorn configuration file
COPY docker/gunicorn_extra_conf.py /gunicorn_extra_conf.py
RUN echo "\n" >> /gunicorn_conf.py && cat /gunicorn_extra_conf.py >> /gunicorn_conf.py
RUN rm -f /gunicorn_extra_conf.py
