#! /usr/bin/env sh

# This script is executed by the docker image before the actual job starts
# We use this to make sure that the prometheus-tmp directory is empty

if [ -z "${prometheus_multiproc_dir}" ]; then
  echo "environment variable prometheus_multiproc_dir is not set but required for prometheus multiprocess mode!"
  exit 1
fi

if [ ! -d "${prometheus_multiproc_dir}" ]; then
  printf "prometheus_multiproc_dir (\"%s\") is not a directory\n" "${prometheus_multiproc_dir}"
  exit 1
fi

# cleanup the prometheus-tmp dir
for f in "${prometheus_multiproc_dir}"/*; do
    rm -rf "${f}"
done
printf "prometheus_multiproc_dir (\"%s\") has been wiped clean\n" "${prometheus_multiproc_dir}"
