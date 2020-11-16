#!/bin/bash

# This script runs the API locally
# It also sets up the multiprocessing engine for prometheus correctly

if [[ -z "${1}" ]]
then
  prometheus_multiproc_dir="./prometheus-tmp"
else
  prometheus_multiproc_dir="${1}"
fi

if [[ ! -d "${prometheus_multiproc_dir}" ]]
then
  printf "Directory \"%s\" does not yet exist, creating it\n" "${prometheus_multiproc_dir}"
  mkdir -p "${prometheus_multiproc_dir}"
else
  printf "Removing existing files from prometheus directory \"%s\"\n" "${prometheus_multiproc_dir}"
  for f in "${prometheus_multiproc_dir}"/*; do
    rm -rf "${f}"
  done
fi
