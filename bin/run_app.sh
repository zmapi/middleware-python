#!/usr/bin/env bash

set -e

script_dir="$(dirname "${0}")"
project_root="$(realpath "${script_dir}/..")"
entrypoint="${project_root}/apps/${1}"
app_dir="$(dirname "${PWD}/${entrypoint}")"

export PYTHONPATH="${app_dir}:${project_root}/lib:${PYTHONPATH}"

shift

exec python3 "${entrypoint}" ${@}
