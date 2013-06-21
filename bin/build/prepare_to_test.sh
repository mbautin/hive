#!/bin/bash

set -e -u -o pipefail
cd $(dirname $0)/../..

echo "Building Hive on $HOSTNAME" >&2
if [ -n "$hive_version" ]; then
  echo "Using Hive version ${hive_version}"
fi
echo
echo "Most recent Hive commits:"
git log -n 10
echo

echo "--------------------------------------------------------------------------------"
echo "build.properties"
echo "--------------------------------------------------------------------------------"
echo
echo
cat build.properties
echo
echo "--------------------------------------------------------------------------------"
echo
