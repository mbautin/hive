#!/bin/bash

# Used when running Hive tests on Jenkins.

set -e -u -o pipefail
cd $(dirname $0)/../..

echo "Building Hive on $HOSTNAME" >&2
if [ -n "${hive_version:-}" ]; then
  echo "Using Hive version ${hive_version}"
else
  echo "Error: hive_version is not set" >&2
  exit 1
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

for d in ~/.m2 ~/.ivy2/cache; do
  if [ -d "${d}" ]; then
    set -x
    find "${d}" \( \
      -name "hive-builtins-${hive_version}.jar" -or \
      -wholename "*/${hive_version}/jars/hive_builtins.jar" \
    \) -exec bash -c "set -x; rm -f {}" \;
    set +x
  fi
done

