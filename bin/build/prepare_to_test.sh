#!/bin/bash

# Used when running Hive tests on Jenkins.

set -e -u -o pipefail
cd $(dirname $0)/../..

echo "Building Hive on $HOSTNAME" >&2
if [ -z "${hive_version:-}" ]; then
  hive_version=$( awk -F= '/^version=/ {print $NF}' <build.properties )
  if [ -z "${hive_version}" ]; then
    echo "Unable to detect Hive version from build.properties" >&2
    exit 1
  fi
fi
echo "Using Hive version: ${hive_version}"

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

