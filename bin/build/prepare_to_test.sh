#!/bin/bash

# Used when running Hive tests on Jenkins.

set -e -u -o pipefail
cd $(dirname $0)/../..

echo "Building Hive on $HOSTNAME" >&2
if [ -z "${hive_version:-}" ]; then

  hive_version_base=$( awk -F= '/^version=/ {print $NF}' <build.properties )
  if [ -z "${hive_version_base}" ]; then
    echo "Failed to determine Hive version configured in build.properties" >&2
    exit 1
  fi

  hive_version_extension=$( c=$(git rev-parse HEAD); echo ${c:0:10} )
  if [ -z "${hive_version_extension}" ]; then
    echo "Failed to get a git sha1 prefix of HEAD in $PWD" >&2
    exit 1
  fi
  hive_version="${hive_version_base}_${hive_version_extension}"
  echo "Using Hive version: ${hive_version} (autodetected)"
else
  echo "Using Hive version: ${hive_version} (already defined)"
fi
echo "hive_version=${hive_version}" >hive_version.env

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

