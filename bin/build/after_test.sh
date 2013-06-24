#!/bin/bash

hive_log_path=build/ql/tmp/hive.log
if [ -f "${hive_log_path}" ] ; then
  echo "Compressing ${hive_log_path} with gzip"
  gzip "${hive_log_path}"
fi

