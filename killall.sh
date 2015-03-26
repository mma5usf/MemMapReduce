#!/bin/bash

for pid in $(ps | egrep "python (worker|master)" | awk '{print $1}' | cut -d' ' -f1); do
  echo kill $pid
  kill -9 $pid
done
