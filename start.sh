#!/bin/bash

python master.py 127.0.0.1:10000 &

for W in $(cat config); do
  python worker.py $W &
done
