#!/bin/bash

while :
do
    python fetch_process.py | python parser.py | python sniper.py
    sleep 1
done