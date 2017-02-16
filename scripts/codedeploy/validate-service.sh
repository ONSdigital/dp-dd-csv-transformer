#!/bin/bash

if [[ $(docker inspect --format="{{ .State.Running }}" dp-dd-csv-transformer) == "false" ]]; then
  exit 1;
fi
