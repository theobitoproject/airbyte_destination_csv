#! /bin/bash

tag=$(date +%s%N)
echo "building destination csv connector with tag: $tag"

docker build . -t bingo/destination-csv:${tag}

echo "pushed ${tag}"
