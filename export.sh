#!/bin/bash


if test -d /caminho/para/a/pasta; then
  rm -r /caminho/para/a/pasta
fi

counter=0
for fileA in /home/eduardo/Git/dados-gov-br-cnpj-python/datasets/raw/parquet/Estabelecimentos/*
do
  for fileB in /home/eduardo/Git/dados-gov-br-cnpj-python/datasets/raw/parquet/Empresas/*
  do
    counter=$((counter+1))
    echo "Combinação $counter: $fileA $fileB"
    $(python cli/export.py --est="$fileA" --emp="$fileB" --lab="$counter")
  done
done