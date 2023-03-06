#!/bin/bash

while getopts ":t:u:b:m:" opt; do
  case $opt in
    t) t="$OPTARG"
    ;;
    u) u="$OPTARG"
    ;;
    b) b="$OPTARG"
    ;;
    m) m="$OPTARG"
    ;;
    \?) echo "Opção inválida: -$OPTARG" >&2
    ;;
  esac
done

echo "Argumento 1: $t"
echo "Argumento 2: $u"
echo "Argumento 3: $b"
echo "Argumento 4: $m"


# Executa o comando com base nos valores dos argumentos
for ((part=0; part <= 9; part++)); do
    python cli/extract.py --part=$part --datatype=$t --update=$u --batch_size=$b
done
