#!/bin/bash
while true; do
    read -p "Digite a data limite do Tweet mais antigo da busca no formato AAAA-MM-DD: " data_desde
    if [[ $data_desde =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && date -d "$data_desde" >/dev/null 2>&1
        then break
    else
        echo "Data incorreta. Tente novamente"
    fi
done
while true; do
    read -p "Digite a data do Tweet mais novo a ser buscado, no formato AAAA-MM-DD: " data_ate
    if [ "$data_ate" = "hoje" ]
        then break
    elif [[ $data_ate =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && date -d "$data_ate" >/dev/null 2>&1
        then
        break
    else
        echo "Data incorreta. Tente novamente"
    fi
done
while true; do
    read -p "Digite o numero de instâncias em paralelo a serem executadas (máx. de 4) : " instancias
    if [[ -n ${instancias//[0-9]/} ]]; then
        echo "Valor inválido. Tente novamente."
    elif [ "$instancias" -gt 4 ]; then
        echo "Valor maior que o numero maximo de instâncias, tente novamente."
    else
        break
    fi
done

python3 src/preprocess.py $instancias
# python3 coleta.py $data_ate $data_desde $instancias
for (( c=1; c<=$instancias; c++ ))
do
    gnome-terminal -e --command="python3 coleta.py $data_ate $data_desde $c"
done
wait
echo "Sessão de busca finalizada."