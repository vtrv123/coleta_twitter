#SCRIPT PARA FUNDIR DATASETS DE COLETA DO MESMO TÓPICO, ORGANIZANDO DE ACORDO COM O USUÁRIO
import csv
import sys
import os
import codecs
import numpy as np
import pandas as pd
print("\nListando arquivos .csv no diretorio:\n")
pasta = os.getcwd() + '/'
lista_csv = []
lista_path = []
lista_conv = []

for path, subdirs, files in os.walk(pasta):
    for name in files:
        if name.endswith('.csv'):
            if name not in lista_csv:
                lista_csv.append(name)
            lista_path = os.path.join(path, name)
for csvfile in lista_csv:
    print(csvfile)
print('')

ans_1 = input("Opções de fusão de datasets: \n\n 1 - Especificar os nomes dos datasets a serem fundidos \n 2 - Escolher um radical comum e fundir todos os datasets derivados automaticamente.\n Resposta:")

if (int(ans_1) == 1):
    while True:
        arquivo = (input("Digite o nome do arquivo .csv a ser adicionado à conversão: (Não precisa colocar .csv)\n")+'.csv')
        if arquivo not in lista_csv:
            print('\nO arquivo '+arquivo+' não existe. Tente novamente.\n')
            continue
        elif arquivo in lista_conv:
            print('\nO arquivo '+arquivo+' já foi adicionado para fusão. Tente novamente.\n')
        else:
            lista_conv.append(arquivo)
            print('Arquivo '+arquivo+' inserido com sucesso. Deseja adicionar mais arquivos?\n')
            ans_2 = input("Digite 'Y' para SIM ou 'N' para NÃO:")
            if (ans_2 == 'Y'):
                continue
            elif(ans_2 == 'N'):
                print('Os arquivos adicionados foram:')
                for arq in lista_conv:
                    print(arq)
                break
            else:
                print("Comando não reconhecido. O último arquivo inserido foi: "+lista_conv[-1]+"\n Refaça a operação.")
                continue
    print('')
     
elif(int(ans_1) == 2):
    while True:
        print("\nListando novamente os arquivos de dataset da pasta para a escolha do radical comum:\n")
        for arquivo in lista_csv:
            print(arquivo)
         
        radical = input("\nDigite o nome do radical comum aos arquivos que serão fundidos (parte da palavra que é igual p/ todos)\n Resposta:")
        match = [s for s in lista_path if radical in s]
        print("\nOs arquivos encontrados foram:")
        for arquivo in match:
            print(arquivo)
         
        print("Deseja proceder com a fusão dos arquivos acima?\n")
        ans_4 = input("Digite 'Y' para 'SIM' e 'N' para 'NÃO':")
        if (ans_4 == 'Y'):
            lista_conv = match.copy()
            break
        elif(ans_2 == 'N'):
            continue
        else:
            print("Comando não reconhecido. Refaça a operação.\n")
            continue
else:
    print("Comando não reconhecido, respostas possíveis: 1 ou 2. Execute novamente o programa.")
    sys.exit()

while True:
    end_file = input("\nEscolha o nome do arquivo de saída a ser gerado: ")
    if not os.path.isdir("merge/"):
        os.makedirs("merge/")
    if os.path.isfile("merge/"+end_file):
        print("Já existe um arquivo com o nome: "+end_file+"\n Digite outro nome e tente novamente.")
        continue
    else:
        end_file = 'merge/'+end_file +'.csv'
        break

print('Como você deseja organizar o dataset?')
lista_var = []
order = False
while True:

    print('''
    Escolha uma das variáveis abaixo:
    1.  Data do tweet
    2.  Número de curtidas
    3.  Número de seguidores usuário
    4.  Número de retweets
    5.  Data de criação do usuário
    ''')
    ans_5 = input('Digite a opção escolhida a seguir:')
    
    if not lista_var:
        while True:
            order = input('\nDigite "A" para ordem ascedente, ou "D" para ordem descendente:')
            if order not in ['A','D']:
                print('\nResposta não reconhecida. Tente novamente.')
                continue
            else:
                break
        order = True if (order == 'A') else False

    if ans_5 == '1':
        lista_var.append('original_tweet_id')
    elif ans_5 == '2':
        lista_var.append('favorite_count')
    elif ans_5 == '3':
        lista_var.append('followers_count')
    elif ans_5 == '4':
        lista_var.append('retweet_count')
    elif ans_5 == '5':
        lista_var.append('original_tweet_user_id')
    else:
        print("Comando não reconhecido. Tente novamente.")
        continue
    
    while True:
        print("\nDeseja adicionar mais alguma variável?")
        ans_7 = input("\nDigite 'S' para Sim ou 'N' para Não:")
        if ans_7 not in ['S','N']:
            print("Comando não reconhecido. Tente novamente.")
            continue
        else:
            break
    
    if ans_7 == 'S':
        continue
    else:
        frames = [ pd.read_csv(f, quotechar = '"') for f in lista_conv ]
        result = pd.concat(frames, ignore_index=True)
        result.sort_values(by=lista_var, inplace=True, ascending=(order))
        result.to_csv(end_file,index = False)
        break

print("Todas as conversões já foram concluídas. Finalizando o programa...")