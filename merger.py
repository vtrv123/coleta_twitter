#SCRIPT PARA FUNDIR DATASETS DE COLETA DO MESMO TÓPICO, ORGANIZANDO CRONOLOGICAMENTE
import csv
import sys
import os
import codecs
import numpy as np
import pandas as pd
print("\nListando arquivos .csv no diretorio:\n")
pasta = os.getcwd() + '/'
lista_csv = []
lista_conv = []
for arquivo in os.listdir(pasta):
    if arquivo.endswith('.csv'):
        lista_csv.append(arquivo)
        print(arquivo)
 
print('')

ans_1 = input("Opções de fusão de datasets: \n\n 1 - Especificar os nomes dos datasets a serem fundidos \n 2 - Escolher um radical comum e fundir todos os datasets derivados automaticamente. \n")

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
                print("Deseja continuar para a fase de processamento?\n")
                ans_3 = input("Digite 'Y' para SIM ou 'N' para NÃO:")
                if (ans_3 == 'Y'):
                    break
                elif(ans_3 == 'N'):
                    continue
            else:
                print("Comando não reconhecido. O último arquivo inserido foi: "+lista_conv[-1]+"\n Refaça a operação.")
                continue
    print('')
     
elif(int(ans_1) == 2):
    while True:
        print("Listando novamente os arquivos de dataset da pasta para a escolha do radical comum:")
        for arquivo in lista_csv:
            print(arquivo)
            print('')
         
        radical = input("Digite o nome do radical comum aos arquivos que serão fundidos (parte da palavra que é igual p/ todos):\n")
        match = [s for s in lista_csv if radical in s]
        print("Os arquivos encontrados foram:")
        for arquivo in match:
            print(arquivo)
            print('')
         
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
        break

frames = [ pd.read_csv(f, quotechar = '"') for f in lista_conv ]
result = pd.concat(frames, ignore_index=True)
# merged = None
# for merge_file in lista_conv:
#     if merged is None:
#         merged = pd.read_csv(merge_file, quotechar = '"')
#     else:
#         new = pd.read_csv(merge_file, quotechar = '"')
#         merged.append(new, ignore_index = True)
 
#print(df.iloc[0])
#df.columns = df.iloc[0]
#df.reindex(df.index.drop(0))
#df = df.sort('red label')
result.to_csv('result.csv', index = False)

# for arq_csv in lista_conv:
#     with open(arq_csv, mode='r') as csv_file:
#         csv_reader = csv.DictReader((l.replace('\x00', '') for l in csv_file),quotechar = '"',delimiter = ',')
#         line_count = 0
#         try:
#             for row in csv_reader:
#                 if line_count == 0:
                  
#                     check = ['tweet_text','retweet_count','favorite_count','followers_count','original_tweet_screen_name','retweet_screen_name',
#                     'original_tweet_created_at','retweet_created_at','retweet_id','original_tweet_id','original_tweet_coordinates','retweet_coordinates',
#                     'original_tweet_user_id','retweet_user_id','search_id','timestamp','search_string','is_retweet','type','source','in_reply_to_status_id',
#                     'in_reply_to_screen_name','in_reply_to_user_id','quoted_id','quoted_screen_name','quoted_user_id','quoted_created_at','quoted_coordinates',
#                     'place_name','place_fullname','place_country','place_cc','place_bb','media','media_url','media_expanded_url']
                    
#                     for name_check in check:
#                         if name_check not in row:
#                             sys.

#                     line_count += 1
                    
#                     csvWriter.writerow(['tweet_text','retweet_count','favorite_count','followers_count','original_tweet_screen_name','retweet_screen_name',
#                     'original_tweet_created_at','retweet_created_at','retweet_id','original_tweet_id','original_tweet_coordinates','retweet_coordinates',
#                     'original_tweet_user_id','retweet_user_id','search_id','timestamp','search_string','is_retweet','type','source','in_reply_to_status_id',
#                     'in_reply_to_screen_name','in_reply_to_user_id','quoted_id','quoted_screen_name','quoted_user_id','quoted_created_at','quoted_coordinates',
#                     'place_name','place_fullname','place_country','place_cc','place_bb','media','media_url','media_expanded_url'])
                
#                 else:
#                     if(row["latitude"] == ''):
#                         coordinates = ''
#                     else:    
#                         coordinates = ("{'coordinates': "+"[{}, {}], 'type': '{}'".format(row["latitude"],row["longitude"],row["point"]) + "}")
#                     is_retweet = True if(row["rt_id"] != '') else False
#                     t_type = ''
#                     source = ''
                
#                     #Processando o tipo de Tweet:
#                     if(row["rt_id"] != ''):
#                         t_type = 'Retweet' 
#                     elif(row["quoted_id"] != ''):
#                         t_type = 'Quote'
#                     else:
#                         t_type = 'Tweet'
                    
#                     #Processando a fonte de quote ou retweet:
#                     if(row["quoted_source"] != ''):
#                         source = row["quoted_source"] 
#                     elif(row["rt_source"] != ''):
#                         source = row["rt_source"] 
#                     else:
#                         source = row["source"]
                    
#                     #Escrevendo os dados no novo formato:
#                     csvWriter.writerow([row["text"],row["rt_count"],row["favorite_count"],row["user_followers"],row["user_screen_name"],row["rt_user"],
#                     row["created_at"],row["rt_created_at"],row["rt_id"],row["tweet_id"],coordinates,'',row["user_id"],row["rt_user_id"],'',
#                     row["timestamp"],'',is_retweet,t_type,source,row["reply_id"],row["reply_user"],row["reply_user_id"],row["quoted_id"],row["quoted_user"],
#                     row["quoted_user_id"],row["quoted_created_at"],'',row["place"],row["place"],row["country"],row["country_code"],row["bounding_box"]
#                     ,'',row["media_url"],row["media_expanded_url"]])
                        
#                     line_count += 1
#         except csv.Error:
#             sys.exit('file %s, line %d: %s' % (arquivo, reader.line_num, e))
#     csv_file.close()
#     w_out.close()
#     print('Arquivo {} concluído.'.format(arq_csv))
#     print('Foram processadas {} linhas.'.format(line_count))
# print('Fim da operação de conversão.')