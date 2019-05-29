import csv
import sys
import os
import codecs
print("\nListando arquivos no diretorio:\n")
pasta = os.getcwd()
lista_arq = os.listdir(pasta)
for arquivo in lista_arq:
    print(arquivo)
print('')
lista_csv = []
while True:
    ans_1 = input("As opções de conversão são: \n 1. Converter todos os arquivos da pasta. \n 2.Converter apenas um arquivo em específico. \n\n Digite a opção desejada: ")
    if (int(ans_1) == 2):
        arquivo = (input("Digite o nome do arquivo .csv a ser convertido:\n")+'.csv')
        if arquivo not in lista_arq:
            print('\nO arquivo '+arquivo+' não existe. Tente novamente.\n')
            continue
        if not os.path.isdir("convert/"):
            os.makedirs("convert/")
        if not os.path.isfile("convert/"+arquivo):
            lista_csv.append(arquivo)
            break
        else:
            ans = input('O arquivo '+arquivo+' já foi convertido. Deseja sobrescrever o arquivo?(Y/N)')
            if(ans == 'N'):
                sys.exit("Erro: arquivo já convertido. Remova o arquivo da pasta e tente novamente.")
            elif(ans == 'Y'):
                os.remove("convert/"+arquivo)
                lista_csv.append(arquivo)
                break
            else:
                print("Comando não reconhecido. Digite Y para 'sim' e N para 'não' e em seguida pressione ENTER.")
    elif(int(ans_1) == 1):
        print("Escolhida a opção de converter todos os arquivos da pasta.")
        if not os.path.isdir("convert/"):
            os.makedirs("convert/")
        for file in lista_arq:
            if file.endswith(".csv"):
                while True:
                    if os.path.isfile("convert/"+file):
                        ans = input('O arquivo '+file+' já foi convertido. Deseja sobrescrever o arquivo?(Y/N)')
                        if(ans == 'N'):
                            sys.exit("Erro: arquivo já convertido. Remova o arquivo da pasta e tente novamente.")
                            break
                        elif(ans == 'Y'):
                            os.remove("convert/"+file)
                            lista_csv.append(file)
                            break
                        else:
                            print("Comando não reconhecido. Digite Y para 'sim' e N para 'não' e em seguida pressione ENTER.")
                    else:
                        lista_csv.append(file)
                        break
        break
    else:
        print('Opção não reconhecida, tente novamente.\n\n')
print(lista_csv)  
        
for arq_csv in lista_csv:
    w_out = open(("convert/"+arq_csv),'w')
    csvWriter = csv.writer(w_out, delimiter =',',quotechar ='"',quoting=csv.QUOTE_MINIMAL)
    with codecs.open(arq_csv, mode='r') as csv_file:
        csv_reader = csv.DictReader((l.replace('\x00', '') for l in csv_file),quotechar = '|',delimiter = ',')
        line_count = 0
        try:
            for row in csv_reader:
                if line_count == 0:
                  
                    #Checando se o arquivo está completo
                    print("\n\nColuna \t\t text \t\t\t\t\t presente") if ('text' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t rt_count \t\t\t\t presente") if ('rt_count' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t favorite_count \t\t\t presente") if ('favorite_count' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t user_followers \t\t\t presente") if ('user_followers' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t user_screen_name \t\t\t presente") if ('user_screen_name' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t rt_user \t\t\t\t presente") if ('rt_user' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t created_at \t\t\t\t presente") if ('created_at' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t rt_created_at \t\t\t\t presente") if ('rt_created_at' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t rt_id \t\t\t\t\t presente") if ('rt_id' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t tweet_id \t\t\t\t presente") if ('tweet_id' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t latitude \t\t\t\t presente") if ('latitude' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t longitude \t\t\t\t presente") if ('longitude' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t point \t\t\t\t\t presente") if ('point' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t user_id \t\t\t\t presente") if ('user_id' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t rt_user_id \t\t\t\t presente") if ('rt_user_id' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t timestamp \t\t\t\t presente") if ('timestamp' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t quoted_source \t\t\t\t presente") if ('quoted_source' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t rt_source \t\t\t\t presente") if ('rt_source' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t reply_id \t\t\t\t presente") if ('reply_id' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t reply_user \t\t\t\t presente") if ('reply_user' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t reply_user_id \t\t\t\t presente") if ('reply_user_id' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t quoted_id \t\t\t\t presente") if ('quoted_id' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t quoted_user \t\t\t\t presente") if ('quoted_user' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t quoted_user_id \t\t\t presente") if ('quoted_user_id' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t quoted_created_at \t\t\t presente") if ('quoted_created_at' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t place \t\t\t\t\t presente") if ('place' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t country \t\t\t\t presente") if ('country' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t country_code \t\t\t\t presente") if ('country_code' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t bounding_box \t\t\t\t presente") if ('bounding_box' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t media_url \t\t\t\t presente") if ('media_url' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    print("Coluna \t\t media_expanded_url \t\t\t presente") if ('media_expanded_url' in row) else sys.exit("Erro: planilha de leitura incompleta.")
                    
                    line_count += 1
                    
                    csvWriter.writerow(['tweet_text','retweet_count','favorite_count','followers_count','original_tweet_screen_name','retweet_screen_name',
                    'original_tweet_created_at','retweet_created_at','retweet_id','original_tweet_id','original_tweet_coordinates','retweet_coordinates',
                    'original_tweet_user_id','retweet_user_id','search_id','timestamp','search_string','is_retweet','type','source','in_reply_to_status_id',
                    'in_reply_to_screen_name','in_reply_to_user_id','quoted_id','quoted_screen_name','quoted_user_id','quoted_created_at','quoted_coordinates',
                    'place_name','place_fullname','place_country','place_cc','place_bb','media','media_url','media_expanded_url'])
                
                else:
                    if(row["latitude"] == ''):
                        coordinates = ''
                    else:    
                        coordinates = ("{'coordinates': "+"[{}, {}], 'type': '{}'".format(row["latitude"],row["longitude"],row["point"]) + "}")
                    is_retweet = True if(row["rt_id"] != '') else False
                    t_type = ''
                    source = ''
                
                    #Processando o tipo de Tweet:
                    if(row["rt_id"] != ''):
                        t_type = 'Retweet' 
                    elif(row["quoted_id"] != ''):
                        t_type = 'Quote'
                    else:
                        t_type = 'Tweet'
                    
                    #Processando a fonte de quote ou retweet:
                    if(row["quoted_source"] != ''):
                        source = row["quoted_source"] 
                    elif(row["rt_source"] != ''):
                        source = row["rt_source"] 
                    else:
                        source = row["source"]
                    
                    #Escrevendo os dados no novo formato:
                    csvWriter.writerow([row["text"],row["rt_count"],row["favorite_count"],row["user_followers"],row["user_screen_name"],row["rt_user"],
                    row["created_at"],row["rt_created_at"],row["rt_id"],row["tweet_id"],coordinates,'',row["user_id"],row["rt_user_id"],'',
                    row["timestamp"],'',is_retweet,t_type,source,row["reply_id"],row["reply_user"],row["reply_user_id"],row["quoted_id"],row["quoted_user"],
                    row["quoted_user_id"],row["quoted_created_at"],'',row["place"],row["place"],row["country"],row["country_code"],row["bounding_box"]
                    ,'',row["media_url"],row["media_expanded_url"]])
                        
                    line_count += 1
        except csv.Error:
            sys.exit('file %s, line %d: %s' % (arquivo, reader.line_num, e))
    csv_file.close()
    w_out.close()
    print('Arquivo {} concluído.'.format(arq_csv))
    print('Foram processadas {} linhas.'.format(line_count))
print('Fim da operação de conversão.')