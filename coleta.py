#Os argumentos a serem lidos pelo script são:
#sys.argv[1] = data da coleta
#sys.argv[2] = data limite da busca
#sys.argv[3] = instancia da execucao paralela

import twython
import json
import csv
import time
import sys
import os
import datetime
from tqdm import tqdm
from requests.exceptions import Timeout, ConnectionError
from urllib3.exceptions import ProtocolError
from twython.exceptions import TwythonError, TwythonRateLimitError, TwythonAuthError
#Abaixo temos as chaves de autenticacao para utilizar a API do Twitter
#Favor não utilizar as chaves abaixo, são de uso individual

CONSUMER_KEY = []
CONSUMER_SECRET = []
# CONSUMER_KEY0 = 'dZouIEWX05CMZ4fkbGjAk2yVj'
# CONSUMER_SECRET0 = 'Q0ZJY6kwtjNJ3lv8SDBjQXuZQnqCMOzkxf5jeMr8Cf7s9R2axH'
# CONSUMER_KEY1 = 'jUM9tdftJGqdktEuYw9M30ok7'
# CONSUMER_SECRET1 = 'SVaspZH9EWXeJzkBdldmAcCCLSZK4N6HFqlznlVOlhGCeI8DNI'
# CONSUMER_KEY2 = 'GPcSV2iY7aHDEBV8HaKkE0wlB'
# CONSUMER_SECRET2 = 'bkiCa6ty14nwStMJJUuamew2d9Lq4ToD7PkNzlbGLtM2mqwx8b'

#Criando a variavel para numero de tweets buscados por requisicao
tweet_por_pag = 100

#Criando o contador do numero de requisicoes da API 
contador_req = 0

#Criando um contador para o conjunto de chaves
contador_key = 0

#Criando um cronometro para a execucao otima do codigo
crono = 0

#Criando uma variavel para armazenar o tempo de execução do código

t_exec = time.time()

#Criando uma variavel para armazenar o diretorio dos resultados da coleta
saida = ''

# #Criando uma classe para fazer o log das saídas de erro e mensagens

class Logger_stderr(object):
    def __init__(self):
        self.terminal = sys.stderr
        self.log = open("temp/log"+str(sys.argv[3])+".txt", "a+")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)  

class Logger_stdout(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("temp/log"+str(sys.argv[3])+".txt", "a+")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)  

sys.stderr = Logger_stderr()
sys.stdout = Logger_stdout()

#Cria uma funcao para realizar as buscas na API e evitar erros:
def buscar_tweets(busca, tweet_por_pag, modo, lingua, maxID = 0):
    try:
        if(maxID == 0):
            return(api.search(q = busca, count = tweet_por_pag, tweet_mode = modo, lang = lingua))
        else:
            return(api.search(q = busca, count = tweet_por_pag, tweet_mode = modo, lang = lingua, max_id = maxID))
    except TwythonRateLimitError:
        trocaKey()
        if(maxID == 0):
            return(api.search(q = busca, count = tweet_por_pag, tweet_mode = modo, lang = lingua))
        else:
            return(api.search(q = busca, count = tweet_por_pag, tweet_mode = modo, lang = lingua, max_id = maxID))
    except(Timeout,ConnectionError,ProtocolError,TwythonError) as exc:
        print(exc)
        print('Erro na busca: '+ busca)
        print('Esperando 5 min para reestabelecer conexão')
        time.sleep(300)
        if(maxID == 0):
            return(api.search(q = busca, count = tweet_por_pag, tweet_mode = modo, lang = lingua))
        else:
            return(api.search(q = busca, count = tweet_por_pag, tweet_mode = modo, lang = lingua, max_id = maxID))

#Função para limpar os textos recuperados dos Tweets
def limpa_tweet(tweet_text):
    tweet_text = tweet_text.rstrip()
    tweet_text = tweet_text.replace('\n','')
    tweet_text = tweet_text.replace(',',' ')
    tweet_text = tweet_text.replace('|',' ')
    tweet_text = tweet_text.replace('\r','')
    return(tweet_text)

#Função para trocar as keys de autenticacao e ganhar mais tempo de processamento
def trocaKey():
    global contador_key
    global api
    global crono
    #Checa se a lista de keys está vazia
    if(len(CONSUMER_KEY) == 0):
        print("ERRO: Não há keys suficientes para executar esta instancia")
        sys.exit()

    #Checando se já utilizamos todas as keys
    if(contador_key == len(CONSUMER_KEY)):
        #Calculando o tempo de espera para o reset da API:
        tempo_exec = time.time() - crono
        tempo_espera = 900 - tempo_exec
        if(tempo_espera > 0):
            print('Entrando na espera de '+str(tempo_espera)+' segundos para resetar a API')
            time.sleep(tempo_espera)
            contador_key = 0
            trocaKey()  
        else:
            print('Sem tempo de espera!')
            contador_key = 0
            trocaKey()
        return

    elif(contador_key == 0):
        try:
            #Fazendo login na API
            twitter = twython.Twython(CONSUMER_KEY[contador_key], CONSUMER_SECRET[contador_key], oauth_version=2)
            #Obtém a chave de acesso
            ACCESS_TOKEN = twitter.obtain_access_token()
            #Recria o objeto Twython, usando a consumer key da aplicação e a chave de acesso obtida anteriormente. Isso faz aumentar a capacidade de requisições que se pode fazer no twitter.
            api = twython.Twython(CONSUMER_KEY[contador_key], access_token=ACCESS_TOKEN)
        except TwythonAuthError:
            print("Erro de login com a key:"+CONSUMER_KEY[contador_key]+" e secret:"+CONSUMER_SECRET[contador_key])
            #Reseta o cronometro
            crono = time.time()
            #Deletando a key e secret com erro
            del CONSUMER_KEY[contador_key]
            del CONSUMER_SECRET[contador_key]
            #Avançando para a proxima key valida
            contador_key += 1
            trocaKey()
            return
        else:
            #Reseta o cronometro
            crono = time.time()
            #Incrementa o contador das chaves
            contador_key += 1
            return

    elif(contador_key > len(CONSUMER_KEY)):
        print('O contador de keys estourou, erro fatal.')
        sys.exit()
        return
    else:
        try:
            #cria um objeto Twython, esse objeto representa a conexão com a API feita no Twitter.
            twitter = twython.Twython(CONSUMER_KEY[contador_key], CONSUMER_SECRET[contador_key], oauth_version=2)
            #Obtém a chave de acesso
            ACCESS_TOKEN = twitter.obtain_access_token()
            #Recria o objeto Twython, usando a consumer key da aplicação e a chave de acesso obtida anteriormente. Isso faz aumentar a capacidade de requisições que se pode fazer no twitter.
            api = twython.Twython(CONSUMER_KEY[contador_key], access_token=ACCESS_TOKEN)
        except TwythonAuthError:
            print("Erro de login com a key: \n"+CONSUMER_KEY[contador_key]+"\n E secret: \n"+CONSUMER_SECRET[contador_key])
            #Reseta o cronometro
            crono = time.time()
            #Avançando para a proxima key valida
            contador_key += 1
            trocaKey()
            return    
        else:
            #Incrementa o contador das chaves
            contador_key += 1
            return

#Função para criar diretorios para armazenar os resultados das buscas
def cria_pasta():
    global saida
    data = datetime.datetime.now()
    data = data.strftime("%Y-%m-%d %H:%M")
    saida = 'Resultados/'+data+'/'
    time.sleep(0.1*float(instancia))
    if not os.path.exists(saida):
        os.makedirs(saida)

#Funcao para extrair os parametros de execucao paralela
def exec_paralela(instancia):
    global CONSUMER_KEY
    global CONSUMER_SECRET
    try:
        #Lendo o arquivo com as keys dessa instancia de execucao
        leitor = open('src/pass/'+'key'+str(instancia)+'.txt', "r")
    except (OSError, IOError) as e:
        print(e)
        print("Numero de keys insuficiente para executar esta instância. Terminando o programa.")
    else:
        #Armazenando os trending topics em uma lista
        for key in leitor:
            CONSUMER_KEY.append(key.rstrip())
        leitor.close()
        #Lendo o arquivo com os secrets dessa instancia de execucao
        leitor = open('src/pass/'+'secret'+str(instancia)+'.txt', "r")
        for secret in leitor:
            CONSUMER_SECRET.append(secret.rstrip())
        leitor.close()

#Testando os argumentos do script
if(len(sys.argv) < 4):
    print('Numero de argumentos insuficiente, leia no código-fonte os argumentos necessários')
    sys.exit()

else:
    #Lendo a instancia de execucao e extraindo os parametros
    instancia = sys.argv[3]
    exec_paralela(instancia)
    
    #Criando a variavel para armazenar a data limite de busca (buscar ate essa data)
    #A data deve estar em formato AAAA-MM-DD
    data_ate = sys.argv[2]

    #Criando a variavel para armazenar ou outro limite da busca
    if(sys.argv[1] == 'hoje'):
        data_desde = (datetime.datetime.now()+datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        data_desde = sys.argv[1]
    #Criando a pasta para armazenar os dados da coleta
    cria_pasta()

#Lendo o arquivo com os trending topics
nome_arq = 'temp/'+'topics'+str(instancia)+'.txt'
leitor = open(nome_arq, "r")
lista_topicos = []

#Armazenando os trending topics em uma lista
for linha in leitor:
    lista_topicos.append(linha.rstrip())
leitor.close()
#Retirando elementos repetidos da lista
lista_topicos = list(dict.fromkeys(lista_topicos))

#Fazendo o primeiro login na API do Twitter
trocaKey()

#Feedback das buscas a serem realizadas
print('\n Realizando buscas nos seguintes topicos:\n')
print('Instancia num.'+str(instancia)+':')
print(lista_topicos)
#Criando uma lista de topicos restantes a serem buscados
lista_restante = lista_topicos.copy()
nome_rest = 'temp/'+'topicos_restantes_'+str(instancia)+'.txt'
updater = open(nome_rest, "w")
#Iniciando a coleta de dados
for busca in tqdm(lista_topicos):
    #Criando um arquivo csv com o nome do topico para armazenar os tweets
    if not os.path.isfile(saida+busca+'.csv'):
        csvFile = open((saida+busca+'.csv'),'a')
        csvWriter = csv.writer(csvFile, delimiter =' ',quotechar =',',quoting=csv.QUOTE_MINIMAL)
    else:
        print('O arquivo'+saida+busca+'.csv'+' já foi buscado. Avançando para o próximo tópico.')
        continue
    #Concatenando a string de busca
    busca = (busca + ' since:' + data_ate +' until:'+data_desde)
    # #Realizando a primeira busca de tweets por assunto, numero de tweets, lingua e texto integral
    # result_busca = buscar_tweets(busca = busca, tweet_por_pag = tweet_por_pag, modo = 'extended',lingua = 'pt')
    # #Abrindo o objeto tweet retornado pela busca
    # dados = result_busca['statuses']
    # #Lendo as partes de interesse do tuite e salvando no arquivo
    # for dado in dados:
    #     #Se o tweet foi retuitado
    #     if('retweeted_status' in dado):
    #         csvWriter.writerow([dado['user']['name'],limpa_tweet(dado['retweeted_status']['full_text']),dado['created_at'],dado['retweet_count']])            
    #     #Se nao fui retuitado, salva o tuite normal:
    #     else:
    #         csvWriter.writerow([dado['user']['name'],limpa_tweet(dado['full_text']), dado['created_at'],dado['retweet_count']])
    
    # #Com o ID do tweet mais antigo, atualizamos o parametro max_id para utilizar no loop
    # if dados: 
    #     maxID = dados[-1]['id']
    maxID = 0
    dados = [1,2,3]

    #Realizamos o loop ate o limite de requisicoes da API (180 req por 15 min)
    while(len(dados)>1):
        #Busca de tweets por assunto, numero de tweets, lingua e texto integral, observando o ultimo tweet buscado em max_id
        result_busca = buscar_tweets(busca = busca, tweet_por_pag = tweet_por_pag, modo = 'extended',lingua = 'pt', maxID = maxID)
        #Abrindo o objeto tweet retornado pela busca
        dados = result_busca['statuses']
        #Lendo as partes de interesse do tuite e salvando no arquivo
        for dado in dados:
            #Se o tweet foi retuitado
            if('retweeted_status' in dado):
                csvWriter.writerow([dado['user']['name'],limpa_tweet(dado['retweeted_status']['full_text']),dado['created_at'],dado['retweet_count']])
            #Se nao fui retuitado, salva o tuite normal:
            else:
                csvWriter.writerow([dado['user']['name'],limpa_tweet(dado['full_text']), dado['created_at'],dado['retweet_count']])
        #Atualizando o id do ultimo tweet buscado
        if dados:
            maxID = dados[-1]['id']
    #Fechando o arquivo csv
    csvFile.close()
    #Atualizando a lista de tópicos restantes
    lista_restante.pop(0)
    updater = open(nome_rest, "w")
    for topico_rest in lista_restante:
        updater.write(topico_rest+'\n')

#Calculando o tempo de execucao final do codigo
t_exec = time.time() - t_exec

#Finalizando o codigo com o comando do usuario
print('Coleta finalizada.')
print('Pressione ENTER para fechar o programa.')
input()