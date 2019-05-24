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
from datetime import datetime,timezone,timedelta
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
    retry_count = 0
    while True: #loop while para tentar novamente a busca caso exceda o limite de buscas
        if(retry_count > 3):
            print('Numero de tentativas excedido, esperando a API resetar para tentar novamente.')
            print('Tempo de espera: 5 min.')
            time.sleep(300)
        try:
            if(maxID == 0):
                return(api.search(q = busca, count = tweet_por_pag, tweet_mode = modo, lang = lingua))
            else:
                return(api.search(q = busca, count = tweet_por_pag, tweet_mode = modo, lang = lingua, max_id = maxID))
        except TwythonRateLimitError:
            trocaKey()
            retry_count += 1
            continue
        except(Timeout,ConnectionError,ProtocolError,TwythonError) as exc:
            print(exc)
            print('Erro na busca: '+ busca)
            print('Esperando 5 min para reestabelecer conexão')
            time.sleep(300)
            retry_count += 1
            continue
        break

#Função para escrever as informações dos Tweets no arquivo csv
def w_tweet(tweets,writer):

    for tweet in tweets:
    
        #Inicializando as variaveis
        point = ''
        latitude = ''
        longitude = ''
        coordinates = ''
        media_expanded_url = ''
        media_url = ''
        media_type = ''
        urls = ''
        hashtags = ''
        mentions = ''
        mentions_id = ''
        place = ''
        country = ''
        country_code = ''
        bounding_box = ''
        is_retweet = ''
        rt_text = ''
        rt_id = ''
        rt_created_at = ''
        rt_source = ''
        rt_user = ''
        rt_user_id = ''
        quoted_text = ''
        quoted_id = ''
        quoted_created_at = ''
        quoted_source = ''
        quoted_user = ''
        quoted_user_id = ''
        user_url = ''
        user_description = ''
        user_location = ''
        user_default_layout = ''
        user_default_image = ''
        user_protected_tweets = ''
        original_tweet_created_at = ''
        retweet_created_at = ''
        quoted_created_at = ''

        #Se o tweet foi retuitado
        if('retweeted_status' in tweet):
            is_retweet = True
            text = limpa_tweet(tweet['retweeted_status']['full_text'])
            rt_id = tweet['retweeted_status']['id_str']
            rt_user = tweet['retweeted_status']['user']['screen_name']
            rt_user_id = tweet['retweeted_status']['user']['id_str']
            rt_created_at = tweet['retweeted_status']['created_at']
            retweet_created_at = datetime.strptime(rt_created_at, "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%d %H:%M:%S")
            rt_source = tweet['retweeted_status']['source']
            head, sep, tail = rt_source.partition('<')
            head, sep, tail = rt_source.partition('>')
            rt_source = tail.replace('</a>', '')
            tweet_type = 'Retweet'
            #Se for uma quote:
            if 'quoted_status' in tweet['retweeted_status']:
                #Salva no espaço de quotes
                tweet['quoted_status'] = tweet['retweeted_status']['quoted_status']
        #Se nao fui retuitado, salva o tuite normal:
        else:
            is_retweet = False
            text = limpa_tweet(tweet['full_text'])
        
        #Salvando dados comuns
        tweet_id = tweet['id_str']
        rt_count = tweet['retweet_count']
        favorite_count = tweet['favorite_count']
        lang = tweet['lang']
        created_at = tweet['created_at']
        tweet_type = 'Tweet'

        #Salvando fonte
        source = tweet['source']
        head, sep, tail = source.partition('<')
        head, sep, tail = source.partition('>')
        source = tail.replace('</a>', '')

        #Salvando respostas
        reply_id = tweet['in_reply_to_status_id_str']
        reply_user = tweet['in_reply_to_screen_name']
        reply_user_id = tweet['in_reply_to_user_id_str']
        
        #Se for um Tweet de resposta, deixar marcado
        if reply_user_id is not None:
            tweet_type = 'Reply'

        #Se for um Tweet de citação
        if 'quoted_status' in tweet:
            # Pegar o texto citado
            quoted_text = (tweet['quoted_status']['full_text']\
                        if 'full_text' in tweet['quoted_status']\
                        else tweet['quoted_status']['text'])\
                        .replace('\n', ' ')\
                        .replace('\r', ' ')\
                        .replace(',', ' ')\
                        .replace('|', ' ')
            # Dados do tweet citado
            quoted_id = tweet['quoted_status']['id_str']
            quoted_user = tweet['quoted_status']['user']['screen_name']
            quoted_user_id = tweet['quoted_status']['user']['id_str']
            quoted_created_at = tweet['quoted_status']['created_at']
            quoted_created_at = datetime.strptime(quoted_created_at, "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%d %H:%M:%S")
            quoted_source = tweet['quoted_status']['source']
            head, sep, tail = quoted_source.partition('<')
            head, sep, tail = quoted_source.partition('>')
            quoted_source = tail.replace('</a>', '')
            # Verifica se foi um retweet
            tweet_type = 'Retweet' if 'retweeted_status' in tweet else 'Quote' # text.startswith('RT @')

        
        #Salvando e formatando datas
        datetime_created_at = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
        timestamp = int(datetime_created_at.replace(tzinfo=timezone.utc).timestamp())
        original_tweet_created_at = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%d %H:%M:%S")
        

        # Geolocalização
        if 'coordinates' in tweet:
            try: # add location data
                latitude = tweet['coordinates']['coordinates'][1]
                longitude = tweet['coordinates']['coordinates'][0]
                point = 'Point'
                coordinates = ("{'coordinates': "+"[{}, {}], 'type': '{}'".format(latitude,longitude,point) + "}")
            except: pass

        # Local
        if 'place' in tweet:
            try: # add place data
                place = tweet['place']['full_name']
                country = tweet['place']['country']
                country_code = tweet['place']['country_code']
                bounding_box = tweet['place']['bounding_box']['coordinates']
            except: pass

        # Dados linkados ao tweet
        if 'entities' in tweet:
            # media URL
            if 'media' in tweet['entities']:
                range_media = range(len(tweet['entities']['media']))
                for i in range_media:
                    if i < max(range_media):
                        media_expanded_url = str(tweet['entities']['media'][i]['expanded_url'])
                        media_type = str(tweet['entities']['media'][i]['type'])
                    else: 
                        media_url = str(tweet['entities']['media'][i]['media_url'])
                        media_type = str(tweet['entities']['media'][i]['type'])
            # external URLs
            if 'urls' in tweet['entities']:
                range_urls = range(len(tweet['entities']['urls']))
                for i in range_urls:
                    if i < max(range_urls):
                        urls = urls+'"'+str(tweet['entities']['urls'][i]['expanded_url'])+'"'+", "
                    else: urls = urls+'"'+str(tweet['entities']['urls'][i]['expanded_url'])+'"'
            # hashtags in text
            if 'hashtags' in tweet['entities']:
                range_hashtags = range(len(tweet['entities']['hashtags']))
                for i in range_hashtags:
                    if i < max(range_hashtags):
                        hashtags = hashtags+'#'+str(tweet['entities']['hashtags'][i]['text'])+", "
                    else: hashtags = hashtags+'#'+str(tweet['entities']['hashtags'][i]['text'])
            # mentions in text
            if 'user_mentions' in tweet['entities']:
                range_mentions = range(len(tweet['entities']['user_mentions']))
                for i in range_mentions:
                    if i < max(range_mentions):
                        mentions = mentions+str(tweet['entities']['user_mentions'][i]['screen_name'])+", "
                        mentions_id = mentions_id+str(tweet['entities']['user_mentions'][i]['id_str'])+", "
                    else: # add both user name and ID to mentions
                        mentions = mentions+str(tweet['entities']['user_mentions'][i]['screen_name'])
                        mentions_id = mentions_id+str(tweet['entities']['user_mentions'][i]['id_str'])
        
        # Extraindo dados do usuario
        user_name = tweet['user']['name']
        user_screen_name = tweet['user']['screen_name']
        user_id = tweet['user']['id_str']
        user_tweets = tweet['user']['statuses_count']
        user_followers = tweet['user']['followers_count']
        user_following = tweet['user']['friends_count']
        user_listed = tweet['user']['listed_count']
        user_favorited = tweet['user']['favourites_count']
        user_created_at = tweet['user']['created_at']
        user_time_zone = tweet['user']['time_zone']
        user_lang = tweet['user']['lang']
        user_image = tweet['user']['profile_image_url']
        user_verified = str(tweet['user']['verified']).replace('False', '')

        if tweet['user']['url']:
            user_url = tweet['user']['url'].replace('\n', ' ').replace('\r', ' ')

        if tweet['user']['description']:
            user_description = tweet['user']['description'].replace('\n', ' ').replace('\r', ' ')

        if tweet['user']['location']:
            user_location = tweet['user']['location'].replace('\n', ' ').replace('\r', ' ')

        if tweet['user']['default_profile']:
            user_default_layout = tweet['user']['default_profile']

        if tweet['user']['default_profile_image']:
            user_default_image = tweet['user']['default_profile_image']

        if tweet['user']['protected']:
            user_protected_tweets = tweet['user']['protected']

        # Formando o link do Tweet
        link = 'https://twitter.com/' + user_screen_name + '/status/' + tweet_id

        # workaround for truncated tweet text:
        # extract full text from retweeted status
        if text.endswith('…')\
        and tweet_type == 'Retweet':
                a,b = text.rstrip('…').split(': ',1)
                if rt_text.startswith(b):
                    text = str(a+': '+rt_text)        

        #Escrevendo as infos do Tweet na planilha
        #["tweet_text","retweet_count","favorite_count","followers_count","original_tweet_screen_name","retweet_screen_name",
       # "original_tweet_created_at","retweet_created_at","retweet_id","original_tweet_id","original_tweet_coordinates","retweet_coordinates",
        #"original_tweet_user_id","retweet_user_id","search_id","timestamp","search_string","is_retweet","type","source",
        #"in_reply_to_status_id","in_reply_to_screen_name","in_reply_to_user_id","quoted_id","quoted_screen_name","quoted_user_id",
        #"quoted_created_at","quoted_coordinates","place_name","place_fullname","place_country","place_cc","place_bb","media","media_url","media_expanded_url"])

        writer.writerow([text, rt_count, favorite_count, user_followers, rt_user, user_name, original_tweet_created_at,
                     retweet_created_at, rt_id, tweet_id, coordinates, ' ', user_id, rt_user_id, " ", timestamp, ' ',
                     is_retweet, tweet_type, source, reply_id, reply_user, reply_user_id, quoted_id, quoted_user, quoted_user_id,
                     quoted_created_at, ' ', place, place, country, country_code, bounding_box, media_type, media_url, media_expanded_url])
    return

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
    data = datetime.now()
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
        data_desde = (datetime.now()+timedelta(days=1)).strftime("%Y-%m-%d")
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
        csvWriter = csv.writer(csvFile, delimiter =',',quotechar ='|',quoting=csv.QUOTE_MINIMAL)
        #Escrevendo o cabeçalho do arquivo
        csvWriter.writerow(["tweet_text","retweet_count","favorite_count","followers_count","original_tweet_screen_name","retweet_screen_name",
        "original_tweet_created_at","retweet_created_at","retweet_id","original_tweet_id","original_tweet_coordinates","retweet_coordinates",
        "original_tweet_user_id","retweet_user_id","search_id","timestamp","search_string","is_retweet","type","source",
        "in_reply_to_status_id","in_reply_to_screen_name","in_reply_to_user_id","quoted_id","quoted_screen_name","quoted_user_id",
        "quoted_created_at","quoted_coordinates","place_name","place_fullname","place_country","place_cc","place_bb","media","media_url","media_expanded_url"])
        # csvWriter.writerow(["text", "reply_user_id", "user_screen_name", "tweet_id", "user_id", "lang", "source", "user_image", "point",
        #              "latitude", "longitude", "created_at", "timestamp", 'tweet_type', 'rt_count', 'favorite_count', 'place',
        #              'country', 'country_code', 'hashtags', 'urls', 'media_expanded_url', 'media_url', 'bounding_box',
        #              'mentions', 'mentions_id', 'reply_user', 'reply_id', 'rt_text', 'rt_user_id', 'rt_user', 'rt_id', 'rt_source',
        #              'rt_created_at', 'quoted_text', 'quoted_id', 'quoted_user', 'quoted_user_id', 'quoted_created_at',
        #              'quoted_source', 'user_name', 'user_tweets', 'user_followers', 'user_following', 'user_listed',
        #              'user_favorited', 'user_created_at', 'user_lang', 'user_location', 'user_time_zone', 'user_description',
        #              'user_url', 'user_protected_tweets', 'user_default_layout', 'user_default_image', 'user_verified', 'link'])
    else:
        print('O arquivo'+saida+busca+'.csv'+' já foi buscado. Avançando para o próximo tópico.')
        continue
    #Concatenando a string de busca
    busca = (busca + ' since:' + data_ate +' until:'+data_desde)
    maxID = 0
    dados = [1,2,3]

    #Realizamos o loop ate o limite de requisicoes da API (180 req por 15 min)
    while(len(dados)>1):
        #Busca de tweets por assunto, numero de tweets, lingua e texto integral, observando o ultimo tweet buscado em max_id
        result_busca = buscar_tweets(busca = busca, tweet_por_pag = tweet_por_pag, modo = 'extended',lingua = 'pt', maxID = maxID)
        #Abrindo o objeto tweet retornado pela busca
        dados = result_busca['statuses']
        #Lendo as partes de interesse do tuite e salvando no arquivo
        w_tweet(dados,csvWriter)
        # for dado in dados:
        #     #Se o tweet foi retuitado
        #     if('retweeted_status' in dado):
        #         csvWriter.writerow([dado['user']['name'],limpa_tweet(dado['retweeted_status']['full_text']),dado['created_at'],dado['retweet_count']])
        #     #Se nao fui retuitado, salva o tuite normal:
        #     else:
        #         csvWriter.writerow([dado['user']['name'],limpa_tweet(dado['full_text']), dado['created_at'],dado['retweet_count']])
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