import sys
import os
import shutil
#Acessando o numero de instancias do script a serem executadas em paralelo
if(len(sys.argv)< 2):
    print('Erro, numero de argumentos insuficientes. Terminando o programa.')
    sys.exit()
n_inst = int(sys.argv[1])
#Acessando a pasta de arquivos temporários
root = os.getcwd()
#Checando se a pasta temp foi excluida
if os.path.exists(root+'/temp/'):
    try:
        shutil.rmtree(root+'/temp/')
    except OSError as e:
        print ("Error: %s - %s." % (e.filename, e.strerror))
#Criando a nova pasta /temp/
os.makedirs(root+'/temp/')
#Abrindo o arquivo de tópicos
conta_txt = 0
for file in os.listdir(root):
        if file.endswith(".txt"):
            conta_txt += 1
if (conta_txt > 1):
    print('Erro, existe mais de um arquivo de topicos na pasta. Terminando o programa.')
    sys.exit()
for file in os.listdir(root):
        if file.endswith(".txt"):
            print('Utilizando as palavras-chave no arquivo: '+file)
            arq_topicos = root+'/'+file
#Lendo o arquivo com os topicos de busca
leitor = open(arq_topicos, "r")
lista_topicos = []
#Armazenando os trending topics em uma lista
for linha in leitor:
    lista_topicos.append(linha.rstrip())
leitor.close()
#Calculando a divisão dos topicos entre as instancias:
num_topicos = len(lista_topicos)
sobra = (num_topicos % n_inst)
parte = (num_topicos - sobra)/n_inst
parte = int(parte)
#Dividindo os topicos em arquivos separados:

for k in range(n_inst):
    nome_arquivo = root+'/temp/topics'+str(k+1)+'.txt'
    escritor = open(nome_arquivo,"w")
    #TA ERRADO, CORRIGIR
    for i in range(parte):
        indice = i + parte*k
        escritor.write(lista_topicos[indice]+'\n')
    if (k == (n_inst-1)):
        if (sobra != 0):
            for j in range(((k+1)*parte),((k+1)*parte)+sobra,1):
                escritor.write(lista_topicos[j]+'\n')        
    escritor.close()