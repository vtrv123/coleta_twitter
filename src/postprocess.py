import os
import sys
import shutil

#Acessando a pasta com os arquivos temporarios
temp = (os.path.dirname(os.getcwd())+'/temp/')
#Tenta apagar a pasta temporaria. Imprime um erro se falhar.
try:
    shutil.rmtree(temp)
except OSError as e:
    print ("Error: %s - %s." % (e.filename, e.strerror))