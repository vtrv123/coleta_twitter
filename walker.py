import csv
import sys
import os
import codecs
import numpy as np
import pandas as pd
print("\nListando arquivos .csv no diretorio:\n")
pasta = os.getcwd() + '/'
lista_csv = []
mapa_csv = {}
lista_conv = []

for path, subdirs, files in os.walk(pasta):
    for name in files:
        if name.endswith('.csv'):
            if name not in lista_csv:
                lista_csv.append(name)
            if name not in mapa_csv:
                mapa_csv[name] = []
                (mapa_csv[name]).append(os.path.join(path, name))
            else:
                (mapa_csv[name]).append(os.path.join(path, name))
lista_csv = list(dict.fromkeys(lista_csv))
for csvfile in lista_csv:
    print(csvfile)
print('')
print(mapa_csv)