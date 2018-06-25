# -*- coding: utf-8 -*-
from ftplib import FTP as ftp
import os, datetime
import pickle
import sys, shutil
import DataAccess.Twitters
import Helpers.Utils
import spacy
import re
from SqlServer import ImportToSqlServer
from SqlServer import Sentimento
import random

ftp_host = ''
ftp_user = ''
ftp_passwd = ''
path_local = './crawler_results/'
path_local_processed = './crawler_results_processed/'
nlp = spacy.load('pt_core_news_sm')


def transferFiles(maxtransfer=100):
    if not (os.path.exists(path_local)):
        os.mkdir(path_local)
    #
    client = ftp(host=ftp_host)
    client.login(user=ftp_user, passwd=ftp_passwd)
    #
    lstFiles = client.nlst()
    #    lstFiles = os.listdir(path_local)

    print("Total de arquivos para transferir:{0}".format(len(lstFiles)))
    print("Quantidade máxima a ser processada:{0}".format(maxtransfer))
    index = 0;

    for file in lstFiles:
        try:
            print("Transferindo arquivo:{0} ....".format(file), end='')
            #            shutil.move(path_local+file,path_local_processed+file)
            client.retrbinary("RETR " + file, open(path_local + file, 'wb').write)
            print("OK", end='')
            print(" Excluindo arquivo:{0} ....".format(file), end='')
            client.delete(file)
            print("OK")

            index += 1
            if (index >= maxtransfer):
                break

        except:
            print("Erro:{0}".format(sys.exc_info()[0]))

    client.quit()


def persiste(lstData,file):
    total = len(lstData)
    index = 0
    Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)

    ImportToSqlServer.InitConnection()

    for data in lstData:
        data['crawler'] = datetime.datetime.now()

        if 'created_at' in data:
            data['criacao'] = Helpers.Utils.convertTwitterUTCTimeToLocal(data['created_at'])

        entidades = []
        if 'text' in data:
            text = data['text']
            text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)", "", text).split()).replace('RT :', '').strip()
            doc = nlp(text)

            for e in doc.ents:
                if 'LOC' == e.label_ or e.label_ == 'PER' or e.label_ == 'ORG':
                    entidades.append(e.string.strip())

            data['entidades'] = entidades
            data['fl_sql_migrated'] = False

            ImportToSqlServer.importToSql(data)

            index += 1
            Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)
            
            if (index == 0 or index % 1000 == 0):
                ImportToSqlServer.Commit()
            
                
    ImportToSqlServer.Commit()        

def processFiles():
    if not (os.path.exists(path_local_processed)):
        os.mkdir(path_local_processed)

    lstdir = os.listdir(path_local)
    total = len(lstdir)
    index = 0
    Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)

    for file in lstdir:
        with open(path_local + file, 'rb') as f:
            lstData = pickle.load(f)
            
            if (len(lstData) == 0):
                continue
            
            #Determina o tamanho da amostragem para grau de confiança de 99%
            #será aplicado apenas para quantidades de posts superiores a 1000
            Zsigma = 2.575 #99% de confiança
            E = 0.03 #Margem de erro ou ERRO MÁXIMO DE ESTIMATIVA. Identifica a diferença máxima entre a PROPORÇÃO AMOSTRAL e a verdadeira PROPORÇÃO POPULACIONAL (p)
            pe = 0.5 #prevalência esperada
            
            n = (Zsigma**2)*pe*(1-pe)/(E**2)
            N = len(lstData)
            #ajuste
            sample = round(n*N/(N+n))
            
            if (len(lstData) > 1000):
                persiste(random.sample(lstData,sample),file)
            else:
                persiste(lstData,file)

        os.remove(path_local + file)
        #     shutil.move(path_local+file,path_local_processed+file)
        Helpers.Utils.printProgressBar(index + 1, total, prefix='Progress:', suffix='Complete', showAndamento=True)
        index += 1


def PurgerImported():
    DataAccess.Twitters.deleteImportedToSqlServer()


max_transfer = 1

try:
    max_transfer = int(sys.argv[1])
except:
    max_transfer = 100

#print("Iniciando processo de obtenção de arquivos...")
#transferFiles(max_transfer)
#print("Finalizado o processo de obtenção de arquivos...")

print()

print("Processando os arquivos")
processFiles()
print("Finalizado o processo dos arquivos...")

print()

print("Atualiza sentimento")
Sentimento.AtualizarSentimento()
print("\nFinalizado o processo...")

# print("Excluindo registro do MongoDB já importados para o Sql Server")
# PurgerImported()
# print("Finalizado o processo de exclusão de registros.")

print("Veja log de importação")
