# -*- coding: utf-8 -*-
from ftplib import FTP as ftp
import os, datetime
import pickle
import sys
import DataAccess.Twitters
import Helpers.Utils
from SqlServer import ImportToSqlServer
from SqlServer import Sentimento
from SqlServer import Analysis
import random
#from nltk.tokenize import word_tokenize

ftp_host = ''
ftp_user = ''
ftp_passwd = ''
path_local = './crawler_results/'
path_local_processed = './crawler_results_processed/'

#nlp = spacy.load('pt_core_news_sm')
#Tagger treinado


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


def persiste(lstData,file,consulta_id):
    total = len(lstData)
    index = 0
    Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)

    ImportToSqlServer.getConnection()

    for data in lstData:
        data['crawler'] = datetime.datetime.now()

        if 'created_at' in data:
            data['criacao'] = Helpers.Utils.convertTwitterUTCTimeToLocal(data['created_at'])

        entidades = []
#        if 'text' in data:
#            text = data['text']
#            text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)", "", text).split()).replace('RT :', '').strip()
#            tokens = word_tokenize(text)
#            tags = tagger.tag(tokens)        
#            
#            for e in tags:
#                if 'PRON' != e[1] and 'ADP' != e[1] and 'DET' != e[1]:
#                    entidades.append(e[0])

        data['entidades'] = entidades
        data['fl_sql_migrated'] = False

        ImportToSqlServer.importToSql(data,file,consulta_id)

        index += 1
        Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)
            
        if (index == 0 or index % 1000 == 0):
              ImportToSqlServer.Commit()
            
                
    ImportToSqlServer.Commit()        

def processFiles(consulta_id):
    if not (os.path.exists(path_local_processed)):
        os.mkdir(path_local_processed)

    lstdir = os.listdir(path_local)
    total = len(lstdir)
    index = 0
    
    if (total == 0):
        return
    Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)

    for file in lstdir:
            try:
                with open(path_local + file, 'rb') as f:
                    lstData = pickle.load(f)
            except:
                continue
        
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
                persiste(random.sample(lstData,sample),file,consulta_id)
            else:
                persiste(lstData,file,consulta_id)

            os.remove(path_local + file)
        #     shutil.move(path_local+file,path_local_processed+file)
            Helpers.Utils.printProgressBar(index + 1, total, prefix='Progress:', suffix='Complete', showAndamento=True)
            index += 1


def PurgerImported():
    DataAccess.Twitters.deleteImportedToSqlServer()

try:
    consulta_id = int(sys.argv[1])
except:
    aux = input("Informe o código da consulta: ")
    consulta_id = int(aux)
    
print("Realizando processamento para a consulta:{0}".format(consulta_id))    
termo = Analysis.getTermo(consulta_id)

if (termo == None):
    sys.exit("Termo de pesquisa inválido")

print("Termo:{0}".format(termo))    

#print("Iniciando processo de obtenção de arquivos...")
#transferFiles(max_transfer)
#print("Finalizado o processo de obtenção de arquivos...")

print()

print("Processando os arquivos")
processFiles(consulta_id)
print("Finalizado o processo dos arquivos...")

print()

print("Atualiza sentimento")
Sentimento.AtualizarSentimento()
print("\nFinalizado o processo...")

print("Processar termos e palavras")

print("Finalizado o processo de termos e palavras.")
Analysis.processWords(consulta_id)
print("Veja log de importação")
