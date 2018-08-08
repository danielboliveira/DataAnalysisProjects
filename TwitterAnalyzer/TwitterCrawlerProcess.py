# -*- coding: utf-8 -*-
from ftplib import FTP as ftp
import os, datetime,shutil
import pickle
import sys
import DataAccess.Twitters
import Helpers.Utils
from SqlServer import ImportToSqlServer
from SqlServer import Sentimento
from SqlServer import Analysis
from SqlServer import dbHelper
import random
from Automacao import Stats as st
import warnings
#from nltk.tokenize import word_tokenize

 # Filter annoying Cython warnings that serve no good purpose.
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

ftp_host = '191.232.48.84'
ftp_user = 'daniel.oliveira'
ftp_passwd = 'Dpkl29154220*'
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
    client.set_pasv(True)
    
    ls = []
    client.retrlines('MLSD', ls.append)
    print(ls)
    
    #
#    lstFiles = client.nlst()
    #    lstFiles = os.listdir(path_local)

#    print("Total de arquivos para transferir:{0}".format(len(lstFiles)))
#    print("Quantidade máxima a ser processada:{0}".format(maxtransfer))
#    index = 0;
#
#    for file in lstFiles:
#        try:
#            print("Transferindo arquivo:{0} ....".format(file), end='')
#            #            shutil.move(path_local+file,path_local_processed+file)
#            client.retrbinary("RETR " + file, open(path_local + file, 'wb').write)
#            print("OK", end='')
#            print(" Excluindo arquivo:{0} ....".format(file), end='')
#            client.delete(file)
#            print("OK")
#
#            index += 1
#            if (index >= maxtransfer):
#                break
#
#        except:
#            print("Erro:{0}".format(sys.exc_info()[0]))

    client.quit()


def persiste(lstData,file,consulta_id):
    total = len(lstData)
    index = 0
    Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)

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
              dbHelper.Commit()
            
                
    dbHelper.Commit()        

def processFiles(consulta_id,path):
    if not (os.path.exists(path_local_processed)):
        os.mkdir(path_local_processed)
        
    lstdir = os.listdir(path)
    total = len(lstdir)
    index = 0
    
    if (total == 0):
        return
    
    Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)

    for file in lstdir:
            try:
                with open('{0}//{1}'.format(path,file), 'rb') as f:
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

            os.remove('{0}//{1}'.format(path,file))
        #     shutil.move(path_local+file,path_local_processed+file)
            Helpers.Utils.printProgressBar(index + 1, total, prefix='Progress:', suffix='Complete', showAndamento=True)
            index += 1
      
    shutil.rmtree(path, ignore_errors=True)

def PurgerImported():
    DataAccess.Twitters.deleteImportedToSqlServer()

#print("Iniciando processo de obtenção de arquivos...")
#transferFiles()
#print("Finalizado o processo de obtenção de arquivos...")

    

#
#
#print()
print("Processando os arquivos")

dirs = [dir for dir in os.scandir(path_local) if dir.is_dir()]
ids  = []

for edir in dirs:
    try:
        print()
        print('\tProcessando diretório:{0}'.format(edir.path))
        consulta_id = int(edir.name)
        print('\tID Consulta:{0}'.format(consulta_id))
        termo = Analysis.getTermo(consulta_id)

        if (termo == None):
            raise ValueError('Termo de consulta inválido')
        print('\tTermo:{0}'.format(termo))
        print()
        ids.append(consulta_id)
        processFiles(consulta_id,edir.path)  
        print()
    except:
        print("Erro:{0}".format(sys.exc_info()[0]))
        continue
print("Finalizado o processo dos arquivos...")

print()

print("Atualiza sentimento")
Sentimento.AtualizarSentimento()
print("\nFinalizado o processo...")
#
print("Processar termos / palavras / Estatísticas")

for id in ids:
    print('\tConsulta ID:{0}'.format(id))
    Analysis.processWords(id)

print("Processar estatísticas de termos")
for id in ids:
    print('\tConsulta ID:{0}'.format(id))
    Analysis.processWordsStats(id)


print("Processar estatísitcas de sentimentos")
for id in ids:
    print('\tConsulta ID:{0}'.format(id))
    Analysis.processStatsSentimento(id)

#
print("Processar estatísitcas de sentimentos (apenas influência)")
for id in ids:
    print('\tConsulta ID:{0}'.format(id))
    Analysis.processStatsSentimentoInfluencia(id)
#
print("Processar estatísitcas de RTs")
for id in ids:
    print('\tConsulta ID:{0}'.format(id))
    Analysis.processStatsRts(id)
#
#
print('Gerando dados de saída') 
#for id in ids:
for id in [10,11]:
    print('\tConsulta ID:{0}'.format(id))
    st.generateAllOutPuts(id)

st.generateIndexHtml([10,11])
print("Veja log de importação")
