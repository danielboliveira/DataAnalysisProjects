# -*- coding: utf-8 -*-
from ftplib import FTP as ftp
import os,datetime
import pickle
import sys,shutil
import DataAccess.Twitters
import Helpers.Utils
import spacy
import re

ftp_host='191.237.254.23'
ftp_user='admin'
ftp_passwd='elrond'
path_local = './crawler_results_not_process/'
path_local_processed = './crawler_results_processed/'
nlp = spacy.load('pt_core_news_sm')

def transferFiles():
    
    if not (os.path.exists(path_local)):
       os.mkdir(path_local)
            
    client = ftp(host=ftp_host)
    client.login(user=ftp_user,passwd=ftp_passwd)

    lstFiles = client.nlst()
    print("Total de arquivos para transferir:{0}".format(len(lstFiles)))
    
    for file in lstFiles:
        try:
            print ("Transferindo arquivo:{0} ....".format(file),end='')
            client.retrbinary("RETR " + file, open(path_local + file, 'wb').write)
            print ("OK",end='')
            print (" Excluindo arquivo:{0} ....".format(file),end='')
            client.delete(file)
            print ("OK")
        except:
            print ("Erro:{0}".format(sys.exc_info()[0]))
            
    
    client.quit()

def PersisteInMongoDB(lstData):
 for data in lstData:
    data['crawler'] = datetime.datetime.now()

    if ('created_at' in data):
      data['criacao'] = Helpers.Utils.convertTwitterUTCTimeToLocal(data['created_at'])
      
    entidades = []    
    if ('text' in data):
      text = data['text']
      text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)","",text).split()).replace('RT :','').strip()
      doc  = nlp(text)
          
      for e in doc.ents:
        if (e.label_ == 'LOC' or e.label_ == 'PER' or e.label_ == 'ORG'):
           entidades.append(e.string.strip())
           
      data['entidades'] = entidades  
      DataAccess.Twitters.insertPost(data)

def processFiles():
    
    if not (os.path.exists(path_local_processed)):
       os.mkdir(path_local_processed)
       
    lstdir = os.listdir(path_local)
    total = len(lstdir)
    index   = 0
    Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
    for file in lstdir:
     with open(path_local+file, 'rb') as f:
         lstData = pickle.load(f)
         PersisteInMongoDB(lstData)

     shutil.move(path_local+file,path_local_processed+file)
     Helpers.Utils.printProgressBar(index+1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
     index+=1

print("Iniciando processo de obtenção de arquivos...")
transferFiles()
print("Finalizado o processo de obtenção de arquivos...")
print()
print("Processando os arquivos")
processFiles()
print("Finalizado o processo dos arquivos...")