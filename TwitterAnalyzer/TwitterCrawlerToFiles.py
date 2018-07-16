# -*- coding: utf-8 -*-
#import Helpers.Utils
#import spacy
#import re
from datetime import datetime
from datetime import timedelta
import sys
from twython import TwythonStreamer
import os
import pickle
import yaml

config = ''
try:
    config = sys.argv[1]
except:
    config = 'crawler.config'
    
print("Arquivo configuracao:{0}".format(config))    

with open(config, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

total = cfg['crawler']['TOTAL']
path_results = cfg['crawler']['PATH_RESULTS']
CONSUMER_KEY = cfg['crawler']['CONSUMER_KEY']
CONSUMER_SECRET = cfg['crawler']['CONSUMER_SECRET']
ACCESS_TOKEN = cfg['crawler']['ACCESS_TOKEN']
ACCESS_TOKEN_SECRET = cfg['crawler']['ACCESS_TOKEN_SECRET']
query = cfg['crawler']['QUERY']
lang_query = cfg['crawler']['TWITTER_LANG']
inicio = cfg['crawler']['INICIO']
fim = cfg['crawler']['FIM']

#Obtem qual termo a consultar
queryList = query.split(';')

aux = queryList[0].split('-')
consulta = aux[1]
id = aux[0]
path_results = path_results + '//' + id + '//'

queryList.append(queryList.pop(0)) #Move o termo para o fim da lista

cfg['crawler']['QUERY'] = ";".join(queryList) #Atualiza o parâmetro do arquivo

bloqueio = False

try:
    dInicio = datetime.strptime(inicio, '%d/%m/%Y %H:%M')
    dFim = datetime.strptime(fim, '%d/%m/%Y %H:%M')
    now = datetime.now()
    
    if (now >= dFim):
        #ajusta as datas
        cfg['crawler']['INICIO'] = (dInicio + timedelta(days=1)).strftime("%d/%m/%Y %H:%M")
        cfg['crawler']['FIM']    = (dFim + timedelta(days=1)).strftime("%d/%m/%Y %H:%M")
        
except:
    pass

   

#Salva o arquivo config
with open(config, 'w') as yaml_file:
    yaml.dump(cfg, yaml_file, default_flow_style=False)


print("Total a capturar:{0}".format(total))
print("Path:{0}".format(path_results))
print("CONSUMER_KEY:{0}".format(CONSUMER_KEY))
print("CONSUMER_SECRET:{0}".format(CONSUMER_SECRET))
print("ACCESS_TOKEN:{0}".format(ACCESS_TOKEN))
print("ACCESS_TOKEN_SECRET:{0}".format(ACCESS_TOKEN_SECRET))
print("query:{0}".format(query))
print("lang:{0}".format(lang_query))
print("Inicio:{0}".format(inicio))
print("Fim:{0}".format(fim))



def convertTwitterUTCTimeToLocal(valor):
     
 clean_timestamp = datetime.strptime(valor,'%a %b %d %H:%M:%S +0000 %Y')
 offset_hours = -3 #offset in hours for EST timezone

     #account for offset from UTC using timedelta                                
 local_timestamp = clean_timestamp + timedelta(hours=offset_hours)

  #convert to am/pm format for easy reading
 #final_timestamp =  datetime.strftime(local_timestamp,'%Y-%m-%d %I:%M:%S %p')  
     
 return local_timestamp

def SafeFile(ts):
        if not (os.path.exists(path_results)):
            os.mkdir(path_results)
            
        n = datetime.now()
        d = os.listdir(path=path_results)
        
        filename = path_results + '{0:05d}'.format(len(d)+1)+'_'+'{0:04d}'.format(n.year)+'_'+'{0:02d}'.format(n.month)+'_'+'{0:02d}'.format(n.day)+'_'
        filename += '{0:02d}'.format(n.hour)+'_'+'{0:02d}'.format(n.minute)+'_'+'{0:02d}'.format(n.second)+'.pkl'
        
        if os.path.exists(filename):
            os.remove(filename)
        
        output = open(filename, 'wb')
        pickle.dump(ts, output, -1)
        output.close()   

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals=1, bar_length=50,showAndamento = False):
    
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    
    andamento = ''
    
    if (showAndamento):
        andamento = '(' + str(iteration) + ' de ' + str(total) + ')'

    print('\r%s |%s| %s%s %s %s' % (prefix, bar, percents, '%', suffix,andamento),end='\r')

    if iteration == total:
         print()

class MyStreamer(TwythonStreamer):
    tweets = 0
    global_tweets = 0;
    ts = [];
    
    def on_success(self,data):

        data['crawler'] = datetime.now()
        
        if ('created_at' in data):
            data['criacao'] = convertTwitterUTCTimeToLocal(data['created_at'])
            
            
        self.ts.append(data)
        
        self.global_tweets += 1
        printProgressBar(self.tweets+1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
        self.tweets += 1
                        
        if self.tweets >= total :
            print("Salvando arquivo...")
            SafeFile(self.ts)
            self.tweets = 0
            self.ts.clear()
            self.disconnect()
            
    def on_error(self,status_code,data):
        print(status_code,data)
        self.disconnect()
       

if not (bloqueio):
    consulta = consulta.strip()
    
    if len(consulta.split(' ')) >= 2:
        consulta = '"' + consulta + '"'
        
    print("Consulta:{0}".format(consulta))
    stream = MyStreamer(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
    stream.statuses.filter(track=consulta,lang=lang_query)


print("Finalizado...")


