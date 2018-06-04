# -*- coding: utf-8 -*-
#import Helpers.Utils
#import spacy
#import re
from datetime import datetime
from datetime import timedelta
import string
from twython import TwythonStreamer
import os
import pickle
import sys

def convertTwitterUTCTimeToLocal(valor):
     
 clean_timestamp = datetime.strptime(valor,'%a %b %d %H:%M:%S +0000 %Y')
 offset_hours = -3 #offset in hours for EST timezone

     #account for offset from UTC using timedelta                                
 local_timestamp = clean_timestamp + timedelta(hours=offset_hours)

  #convert to am/pm format for easy reading
 #final_timestamp =  datetime.strftime(local_timestamp,'%Y-%m-%d %I:%M:%S %p')  
     
 return local_timestamp

def SafeFile(ts):
        __path = './crawler_results/'
        if not (os.path.exists(__path)):
            os.mkdir(__path)
        n = datetime.now()
        d = os.listdir(path=__path)
        
        filename = __path + '{0:05d}'.format(len(d)+1)+'_'+'{0:04d}'.format(n.year)+'_'+'{0:02d}'.format(n.month)+'_'+'{0:02d}'.format(n.day)+'_'
        filename += '{0:02d}'.format(n.hour)+'_'+'{0:02d}'.format(n.minute)+'_'+'{0:02d}'.format(n.second)+'.pkl'
        
        if os.path.exists(filename):
            os.remove(filename)
        
        output = open(filename, 'wb')
        pickle.dump(ts, output, -1)
        output.close()   

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals=1, bar_length=50,showAndamento = False):
    """
    Call in a loop to create terminal progress bar

    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
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
#    sys.stdout.flush()

class MyStreamer(TwythonStreamer):
    tweets = 0
    global_tweets = 0;
    ts = [];
#    nlp = spacy.load('pt_core_news_sm')
    
    def on_success(self,data):

        data['crawler'] = datetime.now()
        
        if ('created_at' in data):
            data['criacao'] = convertTwitterUTCTimeToLocal(data['created_at'])
            
#        entidades = []    
#        if ('text' in data):
#           text = data['text']
#           text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)","",text).split()).replace('RT :','').strip()
#           doc = self.nlp(text)
          
#           for e in doc.ents:
#               if (e.label_ == 'LOC' or e.label_ == 'PER' or e.label_ == 'ORG'):
#                   entidades.append(e.string.strip())
        
#           data['entidades'] = entidades
            
        self.ts.append(data)
        
        self.global_tweets += 1
        printProgressBar(self.tweets+1,1000,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
        self.tweets += 1
                        
        if self.tweets >= 1000 :
            print("Salvando arquivo...")
            SafeFile(self.ts)
            self.tweets = 0
            self.ts.clear()
            self.disconnect()
            
    def on_error(self,status_code,data):
        print(status_code,data)
        self.disconnect()
       


CONSUMER_KEY = "nDc5j9q7vhWi0krocmnM28vXE"
CONSUMER_SECRET = "5KscghsdSOX3BYsVjdME0hsRAdMWHiblrvVcc9cApCKfiO8IQI"
ACCESS_TOKEN = "52572176-zn9okwAeO0iAX1ZEW6E9YaE8iAn8IVdDYqyXN5lYb"
ACCESS_TOKEN_SECRET = "EmQuHiuTncRSGy7Wvh5Q0InseGWm45TrxGcH4Q6pLad87"

query = sys.argv[1]

stream = MyStreamer(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
stream.statuses.filter(track=query,lang="pt")

print(sys.argv[1])

print("Finalizado...")


