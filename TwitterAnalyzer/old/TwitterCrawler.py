# -*- coding: utf-8 -*-
"""
Created on Fri May  4 09:52:14 2018

@author: daniel
"""
import DataAccess.Twitters
import Helpers.Utils
import spacy
import re
import datetime
from twython import TwythonStreamer

class MyStreamer(TwythonStreamer):
    tweets = 0
    global_tweets = 0;
    ts = [];
    nlp = spacy.load('pt_core_news_sm')
    
    def on_success(self,data):

        data['crawler'] = datetime.datetime.now()
        
        if ('created_at' in data):
            data['criacao'] = Helpers.Utils.convertTwitterUTCTimeToLocal(data['created_at'])
            
        entidades = []    
        if ('text' in data):
           text = data['text']
           text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)","",text).split()).replace('RT :','').strip()
           doc = self.nlp(text)
          
           for e in doc.ents:
               if (e.label_ == 'LOC' or e.label_ == 'PER' or e.label_ == 'ORG'):
                   entidades.append(e.string.strip())
           

           data['entidades'] = entidades
            
        self.ts.append(data)
        
        self.tweets += 1
        self.global_tweets += 1
        #print("Obtidos #",self.tweets )
        if (self.tweets+1 <= 1000):
            Helpers.Utils.printProgressBar(self.tweets+1,1000,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
                        
        if self.tweets >= 1000 :
            print("Salvando no banco")
            DataAccess.Twitters.insertMany(self.ts)
            self.tweets = 0
            self.ts.clear()
             
        if self.global_tweets  >= 50000:
           self.disconnect()
            
    def on_error(self,status_code,data):
        print(status_code,data)
        self.disconnect()

        


CONSUMER_KEY = "nDc5j9q7vhWi0krocmnM28vXE"
CONSUMER_SECRET = "5KscghsdSOX3BYsVjdME0hsRAdMWHiblrvVcc9cApCKfiO8IQI"
ACCESS_TOKEN = "52572176-zn9okwAeO0iAX1ZEW6E9YaE8iAn8IVdDYqyXN5lYb"
ACCESS_TOKEN_SECRET = "EmQuHiuTncRSGy7Wvh5Q0InseGWm45TrxGcH4Q6pLad87"

stream = MyStreamer(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
stream.statuses.filter(track='eleições,lula,dilma,bolsonaro,jair bolsonaro,ciro gomes,boulos,manuela d\'avila,manuela davila,stf,sérgio moro,alckmin,henrique meirelles,petrolão,mensalão',lang="pt")
print("Finalizado...")

