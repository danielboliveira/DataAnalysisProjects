import spacy
import DataAccess.Twitters
import Helpers.Utils
import csv
import math
import sys
import re
data = DataAccess.Twitters.getTwittersSemSentimento()

total = data.count()
processado = 0
line_count = 1
nlp = spacy.load('C:/Cloud/Google Drive/Documents/python/TwitterAnalyzer/Model/')

Helpers.Utils.printProgressBar(processado,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

with open('processa_sentimento.csv', 'w',newline='', encoding='utf-8') as csvfile:
    fieldnames = ['id', 'text','POSITIVE','NEGATIVE','sentimento','dev','Erro']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for post in data:
       try:
         id = post['id']
         #text = post['text'].replace('\n', ' ').replace('\r', '')
         #text = re.sub(r'https?:\/\/.*[\r\n]*', '', text)
         #text = re.sub(r'#.*[\r\n]*', '', text)
         text = Helpers.Utils.get_text_sanitized(post)
         text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)"," ",text).split())
         
         sentimento = 'NEUTRO'
         dev = 0                       
         
         ### Sentimento ####
         if (len(text) > 0):                       
             doc = nlp(text)
                
       
             if(len(doc.cats) == 2):
                 pos = doc.cats['POSITIVE']
                 neg = doc.cats['NEGATIVE']
                        
                 dp = pos
                 dn = neg
                 dev = math.sqrt(dp*dp+dn*dn)
                        
                 if (dev > 0.3):
                    if (pos > neg):
                         sentimento = 'POSITIVO'
                    else:
                         sentimento = 'NEGATIVO'
         ### Sentimento ####
         
         DataAccess.Twitters.updatePostSentimento(post,pos,neg,sentimento)
         
         if (not ('criacao' in post)):
             DataAccess.Twitters.updatePostDataCriacao(post,Helpers.Utils.convertTwitterUTCTimeToLocal(post['created_at']))
         
         #writer.writerow({'id':id, 'text': text,'POSITIVE':doc.cats['POSITIVE'],'NEGATIVE':doc.cats['NEGATIVE'],'sentimento':sentimento,'dev':dev,'Erro':''})    
         
         Helpers.Utils.printProgressBar(processado+1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
         processado += 1
         
                
       except:
          writer.writerow({'id':id, 'text': text,'POSITIVE':doc.cats['POSITIVE'],'NEGATIVE':doc.cats['NEGATIVE'],'sentimento':sentimento,'dev':dev,'Erro':sys.exc_info()[0]})   
          csvfile.flush()
          
        