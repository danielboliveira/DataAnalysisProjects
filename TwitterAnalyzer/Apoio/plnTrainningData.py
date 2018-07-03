import SqlServer.dbHelper as db
import csv
import re
import Helpers.Utils

cursor,_ = db.getConnection()

cursor.execute('select text from trainning')
consulta = cursor.fetchall()
   
try:
    EMOJI_REGEX = re.compile(u'([\U00002600-\U000027BF])|([\U0001f300-\U0001f64F])|([\U0001f680-\U0001f6FF])')
except re.error: # pragma: no cover
    EMOJI_REGEX = re.compile(u'([\u2600-\u27BF])|([\uD83C][\uDF00-\uDFFF])|([\uD83D][\uDC00-\uDE4F])|([\uD83D][\uDE80-\uDEFF])')
    
index = 0 
total = len(consulta)
Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
with open('trainningRawData.csv', 'w',newline='', encoding='utf-8') as csvfile:
    fieldnames = ['text','POSITIVE','NEGATIVE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for r in consulta:
     text = r.text.lower()
     text = EMOJI_REGEX.sub(r'', text)
     text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)", "", text).split()).replace('…','').replace('RT :', '').replace('RT _:', '').strip()
     text = text.replace('.','')
     text = text.replace('kk','').replace('"','')
     
     if not (len(text) <= 3):
         pos = neg = 0
         writer.writerow({'text':text,'POSITIVE':pos,'NEGATIVE':neg})

     index += 1 
     Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
     
     
         
