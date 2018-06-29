import spacy
import Helpers.Utils
import math
import re
import SqlServer.dbHelper as db

def AtualizarSentimento():
    __cursor,__cnxn = db.getConnection()
    
    __cursor.execute('select id,text from twitter where sentimento is null')
    data = __cursor.fetchall()
    total = len(data)
    
    if total <= 0:
        return

    processado = 0
#    line_count = 1
    nlp = spacy.load('C:\Cloud\Google Drive\Documents\python\TwitterAnalyzer\Model')

    Helpers.Utils.printProgressBar(processado,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

#    with open('processa_sentimento.csv', 'w',newline='', encoding='utf-8') as csvfile:
#    fieldnames = ['id', 'text','POSITIVE','NEGATIVE','sentimento','dev','Erro']
#    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#    writer.writeheader()
    
    for post in data:
#       try:
         id = post.id
         text = post.text.replace('\n', ' ').replace('\r', '')
         text = re.sub(r'https?:\/\/.*[\r\n]*', '', text)
         text = re.sub(r'#.*[\r\n]*', '', text)
         text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)"," ",text).split())
        
         sentimento = 'NEUTRO'
         dev = 0                       
         pos = 0
         neg = 0
         
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
         
         __cursor.execute('update twitter set negativo = ?,positivo = ?, sentimento = ? where id = ?',neg,pos,sentimento,id)
         
         Helpers.Utils.printProgressBar(processado+1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
         processado += 1
         
         if (processado == 0 or processado % 1000):
             __cursor.commit()
                
#       except:
#          writer.writerow({'id':id, 'text': text,'POSITIVE':doc.cats['POSITIVE'],'NEGATIVE':doc.cats['NEGATIVE'],'sentimento':sentimento,'dev':dev,'Erro':sys.exc_info()[0]})   
#          csvfile.flush()
         
       