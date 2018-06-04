import spacy
import csv
import datetime
import Helpers.Utils

def readcsv(filename):	
    ifile = open(filename, "r")
    reader = csv.reader(ifile, delimiter=";")

    rownum = 0	
    a = []

    for row in reader:
        a.append (row)
        rownum += 1
    
    ifile.close()
    return a

def textExpressions(texto,analyzer):
    print(texto)
    doc = analyzer(texto)
    print(doc.cats)


##############################################
nlp = spacy.load('pt_core_news_sm')
#nlp.from_disk('C:/Cloud/Google Drive/Documents/python/TwitterAnalyzer/Model/')
#textExpressions('Mas, é um merda',nlp) 


print('Inicio:',datetime.datetime.now().time())
#
train_data = []
#
my_data = readcsv('trainningData.csv')
total = 0
#
for row in my_data:
  sentiment= {}
  sentiment['POSITIVE'] = int(row[1])
  sentiment['NEGATIVE'] = int(row[2])
  cats = {'cats':sentiment}  
  train_data.append((row[0],cats))
  total += 1

textcat = nlp.create_pipe('textcat')
nlp.add_pipe(textcat, last=True)
textcat.add_label('POSITIVE')
textcat.add_label('NEGATIVE')

optimizer = nlp.begin_training()

for itn in range(10):
    print()
    print("iteração:",itn)
    sub_int = 0
    Helpers.Utils.printProgressBar(0,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
    for doc, gold in train_data:
        nlp.update([doc], [gold], sgd=optimizer)
        Helpers.Utils.printProgressBar(sub_int+1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
        sub_int +=1
        

nlp.to_disk('C:/Cloud/Google Drive/Documents/python/TwitterAnalyzer/Model/')

print()
print('Fim:',datetime.datetime.now().time())