# -*- coding: utf-8 -*-
"""
Created on Thu May 17 10:47:00 2018

@author: daniel
"""
from pickle import load
import nltk
import pprint
import sys
import Helpers.TextProcessor
import Helpers.Utils
import DataAccess.Twitters


input_tagger = open('tagger.pkl', 'rb')
tagger = load(input_tagger)
input_tagger.close()
portuguese_sent_tokenizer = nltk.data.load("tokenizers/punkt/portuguese.pickle")


tws = DataAccess.Twitters.getTwitterForWordProcess()


total = tws.count()
index = 0

print("Total de documentos:",total)
print()

Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento = True)

for post in tws:
   
   try:
       text = Helpers.TextProcessor.getTwitterFullText(post)
       word_list = Helpers.TextProcessor.ProcessText(text.upper(),tagger,portuguese_sent_tokenizer)
       
#       if (index % 100 == 0) or (index == 1):
#               print("Post processados:#",index)
        
           
       for words in word_list:
           p1 = words[0]
           p2 = words[1]
           
           if (p1 == p2):
               continue;

           w = DataAccess.Twitters.getWordSummary(p1,p2)
           if (w):
               w['total'] = w['total'] + 1
               DataAccess.Twitters.updateWordSummary(p1,p2,w)
           else:
               w = {"text1":p1,"text2":p2,"total":1}
               DataAccess.Twitters.insertWordSummary(w)
           

   except:
       print("Erro:", sys.exc_info()[0])
       pprint.pprint(post)
       print()
   
   DataAccess.Twitters.updatePostWordProcessed(post) 
   index += 1
   Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento = True)

#words = nltk.word_tokenize(text.upper(),language='portuguese')
#pprint.pprint(ProcessText(text,tagger,portuguese_sent_tokenizer))            


# =============================================================================
# nlu = NaturalLanguageUnderstandingV1(
#     username='b41fa953-358a-4b79-be2b-1ab8425a07d4',
#     password='eAIUynKuSiqa',
#     version='2018-03-16'
# )
# 
# try:
#     response = nlu.analyze(
#   text=text,
#   features=Features(
#     entities=EntitiesOptions(
#       emotion=True,
#       sentiment=True,
#       limit=2),
#     keywords=KeywordsOptions(
#       emotion=True,
#       sentiment=True,
#       limit=2)))
#     
#     print(json.dumps(response, indent=2))
# except WatsonApiException as ex:
#     print("Method failed with status code " + str(ex.code) + ": " + ex.message)
# 
# =============================================================================
