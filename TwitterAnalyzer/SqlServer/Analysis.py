# -*- coding: utf-8 -*-
import SqlServer.dbHelper as db
import re
import pickle
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from string import punctuation
import os
import Helpers.Utils

__PATH__ = os.path.dirname(__file__)

def getTermo(consulta_id):
    cursor = db.getCursor()
    cursor.execute('select ds_termo from twitter_consulta where cd_consulta = ?',consulta_id)
    row = cursor.fetchone()
    if row:
        return row.ds_termo.lower()
    else:
        return None

def processWords(consulta_id):
   
    termo = getTermo(consulta_id)
    if not termo:
        return;
    cursor = db.getCursor()
    #Limpa a tabela de termos
    cursor.execute('delete from a from words a join twitter b on a.twitter_id = b.id where b.consulta_id = ? and fl_words_processed = 0',consulta_id)
    cursor.execute('select id,text from twitter where consulta_id = ? and fl_words_processed = 0',consulta_id)
    consulta = cursor.fetchall()
    total = len(consulta)
    index = 0
    Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)        
    
    try:
        EMOJI_REGEX = re.compile(u'([\U00002600-\U000027BF])|([\U0001f300-\U0001f64F])|([\U0001f680-\U0001f6FF])')
    except re.error: # pragma: no cover
        EMOJI_REGEX = re.compile(u'([\u2600-\u27BF])|([\uD83C][\uDF00-\uDFFF])|([\uD83D][\uDC00-\uDE4F])|([\uD83D][\uDE80-\uDEFF])')
    
    with open(__PATH__ +"\\tagger.pkl", "rb") as f:
        tagger = pickle.load(f)

    for row in consulta:
        if not row.text:
            continue
        
        text = row.text.lower()
        text = EMOJI_REGEX.sub(r'', text)
        text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)", "", text).split()).replace('…','').replace('RT :', '').replace('RT _:', '').strip()
        tokens = word_tokenize(text)
        sw = set(stopwords.words('english') + stopwords.words('spanish') + stopwords.words('portuguese') + list(punctuation) + [str(x) for x in range(0,9)])
        sw.add('rt')
        sw.add('pro')
        sw.add('``')
        sw.add('…')
        sw.add('é')
        sw.add('parte')
        
        #sw = sw + list([numero for numero in range(0,9)])
    
        no_stop_words = [token for token in tokens if token not in sw and len(token.strip()) > 0]
        tags = tagger.tag(no_stop_words) 
        
        #words = []

        for tag in tags:
            if (tag[1] == 'NOUN' or tag[1] == 'ADJ' or tag[1] == 'VERB') and (tag[0] != termo and len(tag[0]) > 2 and len(tag[0]) < 100):
                cursor.execute('insert into words(twitter_id,text) values (?,?)',row.id,tag[0])
                #words.append(tag[0])
        
        cursor.commit()
        index += 1
        Helpers.Utils.printProgressBar(index, total, prefix='Progress:', suffix='Complete', showAndamento=True)        


#Sumariza as informação de Estatisticas de palavras
def processWordsStats(consulta_id):
    cursor,_ = db.getConnection()
    cursor.execute('exec [prcStatsWordCount] ?', consulta_id)
    
def getWords(consulta_id,sentimento=None):
  
  cursor,_ = db.getConnection()
  
  if (not sentimento):
      cursor.execute('select a.qt_word,a.ds_word from stats_word_count a ' +
                     'where a.cd_consulta = ? ',
                     consulta_id)
  else:    
      cursor.execute('select a.qt_word,a.ds_word from stats_word_count a ' +
                     'where a.cd_consulta = ? '+ 
                     'and a.ds_sentimento = ?',
                     consulta_id,sentimento)
      
      
  result = cursor.fetchall()
  
  text = []
  
  for row in result:
      text = text + [row.ds_word for i in range(1,row.qt_word)]
  
  return ' '.join(text)
  