# -*- coding: utf-8 -*-
"""
Created on Tue May 22 10:10:11 2018

@author: daniel
"""
import nltk
import itertools

def getTwitterFullText(twitter):
    texto = ""
    
    try:
        texto = twitter['text']
        if (twitter['retweeted_status']['extended_tweet']):
            texto = twitter['retweeted_status']['extended_tweet']['full_text']
    except:
        texto = twitter['text']
    
    
    return texto

def ProcessText(text,tagger,portuguese_sent_tokenizer):
    ret = []
    
    excluir = [ 'AGORA','HIJA','HACE','CORAZÓN','OPERARON','UNA','MES','MESES','ESCUELA','BOGOTÁ','DESCUBRE','GOBIERNO,','MIENTRAS','ELN','REGRESA','CON','GOBIERNO','FISCALIA','LES','HTTP…','HTTP','TUDO','TODOS','TODAS','REALMENTE','ANTE', 'APÓS', 'ATÉ','COM','CONTRA', 'DE', 'DESDE','EM', 'ENTRE','PARA','PERANTE','POR','SEM','SOB','SOBRE','TRÁS','E','NEM','MAS', 'TAMBÉM','COMO','ALÉM','DE','DISSO','DISTO','AQUILO', 'QUANTO','AS','PORÉM', 'TODAVIA',
                'ENTRETANTO','NO','ENTANTO','SENÃO','NÃO','OBSTANTE','CONTUDO','OU','ORA','JÁ','QUE','QUER','SEJA','PORQUE','PORQUANTO','POIS','LOGO','PORTANTO','ENTÃO','POR', 'ISSO','CONSEGUINTE','ISTO','ASSIM','QUE','SE','PORQUÊ','POR QUÊ','POR QUE', 
                'POIS','PORQUANTO','COMO','UMA','VEZ','VISTO','COMO','ENTRE','OUTROS','MAIS','MENOS','MAIOR','MENOR','TAL','QUAL','TANTO','QUANTO','COMO','BEM','NEM','EMBORA','CONQUANTO','AINDA','MESMO','POSTO','APESAR','CASO','QUANDO','CONTANTO',
                'GET','SER','ETÁ','LOS','DEL','ADO…','NIÑOS','ESTA','VAI','ESTÁ','ESTE','ISTO','ISSO','AQUILO','NESSE','NESSA','NESTE','NESTA',
				'PRO','HACE','HIJA','CORAZÓN','CARDIOPATÍA','CONGÉNITA','LIZARALDE','MESES','CUATRO','TCOCEA…','ELE','FOI','ELA','DIZ',
				'DOS','VIA','QUEM','NUNCA','TEM','CORTE','AND','THE','TER','MUITO','PELO','SEU','ESTOU','PELA','SÃO',
				'QUEIMOU','INQUISIÇÃO','CIGARRO','DIZEM','TCOCR6GQNYHI2','IRMÃOZINHO','FEZ','BOMBINHA','JOGOU',
				'NACIÓ'
				]
    
    sentences = portuguese_sent_tokenizer.tokenize(text)
    tags = [tagger.tag(nltk.word_tokenize(sentence)) for sentence in sentences]

    interested_words = set()
    
    for item in tags:
        for w,t in item:
            if (t == 'NOUN') or (t=='ADJ'):
                interested_words.add(w)
    
    
    for word in interested_words:
        word = word.replace('.','').replace('/','').replace('?','')
        
        if (len(word)>=3)and not(word in excluir) and not('#' in word) and not('@' in word) and not('\n' in word) and not('HTTPS' in word) and not ('HTTP' in word):
            ret.append(word)
    
    w_list = []
    for subset in itertools.combinations(ret, 2):
        w_list.append(tuple(subset))
    
    return w_list