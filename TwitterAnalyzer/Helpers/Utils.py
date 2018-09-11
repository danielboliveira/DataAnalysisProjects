#-*- coding: utf-8 -*-
from datetime import datetime
from datetime import timedelta
import string
import sys
import os
from nltk.stem.lancaster import LancasterStemmer
from nltk.corpus import stopwords
import yaml
from SqlServer import Analysis as an

__Analises_Root_Path__ = 'c:\\Analise\\'

def getAnalisesPath(consulta_id,horario=False):
    '''
    Função para obter o caminho para serem gerados os arquivos de resultados de análises
    consulta_id = Código da consulta a ter os dados análisados
    horario = Indica se no nome do sub-folder terá hora e minutos (para análises mais recorrentes)
    '''
    global __Analises_Root_Path__     
    makeDir(__Analises_Root_Path__)

    termo = an.getTermo(consulta_id)
        
    if (not termo):
       path = __Analises_Root_Path__ +  str(consulta_id)
    else:
       path = __Analises_Root_Path__ +  termo.lower()
       path = path.replace('ê','e')
       
    makeDir(path)
    
    if (horario):
        mask = '%Y_%m_%d_%H_%M'
    else:
        mask = '%Y_%m_%d'
        
    path = path + '\\' + datetime.now().strftime(mask)
    makeDir(path)
    
    return path

def removeFile(file):
    '''
    Função para remover um arquivo.
    file = Full Path do arquivo a ser removido
    '''
    if os.path.isfile(file):
        os.remove(file)

def makeDir(path):
    if (not os.path.exists(path)):
        os.mkdir(path)

def getConfig(configFile):
     with open(configFile, 'r') as ymlfile:
         cfg = yaml.load(ymlfile)

     d = {}
    
     d['TOTAL'] = cfg['crawler']['TOTAL']
     d['PATH_RESULTS'] = cfg['crawler']['PATH_RESULTS']
     d['CONSUMER_KEY'] = cfg['crawler']['CONSUMER_KEY']
     d['CONSUMER_SECRET'] = cfg['crawler']['CONSUMER_SECRET']
     d['ACCESS_TOKEN'] = cfg['crawler']['ACCESS_TOKEN']
     d['ACCESS_TOKEN_SECRET'] = cfg['crawler']['ACCESS_TOKEN_SECRET']
     d['QUERY'] = cfg['crawler']['QUERY']
     d['TWITTER_LANG'] = cfg['crawler']['TWITTER_LANG']
     d['INICIO'] = cfg['crawler']['INICIO']
     d['FIM'] = cfg['crawler']['FIM']
     
     return d
     
def convertTwitterUTCTimeToLocal(valor):
     
 clean_timestamp = datetime.strptime(valor,'%a %b %d %H:%M:%S +0000 %Y')
 offset_hours = -3 #offset in hours for EST timezone

     #account for offset from UTC using timedelta                                
 local_timestamp = clean_timestamp + timedelta(hours=offset_hours)

  #convert to am/pm format for easy reading
 #final_timestamp =  datetime.strftime(local_timestamp,'%Y-%m-%d %I:%M:%S %p')  
     
 return local_timestamp

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

    sys.stdout.write('\r%s |%s| %s%s %s %s' % (prefix, bar, percents, '%', suffix,andamento)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()

#Gets the tweet time.
def get_time(tweet):
    return datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")

#Gets all hashtags.
def get_hashtags(tweet):
    return [tag['text'] for tag in tweet['entities']['hashtags']]

#Gets the screen names of any user mentions.
def get_user_mentions(tweet):
    return [m['screen_name'] for m in tweet['entities']['user_mentions']]


def getTwitterFullText(twitter):
    texto = ""
    
    try:
        texto = twitter['text']
        if (twitter['retweeted_status']['extended_tweet']):
            texto = twitter['retweeted_status']['extended_tweet']['full_text']
    except:
        texto = twitter['text']
    
    
    return texto

#Gets the text, sans links, hashtags, mentions, media, and symbols.
def get_text_cleaned(tweet):
    #text = tweet['text']
    text = getTwitterFullText(tweet)
    
    slices = []
    #Strip out the urls.
    if 'urls' in tweet['entities']:
        for url in tweet['entities']['urls']:
            slices += [{'start': url['indices'][0], 'stop': url['indices'][1]}]
    
    #Strip out the hashtags.
    if 'hashtags' in tweet['entities']:
        for tag in tweet['entities']['hashtags']:
            slices += [{'start': tag['indices'][0], 'stop': tag['indices'][1]}]
    
    #Strip out the user mentions.
    if 'user_mentions' in tweet['entities']:
        for men in tweet['entities']['user_mentions']:
            slices += [{'start': men['indices'][0], 'stop': men['indices'][1]}]
    
    #Strip out the media.
    if 'media' in tweet['entities']:
        for med in tweet['entities']['media']:
            slices += [{'start': med['indices'][0], 'stop': med['indices'][1]}]
    
    #Strip out the symbols.
    if 'symbols' in tweet['entities']:
        for sym in tweet['entities']['symbols']:
            slices += [{'start': sym['indices'][0], 'stop': sym['indices'][1]}]
    
    # Sort the slices from highest start to lowest.
    slices = sorted(slices, key=lambda x: -x['start'])
    
    #No offsets, since we're sorted from highest to lowest.
    for s in slices:
        text = text[:s['start']] + text[s['stop']:]
        
    return text

#Sanitizes the text by removing front and end punctuation, 
#making words lower case, and removing any empty strings.
def get_text_sanitized(tweet,case=False):    
    if not(case):
        return ' '.join([w.lower().strip().rstrip(string.punctuation)\
            .lstrip(string.punctuation).strip()\
            for w in get_text_cleaned(tweet).split()\
            if w.strip().rstrip(string.punctuation).strip()])
    else:
        return ' '.join([w.strip().rstrip(string.punctuation)\
            .lstrip(string.punctuation).strip()\
            for w in get_text_cleaned(tweet).split()\
            if w.strip().rstrip(string.punctuation).strip()])

#Gets the text, clean it, make it lower case, stem the words, and split
#into a vector. Also, remove stop words.
def get_text_normalized(tweet):
    #Sanitize the text first.
    text = get_text_sanitized(tweet).split()
    
    #Remove the stop words.
    text = [t for t in text if t not in stopwords.words('english')]
    
    #Create the stemmer.
    stemmer = LancasterStemmer()
    
    #Stem the words.
    return [stemmer.stem(t) for t in text]