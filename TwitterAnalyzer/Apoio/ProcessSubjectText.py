## -*- coding: utf-8 -*-
import spacy
import re
import DataAccess.Twitters
from bson.objectid import ObjectId
import Helpers.Utils

nlp = spacy.load('pt_core_news_sm')
consulta = DataAccess.Twitters.twitters.find({'entidades':{'$exists':False}})

total = consulta.count()
index = 0

Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

for post in consulta:
    if not('text' in post):
        continue
    
    text = post['text']
    text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)","",text).split()).replace('RT :','').strip()
    doc = nlp(text)
    entidades = []
    
    for e in doc.ents:
        if (e.label_ == 'LOC' or e.label_ == 'PER' or e.label_ == 'ORG'):
            entidades.append(e.string.strip())
    
    DataAccess.Twitters.twitters.update_one({'_id':ObjectId(post['_id'])},{"$set":{'entidades':entidades}},upsert=False)
    index += 1
    Helpers.Utils.printProgressBar(index+1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    






