# -*- coding: utf-8 -*-
import DataAccess.Twitters
import Helpers.Utils
from bson.objectid import ObjectId

consulta = DataAccess.Twitters.twitters.find({'criacao':{'$exists':False}})
total = consulta.count()
index = 0

Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

for post in consulta:
    
    if not ('created_at' in post):
        continue
            
    data = Helpers.Utils.convertTwitterUTCTimeToLocal(post['created_at'])
    DataAccess.Twitters.updatePostDataCriacao(post,data)
    Helpers.Utils.printProgressBar(index+1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    index += 1
    

