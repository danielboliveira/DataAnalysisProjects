# -*- coding: utf-8 -*-
import DataAccess.Twitters
import Helpers.Utils
import pprint
import numpy as np
from matplotlib import pyplot as plt
import datetime

termo = 'bolsonaro'
__sentimento = 'positivo'

totaisDia   = DataAccess.Twitters.getTotalCitacoesOrganicaMesDia()
totaisTermo = DataAccess.Twitters.getTotalCitacaoOrganica(termo)
totais = np.array(totaisTermo)

row = 0
for __t in totaisTermo:
    __id = __t[0]
    totalDia = [ (id,dia,mes,ano,total) for (id,dia,mes,ano,sentimento,total) in totaisDia if id == __id and sentimento == __t[5]]
    
    if (len(totalDia) > 0):
       totais[row,7] = (float(__t[6])/totalDia[0][4])*100
    else:
        totais[row,7] = 0
        
    row += 1

pprint.pprint(totaisDia)
pprint.pprint(totais)



