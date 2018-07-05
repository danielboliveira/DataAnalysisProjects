# -*- coding: utf-8 -*-
import DataAccess.Twitters
import pprint
import numpy as np
from matplotlib import pyplot as plt


termo = 'dilma'

totaisDia   = DataAccess.Twitters.getTotalCitacoesMesDia()
totaisTermo = DataAccess.Twitters.getTotalCitacao(termo)
totais = np.array(totaisTermo)

row = 0
for __t in totaisTermo:
    __id = __t[0]
    totalDia = [ (id,dia,mes,ano,total) for (id,dia,mes,ano,total) in totaisDia if id == __id]
    
    if (len(totalDia) > 0):
       totais[row,6] = (float(__t[5])/totalDia[0][4])*100
    else:
        totais[row,6] = 0
        
    row += 1

pprint.pprint(totais)

percents = [row[6] for row in totais]
xs = [float(i)+0.1 for i,_ in enumerate(percents)]
plt.bar(xs,percents)

plt.ylabel("% nas citações por dia")
plt.title("Citações do termo:" + termo)

dias = [row[1] for row in totais]
plt.xticks([i+0.1 for i,_ in enumerate(dias)],dias, rotation='vertical') 

plt.show()


