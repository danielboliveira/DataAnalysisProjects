# -*- coding: utf-8 -*-
import sys
from SqlServer import Analysis as an

try:
    consulta_id = int(sys.argv[1])
except:
    aux = input("Informe o código da consulta: ")
    consulta_id = int(aux)
    
df = an.getStatsRTS(consulta_id)
