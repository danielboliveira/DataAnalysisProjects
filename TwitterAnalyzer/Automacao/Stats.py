# -*- coding: utf-8 -*-
import Helpers.Utils as utils
import traceback
import logging
from SqlServer import Analysis as an
import matplotlib.pyplot as plt_pie

#plt = df.plot(figsize=(10,4),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento')    
#fig = plt.get_figure()
#fig.savefig("output.png")        
#plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento')        


def generateStatsSentimentoGraphs(consulta_id): 
    try:    
        termo = an.getTermo(consulta_id)
        
        if (not termo):
            return
        
        df = an.getStatsTwitters(consulta_id)
        
        path = utils.getAnalisesPath(consulta_id)
        
        file_graph_1 = path+"\\"+"sentimentos_line.png"
        utils.removeFile(file_graph_1)
        plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))        
        fig = plt.get_figure()
        fig.savefig(file_graph_1)        
        
        file_graph_2 = path+"\\"+"sentimentos_bar.png"
        utils.removeFile(file_graph_2)
        plt = df.plot(figsize=(10,4),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))    
        fig = plt.get_figure()
        fig.savefig(file_graph_2)        
        
        #Somentes os posts de influência
        
        df = an.getStatsTwitters(consulta_id,True)
        file_graph_1 = path+"\\"+"sentimentos_line_influencia.png"
        utils.removeFile(file_graph_1)
        plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))        
        fig = plt.get_figure()
        fig.savefig(file_graph_1)        
        
        file_graph_2 = path+"\\"+"sentimentos_bar_influencia.png"
        utils.removeFile(file_graph_2)
        plt = df.plot(figsize=(10,4),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))    
        fig = plt.get_figure()
        fig.savefig(file_graph_2) 
        
        
    except Exception as e:
        logging.error(traceback.format_exc())
        
    
def generateStatsSentimentoPieGraph(consulta_id): 
    try:    
        df = an.getStatsTwitters(consulta_id)
        
        path = utils.getAnalisesPath(consulta_id)
        
        file_graph_1 = path+"\\"+"sentimentos_pie.png"
        utils.removeFile(file_graph_1)
        
        
        ds = df.describe()
        r = ds.drop(['std','count','min','max','25%','50%','75%']).drop(['Negativos','Neutros','Positivos'],axis=1)
        
        prcNeg = r['%Negativos'][0]
        prcPos = r['%Positivos'][0]
        prcNeu = r['%Neutros'][0]
        
        # Data to plot
        labels = 'Negativos', 'Positivos', 'Neutros'
        sizes = [prcNeg, prcPos, prcNeu]
        colors = ['red', 'blue', 'silver']
#        explode = (0.1, 0, 0, 0)  # explode 1st slice
         
        # Plot
        plt_pie.pie(sizes, labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=140)
        plt_pie.axis('equal')
        plt_pie.show()
        plt_pie.savefig(file_graph_1)        
        
    except Exception as e:
        logging.error(traceback.format_exc())
