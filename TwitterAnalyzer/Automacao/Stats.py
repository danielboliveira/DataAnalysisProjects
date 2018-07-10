# -*- coding: utf-8 -*-
import Helpers.Utils as utils
import traceback
import logging
from SqlServer import Analysis as an
import matplotlib.pyplot as mplt
import matplotlib.dates as mdates
import datetime

#plt = df.plot(figsize=(10,4),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento')    
#fig = plt.get_figure()
#fig.savefig("output.png")        
#plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento')        


def generateStatsSentimentoGraphs(consulta_id,time_series=False,resample='H',xticks = 'd'): 
    try:    
        termo = an.getTermo(consulta_id)
        
        if (not termo):
            return
        
        if (time_series == False):
            df = an.getStatsTwitters(consulta_id)
        else:
            df = an.getStatsTwittersTimeSeries(consulta_id,resample = resample,somente_influenciadores = False)
        
        path = utils.getAnalisesPath(consulta_id)
        mask = '%Y_%m_%d'
        file_prefix =  datetime.datetime.now().strftime(mask)

        file_graph_1 = path+"\\"+"{0}_sentimentos_line.png".format(file_prefix)        
        utils.removeFile(file_graph_1)
        
        if not time_series:
            plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))        
        else:
            plt = df.plot(figsize=(20,5),kind='line',x=df.index,y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))        
           
        
        fig = plt.get_figure()
        fig.savefig(file_graph_1)
        
        file_graph_2 = path+"\\"+"{0}_sentimentos_bar.png".format(file_prefix)
        utils.removeFile(file_graph_2)
        
        if not time_series:
            plt = df.plot(figsize=(10,6),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))    
        else:
            plt = df.plot(figsize=(10,6),kind='bar',x=df.index,rot=45,y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))    
            
            


        fig = plt.get_figure()
        fig.savefig(file_graph_2)

        #Somentes os posts de influência
        if (time_series == False):
            df = an.getStatsTwitters(consulta_id,True)
        else:
            df = an.getStatsTwittersTimeSeries(consulta_id,resample = resample,somente_influenciadores = True)
        
        
        file_graph_1 = path+"\\"+"{0}_sentimentos_line_influencia.png".format(file_prefix)
        utils.removeFile(file_graph_1)
        
        if not time_series:
            plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0}) - Apenas Perfis de Influência'.format(termo.upper()))        
        else:
            plt = df.plot(figsize=(20,5),kind='line',x=df.index,y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0}) - Apenas Perfis de Influência'.format(termo.upper()))        
            
        fig = plt.get_figure()
        fig.savefig(file_graph_1)        
        
        file_graph_2 = path+"\\"+"{0}_sentimentos_bar_influencia.png".format(file_prefix)
        utils.removeFile(file_graph_2)
        
        if not time_series:
            plt = df.plot(figsize=(10,4),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0}) - Apenas Perfis de Influência'.format(termo.upper()))
        else:
            plt = df.plot(figsize=(10,4),kind='bar',x=df.index,y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0}) - Apenas Perfis de Influência'.format(termo.upper()))    
        
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
        mplt.pie(sizes, labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=140)
        mplt.axis('equal')
        mplt.savefig(file_graph_1)        
        
    except Exception as e:
        logging.error(traceback.format_exc())
