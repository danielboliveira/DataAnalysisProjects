# -*- coding: utf-8 -*-
import Helpers.Utils as utils
import traceback
import logging
from SqlServer import Analysis as an
import matplotlib.pyplot as mplt
import matplotlib.dates as mdates
from matplotlib.dates import MONDAY
import datetime
import numpy as np

#plt = df.plot(figsize=(10,4),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento')    
#fig = plt.get_figure()
#fig.savefig("output.png")        
#plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento')        

def generateStatsSentimentoLineGraphs(consulta_id,somente_influenciadores=False,resample='D',export=False): 
    try:    
        termo = an.getTermo(consulta_id)
        
        if (not termo):
            return
        
        df = an.getStatsTwittersTimeSeries(consulta_id,resample = resample,somente_influenciadores=somente_influenciadores)
        
        
        xs = df.index.values
        ypos = df['%Positivos'].values
        yneg = df['%Negativos'].values
        yneu = df['%Neutros'].values
        
        if (resample.lower() == 'm'): #xticks Mês
            major_locator = mdates.MonthLocator(range(1, 13))
            minor_locator = mdates.WeekdayLocator(MONDAY) 
            major_formatter = mdates.DateFormatter('%b %y')
            xlabel = 'Mes(es)'
        else:
            major_locator = mdates.DayLocator() 
            minor_locator = mdates.DayLocator()
            major_formatter = mdates.DateFormatter('%d %b')
            xlabel = 'Dia(s)'
        
            
        fig, ax = mplt.subplots()
        ax.plot(xs,ypos,'b',label='% Positivos')
        ax.plot(xs,yneg,'r',label='% Negativos')
        ax.plot(xs,yneu,'gray',label='% Neutros')
        
        ax.xaxis.set_major_locator(major_locator)
        ax.xaxis.set_major_formatter(major_formatter)
        ax.xaxis.set_minor_locator(minor_locator)
       
        ax.set_ylim(0,1)
        ax.legend(loc='best', shadow=True, fontsize='small')
        ax.autoscale_view()
        
        fig.set_size_inches(10,4)
        fig.autofmt_xdate()

        
        mplt.xlabel(xlabel)
        mplt.ylabel('Percentuais')
        
        if (not somente_influenciadores):
            mplt.title('Análise sentimental - Termo:{0}'.format(termo))
        else:
            mplt.title('Análise sentimental - Termo:{0} (Apenas influenciadores)'.format(termo))

        if (not export):             
            mplt.show()  
            print(df)
            print()
            print(df.describe())
        else:
            path = utils.getAnalisesPath(consulta_id)
            mask = '%Y_%m_%d'
            file_prefix =  datetime.datetime.now().strftime(mask)
            
            if not somente_influenciadores:
                file = path+"\\"+"{0}_sentimentos_line.png".format(file_prefix)
            else:
                file = path+"\\"+"{0}_sentimentos_influencia_line.png".format(file_prefix)
                
            file_csv = path+"\\"+"{0}_dados.csv".format(file_prefix)
            file_describe_csv = path+"\\"+"{0}_dados_describe.csv".format(file_prefix)
            utils.removeFile(file)
            fig.savefig(file)
            df.to_csv(file_csv,sep='\t')
            df.describe().to_csv(file_describe_csv,sep='\t')
        
    except Exception as e:
        logging.error(traceback.format_exc())
        

def generateStatsSentimentoBarGraphs(consulta_id,somente_influenciadores=False,resample='D',export=False): 
    try:    
        termo = an.getTermo(consulta_id)
        
        if (not termo):
            return
        
        df = an.getStatsTwittersTimeSeries(consulta_id,resample = resample,somente_influenciadores=somente_influenciadores)
        
        
        xs = df.index.values
        ypos = df['%Positivos'].values
        yneg = df['%Negativos'].values
        yneu = df['%Neutros'].values

        fig, ax = mplt.subplots()
        fig.set_size_inches(10,4)
        
        ppos = mplt.bar(xs, ypos,color='b')
        pneu = mplt.bar(xs, yneu,bottom=ypos,color='gray')
        pneg = mplt.bar(xs, yneg,bottom=ypos+yneu,color='r')
        
        mplt.ylim([0,1])
        mplt.yticks(fontsize=12)
        mplt.ylabel('Percentuais', fontsize=12)
        mplt.legend((ppos[0],pneu[0],pneg[0]), ('Positivos', 'Negativos','Neutros'))
        
        if (resample.lower() == 'm'): #xticks Mês
            major_locator = mdates.MonthLocator(range(1, 13))
            minor_locator = mdates.WeekdayLocator(MONDAY) 
            major_formatter = mdates.DateFormatter('%b %y')
            xlabel = 'Mes(es)'
        else:
            major_locator = mdates.DayLocator() 
            minor_locator = mdates.DayLocator()
            major_formatter = mdates.DateFormatter('%d %b')
            xlabel = 'Dia(s)'
        
        mplt.xlabel(xlabel, fontsize=12)
        ax.xaxis.set_major_locator(major_locator)
        ax.xaxis.set_major_formatter(major_formatter)
        ax.xaxis.set_minor_locator(minor_locator)
        ax.set_ylim(0,1)
        ax.autoscale_view()
        
        if (not somente_influenciadores):
            mplt.title('Análise sentimental - Termo:{0}'.format(termo))
        else:
            mplt.title('Análise sentimental - Termo:{0} (Apenas influenciadores)'.format(termo))

        if (not export):             
            mplt.show()  
        else:
            path = utils.getAnalisesPath(consulta_id)
            mask = '%Y_%m_%d'
            file_prefix =  datetime.datetime.now().strftime(mask)
            
            if not somente_influenciadores:
                file = path+"\\"+"{0}_sentimentos_bar.png".format(file_prefix)
            else:
                file = path+"\\"+"{0}_sentimentos_influencia_bar.png".format(file_prefix)

            utils.removeFile(file)
            fig.savefig(file)
        
    except Exception as e:
        logging.error(traceback.format_exc())

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
