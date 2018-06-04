# -*- coding: utf-8 -*-
"""
Created on Tue May 22 14:04:42 2018

@author: daniel
"""
import spacy

nlp = spacy.load("pt_core_news_sm")

def loadTrainedModelSPACY():
    nlp.from_disk('C:/Cloud/Google Drive/Documents/python/TwitterAnalyzer/Model/')
    
def getDocumentSPACY(texto,analyzer):
    doc = analyzer(texto)
    return doc

