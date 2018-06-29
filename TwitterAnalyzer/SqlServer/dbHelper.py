# -*- coding: utf-8 -*-
import pyodbc

__cnxn = pyodbc.connect('DSN=sqlTwitter;uid=sa;PWD=sa',autocommit=True)
__cursor = __cnxn.cursor()

def getCursor():
    global __cnxn
    if not __cnxn:
        getConnection()
    return __cursor

def getConnection():
    global __cnxn
    global __cursor
    
    if not __cnxn:
        __cnxn = pyodbc.connect('DSN=sqlTwitter;uid=sa;PWD=sa',autocommit=True)
        __cursor = __cnxn.cursor()
    
    return __cursor,__cnxn

def Commit():
    __cursor.commit()
    
def CloseConnection():
    try:
        __cursor.close()
    except:
        pass
    
__all__ = ["getConnection","Commit","CloseConnection"]


