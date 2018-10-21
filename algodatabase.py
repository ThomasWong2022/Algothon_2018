
from arctic import Arctic

import arctic

import os 
import pymongo
import pandas as pd

import timeit
import time
import json
import datetime
import random


def arctichost(host):
    return Arctic(host)

# Download data to arctic db, try to append to existing table or create a bigger table 
def df2arctic(df,store,arcticcollectionname,ticker,indexname=None,alternatename=''):
    # download df from mongo
    # remove duplicate documents(rows)
    # try to append to exisiting table in arctic 
    # if not possible then merge the existing tables to form a bigger table
    
    # Create collectionname if not exist
    try:
        library = store[arcticcollectionname]
    except:
        store.initialize_library(arcticcollectionname)       
    library = store[arcticcollectionname]

    # trying to append the data 
    try:
        library.append(ticker,df,metadata={'Name': alternatename})
        downloaded=True
    except:
        # Try to merge with the old dataframe
        try:
            temp=library.read(ticker)
            olddf=temp.data
            olddf.reset_index(inplace=True)
            df.reset_index(inplace=True)
            newdf=olddf.merge(df,how='outer')
            newdf.set_index(indexname,inplace=True)
            newdf.drop_duplicates(keep='last',inplace=True)
            library.write(ticker,newdf, metadata={'Name': alternatename})
            downloaded=True
        except:
            print(ticker,' not updated')
            downloaded=False
    return [downloaded,ticker]

def arctic2df(store,arcticcollectionname,ticker,filename):
    library = store[arcticcollectionname]
    df=library.read(ticker).data
    if filename!='No':
        df.to_csv(filename)
    return df


