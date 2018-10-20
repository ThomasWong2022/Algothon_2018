import algodatabase as algodb

import pandas as pd

import quandl

def load_kaggle_price(csvfilepath):
    
    pricedf=pd.read_csv(csvfilepath)
    arcticstore=algodb.arctichost('localhost')
    algodb.df2arctic(pricedf,arcticstore,'Kaggle','Price')
    return None 


# Quandlticker a list of 
def load_Quandl_data(Quandldatabase,Quandlticker,**kwargs):
    quandl.ApiConfig.api_key = 'kEmHzbLUntXUqTFuREVV'
    if Quandldatabase=='SHARADAR/SF1':
        df=quandl.get_table('SHARADAR/SF1',ticker=Quandlticker)
    # Insert to arctic DB   
    return df

def load_fundamental(csvfile,collectionname,groupkey,datekey):
    reader=pd.read_csv(csvfile,chunksize=10000)
    arcticstore=algodb.arctichost('localhost')
    for chunk in reader:
        grouped=chunk.groupby(groupkey)
        for name,group in grouped:
            group=group.set_index(datekey)
            algodb.df2arctic(group,arcticstore,collectionname,name)
    return None

def debug_database():
    arcticstore=algodb.arctichost('localhost')
    libraries=arcticstore.list_libraries()
    for lib in libraries:
        symbols=arcticstore[lib].list_symbols()
        for symbol in symbols:
            df=algodb.arctic2df(arcticstore,lib,symbol)
            df.drop_duplicates(keep='last',inplace=True)
            arcticstore[lib].delete(symbol)
            algodb.df2arctic(df,arcticstore,lib,symbol)
    return None

def arctic2df(store,arcticcollectionname,ticker,start=None,end=None):
    df=pd.DataFrame()
    return df 



if __name__ == '__main__':
    load_kaggle_price()
    #load_fundamental('D:\Algothon_2018\Algothon_2018\Raw_data\SHARADAR_SF1.csv','SHARADAR_SF1','ticker','calendardate')
    #load_fundamental('D:\Algothon_2018\Algothon_2018\Raw_data\IFT_NSA.csv','IFA_NSA','ticker','date')
    #debug_database()






