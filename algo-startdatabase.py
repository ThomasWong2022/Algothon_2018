import algodatabase as algodb

import pandas as pd

import quandl

def load_kaggle_price(csvfilepath):
    
    reader=pd.read_csv(csvfilepath,chunksize=10000000)
    arcticstore=algodb.arctichost('localhost')
    arcticstore.initialize_library('Kaggle')
    arcticstore.set_quota('Kaggle',1000*1024*1024*1024)
    for pricedf in reader:
        print(pricedf.dtypes)
        pricedf['time'] =  pd.to_datetime(df['time'])
        pricedf.set_index('time',inplace=True)
        algodb.df2arctic(pricedf,arcticstore,'Kaggle','Price')
        print(pricedf)
        user_input=input('Pause: ')
        if user_input=='N':
            arcticstore['Kaggle'].delete('Price')
            break
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

def retuters_news(folderpath):

    def _list_files(currentpath):
        from os import listdir 
        from os.path import isfile, join 
        files = [join(currentpath, f) for f in listdir(currentpath) if isfile(join(currentpath, f))]
        return files





if __name__ == '__main__':
    #load_kaggle_price()
    #load_fundamental('D:\Algothon_2018\Algothon_2018\Raw_data\SHARADAR_SF1.csv','SHARADAR_SF1','ticker','calendardate')
    #load_fundamental('D:\Algothon_2018\Algothon_2018\Raw_data\IFT_NSA.csv','IFA_NSA','ticker','date')
    #debug_database()

    import pickle
    with open('D:/Algothon_2018/Algothon_2018/kaggle_train_datasets/reuters/20070125.pkl', 'rb') as f:
        data = pickle.load(f)
        df=pd.DataFrame(data)
        print(df)

    load_kaggle_price('D:/Algothon_2018/Algothon_2018/kaggle_train_datasets/mystery_symbol_train/mystery_symbol_train.csv')





        






