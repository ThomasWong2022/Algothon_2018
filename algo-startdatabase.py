import algodatabase as algodb

import pandas as pd

import quandl


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
            #arcticstore[lib].delete(symbol)
            algodb.df2arctic(df,arcticstore,lib,'cleaned_'+symbol)
    return None

def arctic2df(store,arcticcollectionname,ticker,start=None,end=None):
    df=pd.DataFrame()
    return df 






def load_kaggle_price(csvfilepath):
    
    reader=pd.read_csv(csvfilepath,chunksize=10000000)
    arcticstore=algodb.arctichost('localhost')
    arcticstore.initialize_library('Kaggle')
    arcticstore.set_quota('Kaggle',1000*1024*1024*1024)
    for pricedf in reader:
        pricedf['time'] =  pd.to_datetime(pricedf['time'])
        pricedf.set_index('time',inplace=True)
        pricedf=pricedf.resample('3T').mean()
        algodb.df2arctic(pricedf,arcticstore,'Kaggle','Price')
        print(pricedf)
        user_input=input('Pause: ')
        if user_input=='N':
            arcticstore['Kaggle'].delete('Price')
            break
    return None 




def process_news(newsdf):
    # Category of news
    urldf=newsdf['href'].str.split('http://www.reuters.com/',expand=True)[1].str.split('/',expand=True)
    try:
        urldf.columns=['Media_Type','Category','Document_ID']
    except:
        urldf.columns=['Media_Type','Category','Document_ID','Document_ID2','Document_ID3']
    newsdf=pd.concat([newsdf,urldf], axis=1, join='outer')
    newsdf.drop(columns='href',inplace=True)

    newsdf.reset_index(inplace=True)
    categorydf=newsdf.groupby([pd.Grouper(key='ts', freq='1H'),'Category'])['title'].count().unstack().fillna(0).astype(int)
    return categorydf

def retuters_news(folderpath):

    import pickle

    def _list_files(currentpath):
        from os import listdir 
        from os.path import isfile, join 
        files = [join(currentpath, f) for f in listdir(currentpath) if isfile(join(currentpath, f))]
        return files

    newsfiles=_list_files(folderpath)
    arcticstore=algodb.arctichost('localhost')
    faillist=[]
    counter=1
    for news in newsfiles:    
        with open(news, 'rb') as f:
            try:
                data = pickle.load(f)
                df=pd.DataFrame(data)
                df['ts']=pd.to_datetime(df['ts'])
                df.sort_values('ts',inplace=True)
                df.set_index('ts',inplace=True)
                arcticstore.set_quota('Reuters',1000*1024*1024*1024)
                [downloaded,ticker]=algodb.df2arctic(df,arcticstore,'Reuters','News_2017')
            except:
                faillist.append(news)
            #catdf=process_news(df)
            #[downloaded,ticker]=algodb.df2arctic(catdf,arcticstore,'Reuters','Processed_News',indexname='ts')
            if downloaded:
                print('Date added ',news)
                counter+=1
            else:
                print('Fail !!! ',news)
    faildf=pd.DataFrame({'Fail':faillist})
    faildf.to_csv('Failed_news.csv')


def _resample_price():
    arcticstore=algodb.arctichost('localhost')
    df=algodb.arctic2df(arcticstore,'Kaggle','Price','No')
    df=df.resample('3T').mean()
    df.fillna(method='pad',inplace=True)
    df.to_csv('D:/Resampled_price.csv')

def _resample_news():
    arcticstore=algodb.arctichost('localhost')

    df=algodb.arctic2df(arcticstore,'Reuters','News_2017','No')
    df.reset_index(inplace=True)
    newsdf=df.groupby(pd.Grouper(key='ts', freq='1H'))['title'].count().fillna(0).astype(int)
    newsdf.columns=['News_count']
    newsdf.to_csv('D:/Resampled_news2017.csv')

    df=algodb.arctic2df(arcticstore,'Reuters','News','No')
    df.reset_index(inplace=True)
    newsdf=df.groupby(pd.Grouper(key='ts', freq='1H'))['title'].count().fillna(0).astype(int)
    newsdf.columns=['News_count']
    newsdf.to_csv('D:/Resampled_news.csv')



if __name__ == '__main__':
    #load_kaggle_price()
    #load_fundamental('D:\Algothon_2018\Algothon_2018\Raw_data\SHARADAR_SF1.csv','SHARADAR_SF1','ticker','calendardate')
    #load_fundamental('D:\Algothon_2018\Algothon_2018\Raw_data\IFT_NSA.csv','IFA_NSA','ticker','date')
   
    #retuters_news('D:/Algothon_2018/Algothon_2018/kaggle_train_datasets/reuters')
    #print('one-third')
    #retuters_news('D:/Algothon_2018/Algothon_2018/kaggle_train_datasets/reuters2')
    #print('two-third')
    #retuters_news('D:/Algothon_2018/Algothon_2018/kaggle_train_datasets/reuters3')
    #print('all-done')
    _resample_news()
    #load_kaggle_price('D:/Algothon_2018/Algothon_2018/kaggle_train_datasets/mystery_symbol_train/mystery_symbol_train.csv')
    
    

    #debug_database()

    #_resample_price()






        






