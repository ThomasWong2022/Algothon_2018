import algodatabase as algodb


import pandas as pd
import numpy as np
import matplotlib as plt
import ta

def prepare_data():
    df1=pd.read_csv('Resampled_news.csv',index_col=0,parse_dates=True,header=None)
    df2=pd.read_csv('Resampled_news2017.csv',index_col=0,parse_dates=True,header=None)
    volumedf=pd.concat([df1,df2])
    volumedf.columns=['News_count']
    volumedf.index.rename('time',inplace=True)

    pricedf=pd.read_csv('Resampled_price.csv',parse_dates=[0])
    pricedf.set_index('time',inplace=True)
    pricedf=pricedf.resample('1H').first()

    df=pricedf.merge(volumedf,on='time')
    df.to_csv('Resampled_data.csv')

    import ta
    df['open']=(df['bid']+df['ask'])/2
    df['close']=(df['bid']+df['ask'])/2

    df = ta.add_all_ta_features(df, "open", "ask", "bid", "close", "News_count", fillna=True)
    
    df.to_csv('Resampled_data_with_ta.csv')
    print(df.head())
    try:
        del df['time']
    except:
        print('No time column')
    return df


def _PCA_TA(df,column):
    period=181
    y=df[column].shift(period)
    train_ta=df.iloc[-3*period:-1*period, :].values
    newx=df.iloc[-1*period:, :].values
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    # Fit on training set only.
    scaler.fit(train_ta)
    train_ta = scaler.transform(train_ta)
    newx=scaler.transform(newx)
    from sklearn.decomposition import PCA
    # Make an instance of the Model
    pca = PCA(.95)
    pca.fit(train_ta)
    train_ta = pca.transform(train_ta)
    newx=pca.transform(newx)
    from sklearn.linear_model import LinearRegression
    y=y.dropna()
    y=y[-2*period:]
    reg = LinearRegression().fit(train_ta, y)
    newy=reg.predict(newx)
    return newy

def PCA_TA(df):
    openval=_PCA_TA(df,'open')
    closeval=_PCA_TA(df,'close')
    askval=_PCA_TA(df,'ask')
    bidval=_PCA_TA(df,'bid')
    import random 
    News_countval=[random.choice(df['News_count']) for i in range(181)]

    newdf=pd.DataFrame({'open':openval,'close':closeval,'bid':bidval,'ask':askval,'News_count':News_countval})
    df=df[["open", "ask", "bid", "close", "News_count"]]
    df=pd.concat([df,newdf],ignore_index=True)
    df = ta.add_all_ta_features(df, "open", "ask", "bid", "close", "News_count", fillna=True)
    return df

tadf=prepare_data()
for i in range(28):
    tadf=PCA_TA(tadf)
    print(tadf.tail())
tadf.to_csv('PCA_data.csv')
    








