import algodatabase as algodb


import pandas as pd
import numpy as np
import matplotlib as plt
import ta

def random_walk_parameter():   
    pricedf=pd.read_csv('Resampled_price.csv',parse_dates=[0])
    pricedf.set_index('time',inplace=True)
    pricedf['mid']=(pricedf['bid']+pricedf['ask'])/2
    mean=pricedf['mid'].mean()
    variance=pricedf['mid'].var()
    last=pricedf['mid'].iat[-1]
    return [mean,variance,last]

def _generate_random_walk(mean,variance,start,size):
    from scipy.stats import norm
    mylist=[]
    x=start
    for k in range(size):
        x = x + norm.rvs(scale=variance)
        mylist.append(x)
    return mylist

##[mean,variance,last]=random_walk_parameter()
#mylist=_generate_random_walk(mean,variance,last,5068)
#print(mylist)

def explore():   
    pricedf=pd.read_csv('Resampled_data.csv',parse_dates=[0])
    pricedf.set_index('time',inplace=True)
    pricedf['mid']=(pricedf['bid']+pricedf['ask'])/2
    
    import seaborn as sns
    import matplotlib.pyplot as plt
    sns.distplot(pricedf['mid'])
    plt.show()

    sns.heatmap(pricedf.corr(), annot=True, fmt=".2f")
    plt.show()

    import ta
    df=pricedf
    df['open']=(df['bid']+df['ask'])/2
    df['close']=(df['bid']+df['ask'])/2

    #df = ta.add_all_ta_features(df, "open", "ask", "bid", "close", "News_count", fillna=True)
    #sns.heatmap(df.corr(), annot=True, fmt=".2f")
    #plt.show()

    from matplotlib import pyplot
    from statsmodels.graphics.tsaplots import plot_acf
    plot_acf(df['mid'])
    pyplot.show()

def show_OU():
    import numpy as np
    import matplotlib.pyplot as plt

    num_sims = 5 ### display five runs

    t_init = 3
    t_end  = 7
    N      = 1000 ### Compute 1000 grid points
    dt     = float(t_end - t_init) / N 
    y_init = 0

    c_theta = 0.7
    c_mu    = 1.5
    c_sigma = 0.06

    def mu(y, t): 
        """Implement the Ornstein–Uhlenbeck mu.""" ## = \theta (\mu-Y_t)
        return c_theta * (c_mu - y)

    def sigma(y, t): 
        """Implement the Ornstein–Uhlenbeck sigma.""" ## = \sigma
        return c_sigma
        
    def dW(delta_t): 
        """Sample a random number at each call."""
        return np.random.normal(loc = 0.0, scale = np.sqrt(delta_t))

    ts    = np.arange(t_init, t_end, dt)
    ys    = np.zeros(N)

    ys[0] = y_init

    for _ in range(num_sims):
        for i in range(1, ts.size):
            t = (i-1) * dt
            y = ys[i-1]
            ys[i] = y + mu(y, t) * dt + sigma(y, t) * dW(dt)
        plt.plot(ts, ys)

    plt.show()
