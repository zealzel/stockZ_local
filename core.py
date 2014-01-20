 # -*- coding: utf-8 -*-
from datetime import date
from dateutil import relativedelta as rdel
import pandas as pd
import numpy as np

# test samples
tsAll=tsy,tsq,tsm,tsd=[pd.read_csv(data) for data in ['data_Y.csv','data_Q.csv','data_M.csv','data_D.csv']]
valAll=valY,valQ,valM,valD=[t['value'].tolist() for t in tsAll]

tsY,tsQ,tsM,tsD=[pd.DataFrame(v,index=pd.PeriodIndex(t['date'],freq=f),columns=['value']) \
                 for v,t,f in [(valY,tsy,'Y'),(valQ,tsq,'Q'),(valM,tsm,'M'),(valD,tsd,'D')]]

class timeseries():
# time series processor
    def __init__(self,df):
        self.update(df)
    
    def update(self,df):
        self.df=df

    def getSorted(self):
        df_sorted=self.df.sort()
        return df_sorted
    
    def getTimeSeries(self,datetype): # datetype: one of ['Y','Q','M','D']
        '''
        return the DataFrame which is sorted and with periodindex filled in fixed range
        '''
        df_sorted=self.getSorted()
        period_years=3
        periods=dict(zip(['Y','Q','M','D'], \
                         [period_years,4*period_years,12*period_years,365*period_years]))
        
        last_period=df_sorted.index[-1]
        first_period=last_period-periods[datetype]+1
        
        prng=pd.period_range(first_period,last_period,freq=datetype)
        df_period=df_sorted.reindex(prng)
        return df_period
    
    def getTimeSeriesForFit(self,datetype):
        '''
        return the DataFrame which index ranges from 1 to N and omits the row data if value is nan
        '''
        df_period=self.getTimeSeries(datetype)
        y=df_period['value'].tolist()
        x=range(1,len(y)+1)
        x=[x[i] for i in range(len(y)) if ~np.isnan(y[i])]
        y=[y[i] for i in range(len(y)) if ~np.isnan(y[i])]
        return pd.DataFrame(y,index=x,columns=['value'])
    
    def calRate(self,datetype):
        df_fit=self.getTimeSeriesForFit(datetype)
        x=df_fit.index.tolist()
        y=df_fit['value'].tolist()
        A=np.matrix([[1]*len(x),np.array(x)-1]).T
        b=np.matrix(np.log(y)).T
        ans=(A.T*A).I*A.T*b
        a1=np.exp(np.array(ans)[0][0])
        r=np.exp(np.array(ans)[0][0])-1
        return [a1,r]
    
    def getMean(self):
        return self.df.mean()['value']

    def getStd(self):
        return self.df.std()['value']
