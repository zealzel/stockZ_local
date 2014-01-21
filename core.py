 # -*- coding: utf-8 -*-
from datetime import date
from dateutil import relativedelta as rdel
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

# test samples
files=['traindata/data_%s.csv' % str for str in ('Y','Q','M','D')]
files[2]='traindata/data_M_initZero.csv'
tsAll=tsy,tsq,tsm,tsd=[pd.read_csv(data) for data in files]
valAll=valY,valQ,valM,valD=[t['value'].tolist() for t in tsAll]

tsY,tsQ,tsM,tsD=[pd.DataFrame(v,index=pd.PeriodIndex(t['date'],freq=f),columns=['value']) \
                 for v,t,f in [(valY,tsy,'Y'),(valQ,tsq,'Q'),(valM,tsm,'M'),(valD,tsd,'D')]]

class timeseries():
# time series processor
    
    def __init__(self,df):
        self.update(df)
        self.period_years=3
    
    def setPeriodYears(self,pYears):
        self.period_years=pYears
    
    def getPeriods(self,datetype):
        pYears=self.period_years
        periods=dict(zip(['Y','Q','M','D'], \
                         [pYears,4*pYears,12*pYears,365*pYears]))
        return periods[datetype]
    
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
        self.getPeriods(datetype)
        
        last_period=df_sorted.index[-1]
        first_period=last_period-self.getPeriods(datetype)+1
        
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
    
    def calRate(self,datetype,conservative=True):
        df_fit=self.getTimeSeriesForFit(datetype)
        x=df_fit.index.tolist()
        y=df_fit['value'].tolist()
        
        # exclude the data point which value is zero 
        xx=[x[i] for i in range(len(y)) if y[i]>0]
        yy=[y[i] for i in range(len(y)) if y[i]>0]
        
        
        A=np.matrix([[1]*len(xx),np.array(xx)-1]).T
        b=np.matrix(np.log(yy)).T
        ans=(A.T*A).I*A.T*b
        a1=np.exp(np.array(ans)[0][0])
        r=np.exp(np.array(ans)[1][0])-1
        
        # if the data is not complete, then multiply by a discount ratio to the growth rate
        if conservative:
            r*=len(xx)/float(self.getPeriods(datetype))
        
        return [a1,r]
    
    def plotFit(self,datetype):
        ans=self.calRate(datetype)
        df_fit=self.getTimeSeriesForFit(datetype)
        x=df_fit.index.tolist()
        y=df_fit['value'].tolist()

        f=lambda x : ans[0]*(1+ans[1])**(x-1)
        x_fit=range(1,self.getPeriods(datetype)+1)
        y_fit=f(np.array(x_fit))
        
        plt.scatter(x,y)
        plt.plot(x_fit,y_fit)
    
    def getMean(self):
        return self.df.mean()['value']

    def getStd(self):
        return self.df.std()['value']
