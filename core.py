# -*- coding: utf-8 -*-
from datetime import date
from dateutil import relativedelta as rdel
import pandas as pd

y1={'year':[1993,1994,1995,1996,1997], \
    'value':[3,1,2,4,7]}

y2={'year':[1994,1993,1995,1996,1997], \
    'value':[1,3,2,4,7]}


def dateDiff(d1,d2,type):
    delta=rdel.relativedelta(d1,d2)
    if type=='y':
        diff=delta.years
    elif type=='q':
        diff=delta.years*4+delta.months/4
    elif type=='m':
        diff=delta.years*12+delta.months
    else:
        diff=delta.years*365+delta.days
    return diff

class timeseries():
# time series processor
    def __init__(self,df):
        self.update(df)
    
    def update(self,df):
        self.df=df

    def getSorted(self):
        df_sorted=self.df.sort(['y','q','m','d'],ascending=[True]*4)
        return df_sorted
    
    def getTimeSeries(self,datetype):
        df_sorted=self.getSorted()
        
        df_sorted['index']=range(1,len(df_sorted)+1)
        
        if datetype=='y':
            pass
        elif datetype=='q':
            pass
        elif datetype=='m':
            d1=df_sorted.iat[0,0]
            df_time_series=3
        else:
            pass
        return df_sorted

class frameYear():
    def __init__(self,yData):
        self.data=pd.DataFrame(yData)
    
    def update(self,yData):
        self.data=pd.DataFrame(yData)

    def ave(self):  
        pass
    
    def sort(self):
        pass