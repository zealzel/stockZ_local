# -*- coding: utf-8 -*-
from urllib2 import urlopen
from datetime import datetime

from core import *
from fetch import *

Fan=url.Fan
Mkt=url.Mkt
Now=datetime.now()
ThisYear=Now.year

class Pcs():
    def getWebContent(self,Url):
        res=urlopen(Url)
        content=res.read().decode('utf-8')
        return content
    
    def getDataFrame(self): pass
    
    def getTS(self,df,period,type):
        ts=timeseries(df,period)
        tss=ts.getTimeSeries(type)
        tsFit=ts.getTimeSeriesForFit(type)
        return [tss,tsFit]

class PcsDiv(Pcs):
    
    def __init__(self,id=None):
        self.id=id
    
    def getWebContent(self):
        u=url.get(Fan.DIV,Mkt.ALL,pd.Period(2000),self.id)
        return Pcs.getWebContent(self,u)
    
    def getTS(self):
        # get dividend history data
        df=self.getDataFrame(self.getWebContent())
        return Pcs.getTS(self,df,10,'Y')
    
    def getDataFrame(self,content):
        df=pd.io.html.read_html(content,infer_types=False)
        df1=df[1][range(5)][3:]
        df2=df[5][[0,1,2,7,10]][3:]
        
        df1.columns=['Div_year','Date','Section','DivCash','DivStock']
        df2.columns=['Div_year','Date','Section','DivCash','DivStock']
        
        df2=df2.set_index('Div_year')
        df1=df1.set_index('Div_year')
        
        dfDiv=df2.append(df1)
        
        dfDiv[['DivCash','DivStock']]=dfDiv[['DivCash','DivStock']].astype(float)
        dfDiv.index=dfDiv.index.astype(int)+1911+1
        dfDiv.index=[pd.Period(d) for d in dfDiv.index]
        
        dfDiv['DivCash'][(dfDiv.DivCash<0)|np.isnan(dfDiv.DivCash)]=0.0
        dfDiv['DivStock'][(dfDiv.DivStock<0)|np.isnan(dfDiv.DivStock)]=0.0
        
        d=dfDiv[['DivCash']]
        d.columns=['value']
        
        return d


class PcsInc(Pcs):
    
    def __init__(self,id=None):
        self.id=id
    
    def getWebContent(self):
        pass
        #u=url.get(Fan.DIV,Mkt.ALL,pd.Period(2000),self.id)
        #return Pcs.getWebContent(self,u)
    
    def getTS(self):
        pass
        # get dividend history data
        #df=self.getDataFrame(self.getWebContent())
        #return Pcs.getTS(self,df,10,'Y')
    
    def getDataFrame(self,content):
        df=pd.io.html.read_html(content,infer_types=False)



