 # -*- coding: utf-8 -*-
from datetime import date
from dateutil import relativedelta as rdel
import pdb
import collections
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from process import *

# test samples
files=['traindata/data_%s.csv' % s for s in ('Y','Q','M','D')]
files[2]='traindata/data_M_initZero.csv'
tsAll=tsy,tsq,tsm,tsd=[pd.read_csv(data) for data in files]
valAll=valY,valQ,valM,valD=[t['value'].tolist() for t in tsAll]

tsY,tsQ,tsM,tsD=[pd.DataFrame(v,index=pd.PeriodIndex(t['date'],freq=f),columns=['value']) \
                 for v,t,f in [(valY,tsy,'Y'),(valQ,tsq,'Q'),(valM,tsm,'M'),(valD,tsd,'D')]]

class timeseries():
# time series processor
    
    def __init__(self,df,period_years=3):
        self.update(df)
        self.period_years=period_years
    
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
        
        rates_year={'Y':1,'Q':4,'M':12}
        r*=rates_year[datetype]
        
        # if the data is not complete, then multiply by a discount ratio to the growth rate
        if conservative:
            r*=len(xx)/float(self.getPeriods(datetype))
        
        return [a1,r]
    
    def plotFit(self,datetype):
        ans=self.calRate(datetype)
        df_fit=self.getTimeSeriesForFit(datetype)
        
        x=df_fit.index.tolist()
        y=df_fit['value'].tolist()
    
        rates_year={'Y':1,'Q':4,'M':12}
        f=lambda x : ans[0]*(1+ans[1]/float(rates_year[datetype]))**(x-1)
        
        x_fit=range(1,self.getPeriods(datetype)+1)
        y_fit=f(np.array(x_fit))
        
        plt.scatter(x,y)
        plt.plot(x_fit,y_fit)
    
    def getMean(self):
        return self.df.mean()['value']

    def getStd(self):
        return self.df.std()['value']

class eval():
    
    def __init__(self,stkid,period_year=3,return_rate=0.15):
        # --- basic
        if type(stkid)==str:
            self.id=url.getStockID(stkid)
        else:
            self.id=stkid
        self.name=url.getStockName(stkid)
        self.period_year=period_year
        self.return_rate=return_rate
        self.initProcess()
        self.updateDF()
        self.updateTS()
        self.calGrowthRate()
        self.current_price=self.pPrcCurrent.getNewestPrice()
    
    def initProcess(self):
        # --- initialization
        self.pDiv=PcsDiv(self.id)
        
        self.pInc=PcsInc(self.id)
        self.pBal=PcsBal(self.id)
        self.pCsh=PcsCsh(self.id)
        
        self.pSal=PcsSal(self.id)
        self.pPrc=PcsPrcMonth(self.id)
        self.pPer=PcsPerFast(self.pInc,self.pPrc,self.id)
        
        self.pPrcCurrent=PcsPrcCurrent(self.id)
            
    def updateDF(self):

        mY=self.period_year
        pQ_end=Period("%sQ%s" % (ThisYear,ThisQuarter))-1
        pM_end=Period("%s/%s" % (ThisYear,ThisMonth))-1
        
        print "update dataframes..."
        
        # -- no period needed
        self.dfDiv=self.pDiv.getDataFrame(self.pDiv.getWebContent())[-mY:]
        
        # -- quarter-type period
        self.dfInc=self.pInc.getDataFramePeriods(pQ_end-4*mY+1,pQ_end,'Q-DEC')
        self.dfBal=self.pBal.getDataFramePeriods(pQ_end-4*mY+1,pQ_end,'Q-DEC')
        self.dfCsh=self.pCsh.getDataFramePeriods(pQ_end-4*mY+1,pQ_end,'Q-DEC')
    
        # -- month-type period
        self.dfSal=self.pSal.getDataFramePeriods(pM_end-12*mY+1,pM_end,'M')
        #self.dfPrc=self.pPrc.getDataFramePeriods(pM_end-12*mY+1,pM_end,'M','big5')
        self.dfPer=self.pPer.getDataFramePeriods(pM_end-12*mY+1,pM_end,'M','big5')
    
    def updateTS(self):
        
        print "update timeseries..."
        
        # for DDM price
        self.tsDiv=timeseries(self.dfDiv,self.period_year)
        
        # for growth rate
        self.tsInc=timeseries(self.dfInc,self.period_year)
        self.tsBal=timeseries(self.dfBal,self.period_year)
        self.tsSal=timeseries(self.dfSal,self.period_year)
        self.tsCsh=timeseries(self.dfCsh,self.period_year)
    
        # for neweset price & averaged PER
        #self.tsPrc=timeseries(self.dfPrc,self.period_year)
        #self.tsPer=timeseries(self.dfPer,self.period_year)
        
    def calGrowthRate(self):
        
        print "calculate growth rate..."
        
        # --- growth rate calculation
        self.rateInc=self.tsInc.calRate('Q')[1]
        self.rateBal=self.tsBal.calRate('Q')[1]
        self.rateCsh=self.tsCsh.calRate('Q')[1]
        
        self.rateSal=self.tsSal.calRate('M')[1]
        if self.rateSal<0:self.rateSal=0.0
        
        return [self.rateInc,self.rateBal,self.rateSal,self.rateCsh]
    
    def getEPSTTM(self):
        return self.dfInc[-4:]['value'].sum()
    
    def calPriceDiv(self,return_rate=0.15):
    # -- price dividend
        div_mean=self.tsDiv.getTimeSeries('Y')['value'].mean()
        priceDiv=div_mean/return_rate

        return [div_mean,priceDiv]

    def show(self,width=20):
        print "%-*s%s" % (width,"stock name",self.name)
        print "%-*s%s" % (width,"stock id",self.id)
    
    def lineprint(self,title,value,digit,width=20):
        return "%-*s%0.*f" % (width,title,digit,value)
    
    def lineprints(self,title_value_pairs,digits,width=20):
        # ex:   title_value_pairs : {'height':2.31,'length':13.16}
        #       digits=[2,1]
        digits_dict=dict(zip(title_value_pairs.keys(),digits))
        for (key,value) in title_value_pairs.items():
            print self.lineprint(key,value,digits_dict[key],width)

    def report(self):
        print "\n--------- basic ---------"
        self.show()
        self.lineprints(collections.OrderedDict([
                        ("set return years"         ,self.period_year),
                        ("set return rate"          ,self.return_rate)]),
                        [0,2])
        
        print "\n--------- price for dividend ---------"
        print self.tsDiv.getTimeSeries('Y')
        div_mean,priceDiv=self.calPriceDiv()
        self.lineprints(collections.OrderedDict([
                        ("averaged dividend"        ,div_mean),
                        ("price dividend"           ,priceDiv)]),
                        [2,1])
        
        print "\n--------- price for growth ---------"
        fv=lambda pv,r,nper : pv*(1+r)**nper
        pv=lambda fv,r,nper : fv/((1+r)**nper)
        
        rateEPS=self.rateInc
        rateBVPS=self.rateBal
        rateSALE=self.rateSal
        rateCASH=self.rateCsh
        rates=[rateEPS,rateBVPS,rateSALE,rateCASH]
        #rates=[rateEPS,rateBVPS,rateCASH]
        growth_rate=np.mean(rates+[min(rates)])
        
        dfQ=self.tsInc.getTimeSeries('Q').rename(columns={'value':'eps'})
        dfBvps=self.tsBal.getTimeSeries('Q')
        dfFCF=self.tsCsh.getTimeSeries('Q')
        dfQ['bvps']=dfBvps
        dfQ['fcf']=dfFCF
        
        epsTTM=self.getEPSTTM()
        
        per_mean=self.dfPer['value'].mean()
        per_std=self.dfPer['value'].std()
        perAVE=self.dfPer[abs(self.dfPer.value-per_mean)<per_std].mean().iat[0]
        eps10Y=fv(epsTTM,growth_rate,10)
        future_retail_value=eps10Y*perAVE
        MOS=0.5
        sticker_price=pv(future_retail_value,self.return_rate,10)
        growth_price=sticker_price*MOS
        
        self.lineprints(collections.OrderedDict([
                        ("eps TTM"              ,epsTTM),
                        ("per AVE"              ,perAVE)]),
                        [2,2])
        print dfQ
    
        self.lineprints(collections.OrderedDict([
                        ("eps rate"             ,rateEPS),
                        ("bvps rate"            ,rateBVPS),
                        ("fcf rate"             ,rateCASH)]),
                        [3,3,3])

        print self.tsSal.getTimeSeries('M')
       
        self.lineprints(collections.OrderedDict([
                        ("sale rate"           ,rateSALE),
                        ("growth rate"         ,growth_rate),
                        ("eps 10Y"             ,eps10Y),
                        ("retial value 10Y"    ,future_retail_value),
                        ("MOS"                 ,MOS),
                        ("sticker price"       ,sticker_price),
                        ("price growth"        ,growth_price)]),
                        [3,3,2,1,3,1,1])
     
        print "\n--------- final ---------"
        current_price=self.current_price['value'][0]
        eval_price=priceDiv+growth_price
        cheap_ratio=current_price/eval_price
        div_ratio=priceDiv/eval_price
        
        self.lineprints(collections.OrderedDict([
                        ("current price"        ,current_price),
                        ("evaluated price"      ,priceDiv+growth_price),
                        ("cheap ratio"          ,cheap_ratio),
                        ("dividend ratio"       ,div_ratio)]),
                        [1,1,2,2])

if __name__=="__main__":
    print "in core..."
    pass
