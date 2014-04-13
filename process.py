# -*- coding: utf-8 -*-
import urllib2
import socket
import timeit,time
import pandas as pd
import numpy as np
from re import compile
from abc import ABCMeta,abstractmethod
import logging
logging.basicConfig(level=logging.DEBUG)
logger=logging.getLogger(__name__)

from fetch import *

import pdb

Fan=stkBase.Fan
Mkt=stkBase.Mkt
TIMEOUT=30

class urlopenByProxy():
    def __init__(self,proxy="http://proxy.hinet.net:80/",Timeout=30):
        self.opener=urllib2.build_opener(urllib2.ProxyHandler({'http':proxy}))
        socket.setdefaulttimeout(Timeout)
    
    def open(self,url,TimeOut):
        return self.opener.open(url,timeout=TimeOut)

class Pcs():
    __metaclass__=ABCMeta
    proxy='http://proxy.hinet.net:80/'
    #proxy='http://61.219.36.249:80/'
    
    def __init__(self,id=None):
        self.urlProxy=urlopenByProxy(Pcs.proxy)
        self.enableProxyConnection(False)
        self.updateStkId(id)
    
    def updateStkId(self,id):
        if id!=None:
            self.id=id
            self.market=stkBase.getMkt(self.id)

    def enableProxyConnection(self,enable=True):
        self.enableproxy=enable
    
    def getWebContent(self,Url,Decode='utf-8'):
        logger.debug("======= fetching data =======")
        try:
            if self.enableproxy:
                logger.debug("---enable proxy---")
                res=self.urlProxy.open(Url,TimeOut=TIMEOUT)
            else:
                logger.debug("---disable proxy---")
                res=urllib2.urlopen(Url,timeout=TIMEOUT)
    
            logger.debug(" getting contents...")
            content=res.read().decode(Decode)
                    
        except Exception as ex:
            logger.debug("error in Pcs.getWebContent: %s " % (ex))
            raise
        return content
    
    def getWebContentFlexibleProxy(self,Url,Decode='utf-8'):        
        # 在開開關關proxy的狀態中存取網頁
        while (True):
            try:
                content=Pcs.getWebContent(self,Url,Decode)
                break
            except Exception as ex:
                logger.debug("error:%s" % (ex))
                self.enableProxyConnection(not self.enableproxy)
        
        return content
    
    @abstractmethod
    def getDataFrame(self):
        '''
        針對單一個股分析, 將擷取之網頁資料利用pandas.io.html.read_html轉為DataFrame
        '''
        pass



class PcsDiv(Pcs):
    
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def getWebContent(self):
        u=url.get(Fan.DIV,pd.Period(2000),self.id)
        content=Pcs.getWebContent(self,u)
        return content
    
    def getDataFrame(self,content):
        df=pd.io.html.read_html(content,infer_types=False)
        df1=df[1][range(5)][3:]
        
        # 兩種表格模式,有無第三種?
        if len(df)==8: # 2317
            df2=df[5][[0,1,2,7,10]][3:]
        elif len(df)==4: # ex:2705
            df2=df[3][[0,1,2,7,10]][3:]
        
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
        d.sort(inplace=True)
        d=d.groupby(level=0).last()
        
        return d

class PcsDataFrame(Pcs):
    '''
    ---將網頁內容轉成pandas.DataFrame之處理器---
    '''
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def printBasic(self,period):
        logger.debug("  stock id=%s" % (self.id))
        logger.debug("  data :%s" % (period))
    
    def getUrl(self,period): pass
    def processWebContent(self,content,period): return content
    def getWebContent(self,period,Decode='utf-8'):
        '''
        擷取單一期間之html網頁內容
        '''
        u=self.getUrl(period)
        content=Pcs.getWebContent(self,u,Decode)
        
        self.printBasic(period)
        content_processed=self.processWebContent(content,period)
        
        return content_processed
    
    def getWebContentFlexibleProxy(self,period,Decode='utf-8'):
        '''
        彈性透過proxy擷取單一期間之html網頁內容
        '''
        u=self.getUrl(period)
        content=Pcs.getWebContentFlexibleProxy(self,u,Decode)
        self.printBasic(period)
        content_processed=self.processWebContent(content,period)
        
        return content_processed
    
    def getDF(self,content,period): pass
    def arrangeDF(self,df,period): pass
    def isValid(self,df_raw):
        if df_raw is None or len(df_raw)<3:
            return False
        else:
            return True

    def getDataFrame(self,content,period):
        '''
        產生單一期間之DataFrame內容
        '''
        dfNone=pd.DataFrame(None,index=[period],columns=['value'])
        
        # 根據單一區間和網頁內容產生dataframe物件
        df_raw=self.getDF(content,period)
        if not self.isValid(df_raw): return dfNone
        
        df_processed=self.arrangeDF(df_raw,period)
        if df_processed is None: return dfNone
        
        logger.debug(df_processed)
        return df_processed
    
    def prepareDF_Periods(self,p_start,p_end,freq_selected,decode_selected):
        content_init=self.getWebContentFlexibleProxy(p_start,Decode=decode_selected)
        df_init=self.getDataFrame(content_init,p_start)
        period_range=pd.period_range(p_start+1,p_end,freq=freq_selected)
        
        if df_init is None:
            df_init=pd.DataFrame(None,index=[p_start],columns=['value'])
        
        return [df_init,period_range]
    
    def arrangeDF_Periods(self,df_periods): return df_periods
    def getDataFramePeriods(self,p_start,p_end,freq_selected,Decode='utf-8'):
        '''
        產生固定區間之財務資料,資料形態為pandas的DataFrame
        '''
        logger.info("擷取%s資料中..." % self)
        
        df_accum,period_range=self.prepareDF_Periods(p_start,p_end,freq_selected,Decode)
        for pi in period_range:
            # 視情況彈性啟動proxy, 以防止網頁阻擋robot行為
            content=self.getWebContentFlexibleProxy(pi,Decode)
            
            # 固定選擇是否啟動proxy(建議全程啓動,若啓動,擷取網頁速度會較慢)
            #content=self.getWebContent(pi,Decode)
            
            df_loop=self.getDataFrame(content,pi)
            df_accum=df_accum.append(df_loop)
        
        df_accum=df_accum.convert_objects(convert_numeric=True)
        df_final=self.arrangeDF_Periods(df_accum)
        
        logger.info("%s資料擷取完成!" % self)
        return df_final
    
class PcsInc(PcsDataFrame):
    '''
    ---產生eps歷史資料之處理器---
    說明
        1. 根據個股代碼擷取公開資訊觀測站之損益表資訊,並將之轉化為EPS歷史資料
        2. 擷取公開資訊觀測站匯總損益表資訊,轉化為所有股票之EPS歷史資料 [TBD]
    '''
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def __str__(self): return "合併損益表"
    
    def getUrl(self,period): # freq='Q-DEC'
        # 根據時間點決定是否需考量IFRS
        if  period.year<2013:
            # 使用非合併損益表之EPS(和合併損益表之EPS相同,但歷史資料較齊全)
            u=url.get(Fan.INC,period,self.id,beforeIFRS=True,isCombined=False,mktType=self.market)
        else:
            u=url.get(Fan.INC,period,self.id,mktType=self.market)
        return u
    
    def processWebContent(self,content,period):
        # 2013年前的網站資料需要手動處理Html標籤,以利後續分析
        if period.year<2013: content=content.replace('</TBODY></BODY></HTML>','')
        return content
    
    def getDF(self,content,period):
        logger.debug(" get eps: using pandas.io.html.read_html to extract contents into DataFrame...")
        try:
            if  period.year<2013:
                return pd.io.html.read_html(content,infer_types=False)[4][[0,1]]
            else:
                # IFRS新制第2,3季有單季EPS資料
                if period.quarter in [2,3]:
                    df_temp=pd.io.html.read_html(content,infer_types=False)[1][[0,5]]
                    df_temp.columns=[[0,1]]
                    return df_temp
                else:
                    return pd.io.html.read_html(content,infer_types=False)[1][[0,1]]
        except Exception as ex:
            # 若資料庫還沒更新或無此資料,會回傳查無所需資料,意即網頁內無表格,read_html會產生error,此時便回傳None
            return None
    
    def arrangeDF(self,df,period):
        logger.debug(" get eps: rearrange the dataframe data...")
        # 擷取從表頭往後數第一個字尾四個字為每股盈餘之資料列
        df=df[df[0].str.contains(u'每股盈餘')]
        for ix in df.index:
            if df.at[ix,1]!='nan':
                df_eps=df[df.index==ix][[1]].T
                break
            else:
                df_eps=pd.DataFrame(None,index=[period],columns =['value'])
                    
        df_eps.columns=['value']
        df_eps.index=[period]
        return df_eps
    
    def prepareDF_Periods(self,p_start,p_end,freq_selected,decode_selected):
        content_init=self.getWebContentFlexibleProxy(p_start-1,Decode=decode_selected)
        
        df_init=self.getDataFrame(content_init,p_start-1)
        period_range=pd.period_range(p_start,p_end,freq=freq_selected)
        
        if df_init is None:
            df_init=pd.DataFrame(None,index=[p_start-1],columns=['value'])
        
        return [df_init,period_range]
    
    def arrangeDF_Periods(self,df_periods):
        # 2013年前的eps資料為累進式, 須將改為單季eps
        d2=df_periods.copy()
        d2.index=df_periods.index+1
        #d2[(d2.index.quarter==1) | (d2.index.year>2012)]=0.0
        d2[(d2.index.quarter==1)]=0.0
        d1=df_periods[1:]
        d2=d2[:-1]
        df_final=d1-d2
        return df_final


class PcsBal(PcsDataFrame):
    '''
    ---產生bvps歷史資料之處理器---
    說明
    1. 根據個股代碼擷取公開資訊觀測站之資產負債表資訊,並將之轉化為BVPS歷史資料
    2. 擷取公開資訊觀測站匯總損益表資訊,轉化為所有股票之BVPS歷史資料 [TBD]
    '''
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def __str__(self): return "合併資產負債表"
    
    def getUrl(self,period): # freq='Q-DEC'
        # 根據時間點決定是否需考量IFRS
        if  period.year<2013:
            # 使用合併資產負債表之股東權益和股本資料, 計算合併BVPS, 從2007/4開始有資料
            u=url.get(Fan.BAL,period,self.id,beforeIFRS=True,isCombined=True,mktType=self.market)
        else:
            u=url.get(Fan.BAL,period,self.id,mktType=self.market)
        return u
    
    def processWebContent(self,content,period):
        # 2013年前的網站資料需要手動處理Html標籤,以利後續分析
        if period.year<2013: content=content.replace('</TBODY></BODY></HTML>','')
        return content
    
    def getDF(self,content,period):
        logger.debug(" get bvps: using pandas.io.html.read_html to extract contents into DataFrame...")
        try:
            return pd.io.html.read_html(content,infer_types=False)[-1][[0,1]]
        except Exception as ex:
            # 若資料庫還沒更新或無此資料,會回傳查無所需資料,意即網頁內無表格,read_html會產生error,此時便回傳None
            return None
            
    def arrangeDF(self,df,period):
        logger.debug(" get bvps: rearrange the dataframe data...")
        if period.year<2013:
            a=df[df[0]==u'股東權益總計'][[1]]
        else:
            a=df[df[0]==u'權益總額'][[1]]
        b=df[df[0]==u'普通股股本'][[1]]
        
        a.index=b.index=[period]
        a=a.convert_objects(convert_numeric=True)
        b=b.convert_objects(convert_numeric=True)
        
        df_bvps=a/b*10
        df_bvps.columns=['value']
        
        return df_bvps

class PcsPrcCurrent(PcsDataFrame):
    '''
    ---產生現價之處理器---
    '''
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def __str__(self): return "股票現價"
    
    def getUrl(self,period): # freq='M'
        u=url.get(Fan.PRC,period,self.id,mktType=self.market)
        return u
    
    def getDF(self,content,period):
        try:
            if self.market==Mkt.SII:
                logger.debug(" get price: using pandas.io.html.read_html to extract contents into DataFrame...")
                return pd.io.html.read_html(content,infer_types=False)[-2][2:]
            elif self.market==Mkt.OTC:
                logger.debug(" get price: using pandas.io.json.read_json to extract contents into DataFrame...")
                return pd.io.json.read_json(content)[['aaData']]
        except Exception as ex:
            # 若資料庫還沒更新或無此資料,會回傳查無所需資料,意即網頁內無表格,read_html會產生error,此時便回傳None
            return None

    def getNewestPrice(self):
        
        f=lambda s: s.replace(s[:-6],str(int(s[:-6])+1911))
        period_current=pd.Period("%s/%s" % (ThisYear,ThisMonth))
        
        
        try:
            content=self.getWebContent(period_current,'big5')
        except urllib2.HTTPError, err:
            if err.code==404:
                logger.debug("Error found : %s" % (err))
                content=self.getWebContent(period_current-1,'big5')
            else:
                logger.debug("Error in PcsPrc : %s" % (err))

        df=self.getDF(content,period_current)
        if len(df)==0:
            content=self.getWebContent(period_current-1,'big5')
            df=self.getDF(content,period_current-1)
        
        if self.market==Mkt.SII:
            day_period=pd.Period(f(df.iat[-2,0]))
            price_current=float(df.iat[-2,1])
        elif self.market==Mkt.OTC:
            data_last=df.aaData.iat[-1,0]
            day_period=pd.Period(f(data_last[0]))
            price_current=float(data_last[6])
        price_newest=pd.DataFrame([price_current],index=[day_period],columns=['value'])
        return price_newest

class PcsPrc(PcsDataFrame):
    '''
    ---產生價格歷史資料之處理器---
    '''
    def __init__(self,id=None,isPriceDaily=True):
        Pcs.__init__(self,id)
        self.isPriceDaily=isPriceDaily
    
    def __str__(self): return "股票歷史價格"
    
    def getUrl(self,period): # freq='M'
        u=url.get(Fan.PRC,period,self.id,mktType=self.market)
        return u
    
    def getDF(self,content,period):
        try:
            if self.market==Mkt.SII:
                logger.debug(" get price: using pandas.io.html.read_html to extract contents into DataFrame...")
                return pd.io.html.read_html(content,infer_types=False)[-2][2:]
            elif self.market==Mkt.OTC:
                logger.debug(" get price: using pandas.io.json.read_json to extract contents into DataFrame...")
                return pd.io.json.read_json(content)[['aaData']]
        except Exception as ex:
            # 若資料庫還沒更新或無此資料,會回傳查無所需資料,意即網頁內無表格,read_html會產生error,此時便回傳None
            return None
    
    def arrangeDF(self,df,period):
        logger.debug(" get price: rearrange the dataframe data...")
        
        f=lambda s: s.replace(s[:-6],str(int(s[:-6])+1911))
        if self.market==Mkt.SII:
            # -- web return content is table
            if self.isPriceDaily:
                df=df[:-1]
                df.index=[pd.Period(f(s)) for s in df[0]]
                df_arr=df[[1]]
            else:
                # monthly averaged prices
                df_arr=df[-1:][[1]]
                df_arr.index=[period]
            df_arr=df_arr.convert_objects(convert_numeric=True)
            df_arr.columns=['value']
        elif self.market==Mkt.OTC:
            # -- web return content is json
            if self.isPriceDaily:
                pix=[pd.Period(f(i[0])) for i in df.aaData]
                price=[i[6] for i in df.aaData]
                df_arr=pd.DataFrame(price,index=pix,columns=['value'])
            else:
                # monthly averaged prices
                prices=np.array([i[6] for i in df.aaData])
                averaged_price=[prices.astype(float).mean()]
                df_arr=pd.DataFrame(averaged_price,index=[period],columns=['value'])
        return df_arr

class PcsPrcMonth(PcsDataFrame):
    '''
    ---產生個股月成交價格資料之處理器---
    '''
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def __str__(self): return "股票月成交價格"
    
    def getUrl(self,period): # freq='A-DEC'
        u=url.get(Fan.PRCm,period,self.id,mktType=self.market)
        return u
    
    def getDF(self,content,period):
        try:
            if self.market==Mkt.SII:
                logger.debug(" get price: using pandas.io.html.read_html to extract contents into DataFrame...")
                return pd.io.html.read_html(content,infer_types=False)[-2][2:]
            elif self.market==Mkt.OTC:
                logger.debug(" get price: using pandas.io.json.read_json to extract contents into DataFrame...")
                return pd.io.json.read_json(content)[['aaData']]
        except Exception as ex:
            # 若資料庫還沒更新或無此資料,會回傳查無所需資料,意即網頁內無表格,read_html會產生error,此時便回傳None
            return None
    
    def arrangeDF(self,df,period):
        logger.debug(" get monthly price: rearrange the dataframe data...")
        
        f=lambda s: s.replace(s[:-6],str(int(s[:-6])+1911))
        if self.market==Mkt.SII:
            # -- web return content is table
            periods=[]
            for x in df.index:
                row=df[df.index==x]
                y=row.iloc[0,0]
                m=row.iloc[0,1]
                periods.append(pd.Period("%s/%s" % (int(y)+1911,m)))
            df.index=periods
            df_arr=df[[4]]
            df_arr=df_arr.convert_objects(convert_numeric=True)
            df_arr.columns=['value']
        elif self.market==Mkt.OTC:
            # -- web return content is json
            prices=np.array([i[6] for i in df.aaData])
            averaged_price=[prices.astype(float).mean()]
            df_arr=pd.DataFrame(averaged_price,index=[period],columns=['value'])
        return df_arr
    
    def getDataFrame(self,content,period):
        
        dfNone=pd.DataFrame(None,index=[period],columns=['value'])
        
        pr=lambda m:pd.Period('%d/%d' % (period.year,m))
        periods_month=pd.period_range(pr(1),pr(12),freq='M')
        
        dfNone=pd.DataFrame(None,index=periods_month,columns=['value'])
        
        # 根據單一區間和網頁內容產生dataframe物件
        df_raw=self.getDF(content,period)
        if df_raw is None or len(df_raw)==0: return dfNone
        
        df_processed=self.arrangeDF(df_raw,period)
        if df_processed is None: return dfNone
        
        logger.debug(df_processed)
        return df_processed
    
    def prepareDF_Periods(self,p_start,p_end,freq_selected,decode_selected):
        
        pY_start=pd.Period(p_start.year)
        pY_end=pd.Period(p_end.year)

        if p_start.year==p_end.year:
            period_range=[]
        else:
            period_range=pd.period_range(pY_start+1,pY_end,freq='A-DEC')

        content_init=self.getWebContentFlexibleProxy(pY_start,Decode=decode_selected)
        df_init=self.getDataFrame(content_init,pY_start)
    
        return [df_init,period_range]

    def getDataFramePeriods(self,p_start,p_end,freq_selected,Decode='utf-8'):
        df_final=PcsDataFrame.getDataFramePeriods(self,p_start,p_end,freq_selected,Decode)
        return df_final[(df_final.index>=p_start) & (df_final.index<=p_end)]

class PcsPer(PcsDataFrame):
    '''
    ---產生本益比歷史資料之處理器---
    '''
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def __str__(self): return "股票本益比"
    
    def getUrl(self,period): # freq='M'
        u=url.get(Fan.PER,period,self.id,mktType=self.market)
        return u

    def getDF(self,content,period):
        try:
            if self.market==Mkt.SII:
                logger.debug(" get PER: using pandas.io.html.read_html to extract contents into DataFrame...")
                return pd.io.html.read_html(content,infer_types=False)[-2][2:]
            elif self.market==Mkt.OTC:
                logger.debug(" get PER: using pandas.io.json.read_json to extract contents into DataFrame...")
                return pd.io.json.read_json(content)[['aaData']]
        except Exception as ex:
            # 若資料庫還沒更新或無此資料,會回傳查無所需資料,意即網頁內無表格,read_html會產生error,此時便回傳None
            return None
    
    def arrangeDF(self,df,period):
        logger.debug(" get PER: rearrange the dataframe data...")
        # monthly averaged per
        f=lambda s: s.replace(s[:-6],str(int(s[:-6])+1911))
        
        try:
            if self.market==Mkt.SII:
                # -- web return content is table
                averaged_per=[df[1].convert_objects(convert_numeric=True).mean()]    
            elif self.market==Mkt.OTC:
                # -- web return content is json
                averaged_per=np.array([i[1] for i in df.aaData]).astype(float).mean()
            df_arr=pd.DataFrame(averaged_per,index=[period],columns=['value'])
        except Exception, err:
            df_arr=pd.DataFrame(None,index=[period],columns=['value'])
                
        return df_arr

class PcsPerFast(PcsDataFrame):
    '''
    ---產生本益比歷史資料之處理器---
    '''
    def __init__(self,pcsINC,pcsPRCMonth,id=None):
        Pcs.__init__(self,id)
        self.setRelation(pcsINC,pcsPRCMonth)
    
    def __str__(self): return "個股月均本益比"
    
    def setRelation(self,pcsINC,pcsPRCMonth):
        self.pcsINC=pcsINC
        self.pcsPRCMonth=pcsPRCMonth
    
    def getDataFramePeriods(self,p_start,p_end,freq_selected,Decode='utf-8'):
        '''
        產生固定區間之財務資料,資料形態為pandas的DataFrame
        '''
        logger.info("擷取%s資料中..." % self)
        
        pQ_start=pd.Period('%dQ%d' % (p_start.year,p_start.quarter))-4
        pQ_end=pd.Period('%dQ%d' % (p_end.year,p_end.quarter))-1
        
        dfInc=self.pcsINC.getDataFramePeriods(pQ_start,pQ_end,'Q')
        dfPrcMonth=self.pcsPRCMonth.getDataFramePeriods(p_start,p_end,'M','big5')
        
        dfInc_for_calc=pd.DataFrame()
        for ix in dfPrcMonth.index:
            q=pd.Period("%dQ%d" % (ix.year,ix.quarter))
            epsTTM=dfInc[(dfInc.index<q)&(dfInc.index>=q-4)]['value'].sum()
            dfInc_for_calc=dfInc_for_calc.append(pd.DataFrame([epsTTM],index=[ix],columns=['value']))

        df_per=dfPrcMonth/dfInc_for_calc
    
        df_per[(df_per.value<0)|(df_per.value==np.inf)]=np.nan
        
        logger.info("%s資料擷取完成!" % self)
        return df_per

class PcsSal(PcsDataFrame):
    '''
    ---產生sales歷史資料之處理器---
    '''
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def __str__(self): return "合併營收"
    
    def getUrl(self,period): # freq='M'
        # 根據時間點決定是否需考量IFRS
        if  period.year<2013:
            # 使用合併營收表之合併營收
            u=url.get(Fan.SAL,period,self.id,beforeIFRS=True,isCombined=True,mktType=self.market)
        else:
            u=url.get(Fan.SAL,period,self.id,mktType=self.market)
        return u
    
    def getDF(self,content,period):
        logger.debug(" get sale: using pandas.io.html.read_html to extract contents into DataFrame...")
        try:
            if period.year<2013:
                #return pd.io.html.read_html(content,infer_types=False)[2]
                return pd.io.html.read_html(content,infer_types=False)[-3]
            else:
                return pd.io.html.read_html(content,infer_types=False)[2]
        except Exception as ex:
            # 若資料庫還沒更新或無此資料,會回傳查無所需資料,意即網頁內無表格,read_html會產生error,此時便回傳None
            return None

    def arrangeDF(self,df,period):
        logger.debug(" get sale: rearrange the dataframe data...")
        if period.year<2013:
            
            # 2013年前僅有少數有合併營收資料
            if len(df.columns)==2:
                df_sales=df[df[0]==u'本月'][[1]]
            else:
                df_sales=pd.DataFrame([None])
        else:
            df_sales=df[df[0]==u'本月'][[1]]
        
        df_sales.index=[period]
        df_sales=df_sales.convert_objects(convert_numeric=True)
        df_sales.columns=['value']
        
        return df_sales

class PcsCsh(PcsDataFrame):
    '''
        ---產生cash歷史資料之處理器---
    '''
    def __init__(self,id=None):
        Pcs.__init__(self,id)
    
    def __str__(self): return "合併現金流量表"
    
    def getUrl(self,period): # freq='Q-DEC'
        # 根據時間點決定是否需考量IFRS
        if  period.year<2013:
            u=url.get(Fan.CSH,period,self.id,beforeIFRS=True,isCombined=True,mktType=self.market)
        else:
            u=url.get(Fan.CSH,period,self.id,mktType=self.market)
        return u
    
    def extractText_beforeIFRS(self,content):
        
        strCashOper=(u"營[\s　]*.[\s　]*活[\s　]*動[\s　]*[產生之淨現縣金流出入數量]+",)
        strCashInv=(u"投[\s　]*資[\s　]*活[\s　]*動[\s　]*[產生之淨現縣金流出入數量]+",)
        strCashFinan=(u"融[\s　]*資[\s　]*活[\s　]*動[\s　]*[產生之淨現縣金流出入數量]+", \
                      u".[\s　]*財[\s　]*活[\s　]*動[\s　]*[產生之淨現縣金流出入數量]+")
        strCash=(strCashOper,strCashInv,strCashFinan)
        
        cash=[0,0,0]  # 累加現金流量 cash[0]:營運現金 cash[1]:投資現金
        
        for k in range(3):
            n=0
            while n<len(strCash[k]):
                
                mp1=u"[-\(（\[\s　$]*[\d,]+[\s　]*[\)）\]]*" # money pattern
                
                ptn1=strCash[k][n]+u"[（） ﹝﹞\s\(\)產生之淨現縣金流出入數量]+[:\s　|│]*"+ mp1+u"[\s　|│]*"+mp1
                ptn2=strCash[k][n]+u"[（） ﹝﹞\s\(\)產生之淨現縣金流出入數量]+[-]+[-]+[\s　]*"+ mp1
                
                pattern=(ptn1,ptn2)
                m,mMatch=(0,-1)
                while m<len(pattern):
                    
                    regex=compile(pattern[m])
                    match=regex.findall(content)
                    if len(match)>0:
                        mMatch=m
                        pos1=content.find(match[0])
                        dataLineLength=len(match[0])
                        break
                    else:
                        m+=1
                
                if len(match)>0:
                    break
                else:
                    n+=1
            if n==len(strCash[k]): continue
            
            strTemp=content[pos1:pos1+dataLineLength]
            
            try:
                regex=compile(mp1)
                cash[k]=regex.findall(strTemp)[0]
                
                if cash[k].find('(')!=-1 or cash[k].find(u'（')!=-1 or cash[k].find(')')!=-1 or cash[k].find(u'）')!=-1:
                    cash[k]=cash[k].replace('-','')
                    cash[k]=cash[k].replace(']','').replace('[','')
                    cash[k]=cash[k].replace(u'）','').replace(u'（','')
                    cash[k]="-" + cash[k].replace(')','').replace('(','')
                cash[k]=cash[k].replace('$','')
                cash[k]=int(cash[k].replace(",",""))
            
            except Exception, err:
                if cash[k]=="-":
                    cash[k]=0
                else:
                    break
        try:
            dollor=compile(u'單位[：:\s　\u4e00-\u9fff]*[元]').findall(content)
            if len(dollor)!=0:
                dollor=compile(u'[仟千]').findall(content)
                if len(dollor)==0:
                    cash=[int(round(c/1000.)) for c in cash]
        except:
            pass
        
        return cash
    
    def getDF(self,content,period):
        logger.debug(" get cash: using pandas.io.html.read_html to extract contents into DataFrame...")
        try:
            if  period.year>=2013:
                return pd.io.html.read_html(content,infer_types=False)[1]
        except Exception as ex:
            # 若資料庫還沒更新或無此資料,會回傳查無所需資料,意即網頁內無表格,read_html會產生error,此時便回傳None
            logger.debug("Error:%s" % (ex))
            return None
    
    def arrangeDF(self,df,period):
        logger.debug(" get cash: rearrange the dataframe data...")
        
        if df is None or len(df)==0: return None
        
        cashOper=df[df[0]==u'營運產生之現金流入（流出）'].iat[0,1]
        cashInv=df[df[0]==u'投資活動之淨現金流入（流出）'].iat[0,1]
        cashFinan=df[df[0]==u'籌資活動之淨現金流入（流出）'].iat[0,1]
        df_cash=pd.DataFrame([[cashOper,cashInv,cashFinan]],index=[period],columns=['operation','invest','financial'])
        
        return df_cash

    def getDataFrame(self,content,period):
        logger.debug(" get cash: process plain text into dataframe")
        
        if  period.year<2013:
            strFinds=['資料庫中查無需求資料','無應編製合併財報之子公司']
            for ss in strFinds:
                if ss in content.encode('utf-8'):
                    content_found=False
                    break
                else:
                    content_found=True
        
            if not content_found:
                df_cash=pd.DataFrame([[None]*3],index=[period],columns=['operation','invest','financial'])
            else:
                cash_lists=self.extractText_beforeIFRS(content)
                df_cash=pd.DataFrame([cash_lists],index=[period],columns=['operation','invest','financial'])
        else:
            u=url.get(Fan.CSH,period,self.id,mktType=self.market)
            df_raw=self.getDF(content,period)
            df_cash=self.arrangeDF(df_raw,period)
        
        if df_cash is None:
            df_cash=pd.DataFrame([[None]*3],index=[period],columns=['operation','invest','financial'])

        df_cash=df_cash.convert_objects(convert_numeric=True)
        df_freeCash=pd.DataFrame(df_cash['operation']+df_cash['invest'],columns=['value'])

        return df_freeCash

    def prepareDF_Periods(self,p_start,p_end,freq_selected,decode_selected):
        content_init=self.getWebContentFlexibleProxy(p_start-1,Decode=decode_selected)
        
        df_init=self.getDataFrame(content_init,p_start-1)
        period_range=pd.period_range(p_start,p_end,freq=freq_selected)
        
        if df_init is None:
            df_init=pd.DataFrame(None,index=[p_start-1],columns=['value'])
        
        return [df_init,period_range]
    
    def arrangeDF_Periods(self,df_periods):
        # 2013年前的現金流量資料為累進式, 須將改為單季現金流量
        d2=df_periods.copy()
        d2.index=df_periods.index+1
        d2[(d2.index.quarter==1) | (d2.index.year>2012)]=0.0
        d1=df_periods[1:]
        d2=d2[:-1]
        df_final=d1-d2
        return df_final

def test_failed():
    id_failed=[2002]
    test(PcsInc,id_failed,3)


    url.getBasic('厚生')    # failed
    url.getBasic(2002)      # failed

if __name__=='__main__':
    pass
    #tested=test(2317,5)
    #timeit.timeit(tested)

