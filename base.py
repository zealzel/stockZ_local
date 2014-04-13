# -*- coding: utf-8 -*-
from urllib2 import urlopen
from re import findall
from pandas.io.html import read_html
from datetime import datetime

Now=datetime.now()
ThisYear=Now.year
ThisQuarter=(Now.month-1)/3+1
ThisMonth=Now.month

class stkBase():
    
    class Fan() : DIV, INC, BAL, SAL, CSH, PRC, PRCm, PER = range(1,9)
    class Mkt() :
        ALL, SII, OTC = range(3)
        @classmethod
        def toStr(cls,key):
            v=('all','sii','otc')
            return v[key]
    
    class F(): # Flags
        SINGLE=1 	# 個股
        NO_IFRS=2 	# IFRS前
        COMBINED=4 	# 合併報表
    
    @classmethod
    def getUrlStock(cls,stk_id):
        # both id & name of stock can be queryed
        url_stock='http://mops.twse.com.tw/mops/web/ajax_quickpgm?firstin=true&step=4&checkbtn=1&queryName=co_id&keyword4=%s' % (stk_id)
        return url_stock
    
    @classmethod
    def getBasic(cls,stk_id):
        # stk_id can be either stock id or stock name
        try:
            res=urlopen(cls.getUrlStock(stk_id))
        except Exception as ex:
            print 'error: %s' % (ex)
            return None
        content=res.read().decode('utf-8')
        strErase=findall("<tr class='tblHead'>.*\n.*</div>",content)[0]
        content=content.replace(strErase,'')
        df=read_html(content)[1]
        return df
    
    @classmethod
    def getMkt(cls,stk_id):
        df=cls.getBasic(stk_id)
        try:
            market_type=df.at[0,3]
            if market_type==u'上市':
                return cls.Mkt.SII
            elif market_type==u'上櫃':
                return cls.Mkt.OTC
            else:
                return cls.Mkt.ALL
        except Exception as ex:
            return None
    
    @classmethod
    def getStockID(cls,stk_id):
        df=cls.getBasic(stk_id)
        return df.at[0,0]
    
    @classmethod
    def getStockName(cls,stk_id):
        df=cls.getBasic(stk_id)
        return df.at[0,1]