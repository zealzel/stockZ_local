# -*- coding: utf8 -*-

import urllib2
import string
from pandas import Period

class utils():
    
    @classmethod
    def E2M(cls,year): #西元->民國
        return year-1911
    
    @classmethod
    def M2E(cls,year): #民國->西元
        return year+1911

# collect the url of all stock data from web
class url():

    class Fan() : DIV, INC, BAL, SAL, CSH, PRC = range(1,7)
    class Mkt() :
        ALL, SII, OTC = range(3)
        
        @classmethod
        def str(cls,key):
            v=('all','sii','otc')
            return v[key]
    
    class F(): # Flags
        ZERO=0		# 非個股&IFRS後&非合併報表
        SINGLE=1 	# 個股
        NO_IFRS=2 	# IFRS前
        COMBINED=4 	# 合併報表
    
    @classmethod
    def cond_to_flags(cls,stkid,beforeIFRS,isCombined):
        Z,S,B,C=[cls.F.ZERO,cls.F.SINGLE,cls.F.NO_IFRS,cls.F.COMBINED]
        if stkid!=None:
            if beforeIFRS:
                if isCombined:
                    flags=S|B|C
                else:
                    flags=S|B
            else:
                flags=S
        else:
            if beforeIFRS:
                if isCombined:
                    flags=B|C
                else:
                    flags=B
            else:
                flags=Z
        return flags
    
    @classmethod
    def get(cls,financial,market,period,stkid=None,beforeIFRS=False,isCombined=True): # stkid=None --> 財務彙整總表
        
        m,p,s,b,c=(market,period,stkid,beforeIFRS,isCombined)
        funcmap={   cls.Fan.DIV : cls.getUrlDIVIDEND(m,p,s),
                    cls.Fan.PRC : cls.getUrlPRICE(m,p,s),
                    cls.Fan.INC : cls.getUrlINCOME(m,p,s,b,c),
                    cls.Fan.BAL : cls.getUrlBALANCE(m,p,s,b,c),
                    cls.Fan.CSH : cls.getUrlCASH(m,p,s,b,c),
                     }
    
        return funcmap[financial]
    
    @classmethod
    def getUrlPRICE(cls,market,period,stkid):
        
        if period.freq!='M':return None
        y,m = [period.year, period.month]
        
        if stkid!=None:
            if market==cls.Mkt.SII:
                url="http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY_AVG/STOCK_DAY_AVG.php?STK_NO=%s&myear=%s&mmon=%s" % \
                    (stkid,y,string.zfill(m,2))
            elif market==cls.Mkt.OTC:
                url="http://www.otc.org.tw/ch/stock/aftertrading/daily_trading_info/st43_result.php?d=%s/%s&stkno=%s" %  \
                    (utils.E2M(period.year),string.zfill(m,2),stkid)
        else:
            url=None
        
        return url
    
    # year period data
    @classmethod
    def getUrlDIVIDEND(cls,market,period,stkid):
        if period.freq!='A-DEC':return None
        y=period.year
        if stkid!=None:
            # 個股歷年股利
            url="http://mops.twse.com.tw/mops/web/ajax_t05st09?step=1&firstin=1&co_id=%s" % (stkid,)
        else:
            # 總股單年股利
            if market==cls.Mkt.SII or market==cls.Mkt.OTC:
                url="http://mops.twse.com.tw/server-java/t05st09sub?step=1&TYPEK=%s&YEAR=%s" % (cls.Mkt.str(market),utils.E2M(y))
        return url
        
    @classmethod
    def getUrlINCOME(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='Q-DEC' or (market!=cls.Mkt.SII and market!=cls.Mkt.OTC and market!=cls.Mkt.ALL):return None
        y,q = [utils.E2M(period.year), period.quarter]
        strMkt=cls.Mkt.str(market)
        
        url_single=lambda s: "http://mops.twse.com.tw/mops/web/%s?step=1&firstin=1&co_id=%s&year=%s&season=%s" % (s,stkid,y,string.zfill(q,2))
        url_whole =lambda s: "http://mops.twse.com.tw/mops/web/%s?step=1&firstin=1&TYPEK=%s&year=%s&season=%s" % (s,strMkt,y,string.zfill(q,2))
    
       	Z,S,B,C=[cls.F.ZERO,cls.F.SINGLE,cls.F.NO_IFRS,cls.F.COMBINED]
        flags=cls.cond_to_flags(stkid,beforeIFRS,isCombined)
        flagValue={ S|B|C   : url_single('ajax_t05st34'),
                    S|B     : url_single('ajax_t05st32'),
                    S       : url_single('ajax_t164sb04'),
                    B|C     : url_whole('ajax_t51sb13'),
                    B       : url_whole('ajax_t51sb08'),
                    Z       : url_whole('ajax_t163sb04')}
        
       	return flagValue[flags]
    
    @classmethod
    def getUrlBALANCE(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='Q-DEC' or (market!=cls.Mkt.SII and market!=cls.Mkt.OTC):return None
        y,q = [utils.E2M(period.year), period.quarter]
        strMkt=cls.Mkt.str(market)
        
        url_single=lambda s: "http://mops.twse.com.tw/mops/web/%s?step=1&firstin=1&co_id=%s&year=%s&season=%s" % (s,stkid,y,string.zfill(q,2))
        url_whole =lambda s: "http://mops.twse.com.tw/mops/web/%s?step=1&firstin=1&TYPEK=%s&year=%s&season=%s" % (s,strMkt,y,string.zfill(q,2))
        
        S,B,C=[cls.F.SINGLE,cls.F.NO_IFRS,cls.F.COMBINED]
        flags=cls.cond_to_flags(stkid,beforeIFRS,isCombined)
        flagValue={ S|B|C   : url_single('ajax_t05st33'),
                    S|B     : url_single('ajax_t05st31'),
                    S       : url_single('ajax_t164sb03'),
                    B|C     : url_whole('ajax_t51sb07'),
                    B       : url_whole('ajax_t51sb12'),
                    0       : url_whole('ajax_t163sb05')}
        
        return flagValue[flags]

    @classmethod
    def getUrlCASH(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='Q-DEC' or (market!=cls.Mkt.SII and market!=cls.Mkt.OTC):return None
        y,q = [utils.E2M(period.year), period.quarter]
        strMkt=cls.Mkt.str(market)
        
        u=lambda s: "http://mops.twse.com.tw/mops/web/%s?step=1&firstin=1&co_id=%s&year=%s&season=%s" % (s,stkid,y,string.zfill(q,2))
        
        S,B,C=[cls.F.SINGLE,cls.F.NO_IFRS,cls.F.COMBINED]
        flags=cls.cond_to_flags(stkid,beforeIFRS,isCombined)
        flagValue={ S|B|C   : u('ajax_t05st39'),
                    S|B     : u('ajax_t05st36'),
                    S       : u('ajax_t164sb05'),
                    B|C     : None,
                    B       : None,
                    0       : None} 
        return flagValue[flags]

if __name__=='__main__':
    print 'start'

    co_id=2317

    # ----個股----
    # before IFRS
    
    # after IFRS










