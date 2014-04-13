# -*- coding: utf8 -*-
from base import Now,ThisYear,ThisQuarter,ThisMonth
from base import stkBase

import pdb

class utils():
    @classmethod
    def E2M(cls,year): #西元->民國
        return year-1911
    
    @classmethod
    def M2E(cls,year): #民國->西元
        return year+1911

# collect the url of all stock data from web
class url():
    @classmethod
    def cond_to_flags(cls,stkid,beforeIFRS,isCombined):
        S,B,C=[stkBase.F.SINGLE,stkBase.F.NO_IFRS,stkBase.F.COMBINED]
        if stkid!=None:
            if beforeIFRS:
                if isCombined:
                    flags=S|B|C
                else:   
                    flags=S|B
            elif isCombined:
                flags=S|C
        else:
            if beforeIFRS:
                if isCombined:
                    flags=B|C
                else:
                    flags=B
            elif isCombined:
                flags=C
        return flags
    
    @classmethod
    def get(cls,financial,period,stkid=None,beforeIFRS=False,isCombined=True,mktType=None): # stkid=None --> 財務彙整總表
        
        # <-- speed bottleneck!!
        if mktType==None:
            m=stkBase.getMkt(stkid)
        else:
            m=mktType
        
        p,s,b,c=(period,stkid,beforeIFRS,isCombined)
        funcmap={   stkBase.Fan.DIV : cls.getUrlDIVIDEND,
                    stkBase.Fan.PRC : cls.getUrlPRICE,
                    stkBase.Fan.PRCm : cls.getUrlPRICE_Month,
                    stkBase.Fan.SAL : cls.getUrlSALE,
                    stkBase.Fan.INC : cls.getUrlINCOME,
                    stkBase.Fan.BAL : cls.getUrlBALANCE,
                    stkBase.Fan.PER : cls.getUrlPER,
                    stkBase.Fan.CSH : cls.getUrlCASH
                }
        return funcmap[financial](m,p,s,b,c)
    
    @classmethod
    def getUrlTWSE(cls,type,db,**kwargs):
        args=''
        if type==1:
            #   公開資訊觀測站資料庫1
            #   財務資料        回傳類型
            #   ---------------------------
            #   EPS            <table></table>
            #   每股淨值        <table></table>
            #   每月營收        <table></table>
            #   現金流量        text
            base="http://mops.twse.com.tw/mops/web/"
        elif type==2:
            #   公開資訊觀測站資料庫2
            #   財務資料        回傳類型
            #   ---------------------------
            #   總股股利        <table></table>
            base="http://mops.twse.com.tw/server-java/"
        elif type==3:
            #   證券交易所
            #   財務資料        回傳類型
            #   ---------------------------
            #   上市個股日成交價 <table></table>
            #   及月平均價
            base="http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY_AVG/genpage/"
        elif type==4:
            #   證券交易所
            #   財務資料        回傳類型
            #   ---------------------------
            #   上市個股日本益比 <table></table>
            base="http://www.twse.com.tw/ch/trading/exchange/BWIBBU/"#BWIBBU.php?myear=2014&mmon=3&STK_NO=2317&"
        elif type==5:
            #   櫃買中心
            #   財務資料        回傳類型
            #   ---------------------------
            #   上櫃個股        json
            #   日成交資訊
            base="http://www.otc.org.tw/ch/stock/aftertrading/daily_trading_info/"
        elif type==6:
            #   櫃買中心
            #   財務資料        回傳類型
            #   ---------------------------
            #   上櫃個股日本益比 json
            base="http://www.gretai.org.tw/ch/stock/aftertrading/peratio_stk/"#pera_result.php?stkno=2317&d=103/01
        elif type==7:
            #   證券交易所
            #   財務資料        回傳類型
            #   ---------------------------
            #   上市個股        <table></table>
            #   月成交資訊
            base="http://www.twse.com.tw/ch/trading/exchange/FMSRFK/genpage/"
            #http://www.twse.com.tw/ch/trading/exchange/FMSRFK/genpage/Report201304/2013_F3_1_10_2317.php?STK_NO=2317&myear=2013
        
        for key in kwargs: args+='%s=%s&' % (key,kwargs[key])
        return '%s%s?%s' % (base,db,args)
    
    @classmethod
    def getUrlPRICE(cls,market,period,stkid,beforeIFRS,isCombined):
        
        if period.freq!='M':return None
        y,m = [period.year, period.month]
        
        kwargs_sii={'STK_NO':stkid,'myear':y,'mmon':m}
        kwargs_otc={'stkno':stkid,'d':"%s/%.2d" % (utils.E2M(y),m)}
        
        date="%s%.2d" % (y,m)
        db="Report%s/%s_F3_1_8_%s.php" % (date,date,stkid)      #個股日收盤價及月平均價
        
        if stkid!=None:
            if market==stkBase.Mkt.SII:
                url=cls.getUrlTWSE(3,db,**kwargs_sii)
            elif market==stkBase.Mkt.OTC:
                url=cls.getUrlTWSE(5,'st43_result.php',**kwargs_otc)
        else:
            url=None
        
        return url

    @classmethod
    def getUrlPRICE_Month(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='A-DEC':return None
        y=period.year
        
        kwargs_sii={'STK_NO':stkid,'myear':y}
        
        #個股月成交資訊
        if y<ThisYear:
            db="Report%s01/%s_F3_1_10_%s.php" % (y,y,stkid)
        else:
            db="Report%s%0.2d/%s_F3_1_10_%s.php" % (y,ThisMonth,y,stkid)
        
        if stkid!=None:
            if market==stkBase.Mkt.SII:
                url=cls.getUrlTWSE(7,db,**kwargs_sii)
        else:
            # 櫃買中心無個股月成交資訊
            url=None
        
        return url

    @classmethod
    def getUrlPER(cls,market,period,stkid,beforeIFRS,isCombined):
        
        if period.freq!='M':return None
        y,m = [period.year, period.month]
        
        kwargs_sii={'STK_NO':stkid,'myear':y,'mmon':m}
        kwargs_otc={'stkno':stkid,'d':"%s/%s" % (utils.E2M(period.year),m)}
        
        if stkid!=None:
            if market==stkBase.Mkt.SII:
                url=cls.getUrlTWSE(4,'BWIBBU.php',**kwargs_sii)
            elif market==stkBase.Mkt.OTC:
                url=cls.getUrlTWSE(6,'pera_result.php',**kwargs_otc)
        else:
            url=None
        
        return url


    # year period data
    @classmethod
    def getUrlDIVIDEND(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='A-DEC':return None

        y=period.year
        kwargs_single={'step':1,'firstin':1,'co_id':stkid}
        kwargs_whole={'step':1,'TYPEK':stkBase.Mkt.toStr(market),'YEAR':utils.E2M(y)}
        
        if stkid!=None:
            # 個股歷年股利
            url=cls.getUrlTWSE(1,'ajax_t05st09',**kwargs_single)
        else:
            # 總股單年股利
            if market==stkBase.Mkt.SII or market==stkBase.Mkt.OTC:
                url=cls.getUrlTWSE(2,'t05st09sub',**kwargs_whole)
        return url
        
    @classmethod
    def getUrlINCOME(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='Q-DEC' or (market!=stkBase.Mkt.SII and market!=stkBase.Mkt.OTC and market!=stkBase.Mkt.ALL):return None
        y,q = [utils.E2M(period.year), period.quarter]
        strMkt=stkBase.Mkt.toStr(market)
 
        kwargs_single={'step':1,'firstin':1,'co_id':stkid,'year':y,'season':"%.2d"%(q)}
        kwargs_whole={'step':1,'firstin':1,'TYPEK':strMkt,'year':y,'season':"%.2d"%(q)}
        
       	S,B,C=[stkBase.F.SINGLE,stkBase.F.NO_IFRS,stkBase.F.COMBINED]
        flags=cls.cond_to_flags(stkid,beforeIFRS,isCombined)
        flagValue={ S|B|C   : cls.getUrlTWSE(1,'ajax_t05st34',**kwargs_single),
                    S|B     : cls.getUrlTWSE(1,'ajax_t05st32',**kwargs_single),
                    S|C     : cls.getUrlTWSE(1,'ajax_t164sb04',**kwargs_single),
                    B|C     : cls.getUrlTWSE(1,'ajax_t51sb13',**kwargs_whole),
                    B       : cls.getUrlTWSE(1,'ajax_t51sb08',**kwargs_whole),
                    C       : cls.getUrlTWSE(1,'ajax_t163sb04',**kwargs_whole)}
       	return flagValue[flags]
    
    @classmethod
    def getUrlBALANCE(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='Q-DEC' or (market!=stkBase.Mkt.SII and market!=stkBase.Mkt.OTC):return None
        y,q = [utils.E2M(period.year), period.quarter]
        strMkt=stkBase.Mkt.toStr(market)
        
        kwargs_single={'step':1,'firstin':1,'co_id':stkid,'year':y,'season':"%.2d"%(q)}
        kwargs_whole={'step':1,'firstin':1,'TYPEK':strMkt,'year':y,'season':"%.2d"%(q)}
        
        S,B,C=[stkBase.F.SINGLE,stkBase.F.NO_IFRS,stkBase.F.COMBINED]
        flags=cls.cond_to_flags(stkid,beforeIFRS,isCombined)
        flagValue={ S|B|C   : cls.getUrlTWSE(1,'ajax_t05st33',**kwargs_single),
                    S|B     : cls.getUrlTWSE(1,'ajax_t05st31',**kwargs_single),
                    S|C     : cls.getUrlTWSE(1,'ajax_t164sb03',**kwargs_single),
                    B|C     : cls.getUrlTWSE(1,'ajax_t51sb07',**kwargs_whole),
                    B       : cls.getUrlTWSE(1,'ajax_t51sb12',**kwargs_whole),
                    C       : cls.getUrlTWSE(1,'ajax_t163sb05',**kwargs_whole)}
        
        return flagValue[flags]

    @classmethod
    def getUrlSALE(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='M':return None
        y,m = [utils.E2M(period.year), period.month]
        kwargs_single={'step':0,'firstin':1,'co_id':stkid,'year':y,'month':m}
        
        S,B,C=[stkBase.F.SINGLE,stkBase.F.NO_IFRS,stkBase.F.COMBINED]
        flags=cls.cond_to_flags(stkid,beforeIFRS,isCombined)
        flagValue={ S|B|C   : cls.getUrlTWSE(1,'ajax_t05st10',**kwargs_single),
                    S|B     : None,
                    S|C     : cls.getUrlTWSE(1,'ajax_t05st10_ifrs',**kwargs_single),
                    B|C     : None,
                    B       : None,
                    C       : None}

        return flagValue[flags]

    @classmethod
    def getUrlCASH(cls,market,period,stkid,beforeIFRS,isCombined):
        if period.freq!='Q-DEC' or (market!=stkBase.Mkt.SII and market!=stkBase.Mkt.OTC and market!=stkBase.Mkt.ALL):return None
        y,q = [utils.E2M(period.year), period.quarter]
        strMkt=stkBase.Mkt.toStr(market)
        
        kwargs_single={'step':1,'firstin':1,'co_id':stkid,'year':y,'season':"%.2d"%(q)}
        kwargs_whole={'step':1,'firstin':1,'TYPEK':strMkt,'year':y,'season':"%.2d"%(q)}
        
       	S,B,C=[stkBase.F.SINGLE,stkBase.F.NO_IFRS,stkBase.F.COMBINED]
        flags=cls.cond_to_flags(stkid,beforeIFRS,isCombined)
        flagValue={ S|B|C   : cls.getUrlTWSE(1,'ajax_t05st39',**kwargs_single),
                    S|B     : cls.getUrlTWSE(1,'ajax_t05st36',**kwargs_single),
                    S|C     : cls.getUrlTWSE(1,'ajax_t164sb05',**kwargs_single),
                    B|C     : None,
                    B       : None,
                    C       : None}
       	return flagValue[flags]

if __name__=='__main__':
    print 'start'

    co_id=2317
  









