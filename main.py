# -*- coding: utf8 -*-

import urllib2
import string

#edit at fedoraUSB

# collect the url of all stock data from web
class urlMaker():
    '''
    examples
    
    A.Income
    -Before IFRS
     (A.1)total compony 個別損益表 [till 2012/Q4]
        sii : http://mops.twse.com.tw/mops/web/ajax_t51sb08?step=1&firstin=1&TYPEK=sii&year=101&season=03
        otc : http://mops.twse.com.tw/mops/web/ajax_t51sb08?step=1&firstin=1&TYPEK=otc&year=101&season=03
     (A.2)total compony 合併損益表
        sii : http://mops.twse.com.tw/mops/web/ajax_t51sb13?step=1&firstin=1&TYPEK=sii&year=101&season=03
        otc : http://mops.twse.com.tw/mops/web/ajax_t51sb13?step=1&firstin=1&TYPEK=otc&year=101&season=03
    -After IFRS
     (A.3)total compony 合併損益表 [starts from 2013/Q1]
        sii : http://mops.twse.com.tw/mops/web/ajax_t163sb04?step=1&firstin=1&TYPEK=sii&year=102&season=01
        otc : http://mops.twse.com.tw/mops/web/ajax_t163sb04?step=1&firstin=1&TYPEK=otc&year=102&season=01
    
    Price
    sii prices : http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY_AVG/STOCK_DAY_AVG.php?STK_NO=5608&myear=2003&mmon=09
    otc prices : http://www.otc.org.tw/ch/stock/aftertrading/daily_trading_info/st43_result.php?d=93/01&stkno=6231
    
    '''
    @classmethod
    def getUrlIncome(cls,type,y,q,id,tableType=3):
        # tableType=1 -> A.1
        # tableType=2 -> A.2
        # tableType=3 -> A.3
        if type!='sii' and type!='otc':return None
        if tableType==1:
            ajaxType='ajax_t51sb08'
        elif tableType==2:
            ajaxType='ajax_t51sb13'
        elif tableType==3:
            ajaxType='ajax_t163sb04'
        else:
            return None
        url='http://mops.twse.com.tw/mops/web/%s?step=1&firstin=1&TYPEK=%s&year=%s&season=%s' % (ajaxType,type,y-1911,string.zfill(q,2))
        return url
            
    @classmethod
    def getUrlPrice(cls,type,y,q,id):
        if type=='sii':
            url="http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY_AVG/STOCK_DAY_AVG.php?STK_NO=%s&myear=%s&mmon=%s" % (id,y,string.zfill(q,2))
        elif type=='otc':
            url="http://www.otc.org.tw/ch/stock/aftertrading/daily_trading_info/st43_result.php?d=%s/%s&stkno=%s" % (y-1911,string.zfill(q,2),id)
        return url
    

class stock():
    def __init__(self,id,name):
        self.id=id
        self.name=name


def load_stock_lists():
    print 'load stock lists'
    st1=stock(2317,u'鴻海')
    st2=stock(2330,u'台積電')
    return [st1,st2]

def process_price(stockLists):
    print 'process prices...'

def process_income(stockLists):
    print 'process incomes...'

def process_balance(stockLists):
    print 'process balances...'

def process_sale(stockLists):
    print 'process sales...'

def process_cash(stockLists):
    print 'process cashes...'

if __name__=='__main__':
    print 'start'
    stockLists=load_stock_lists()
    process_price(stockLists)
    process_income(stockLists)
    process_balance(stockLists)
    process_sale(stockLists)
    process_cash(stockLists)

