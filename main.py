# -*- coding: utf8 -*-

import urllib2
import os
import sys
import urllib2

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

