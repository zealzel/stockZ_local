# -*- coding: utf-8 -*-
from process import *

if __name__=="__main__":

    id=raw_input("please enter stock id:")

    p=PcsDiv(id)

    '''
    tss,tsFit=p.getTS()
    
    mean=tsFit.mean().loc['value']
    std=tsFit.std().loc['value']
    print "\nstock id = %s\n股利計算基準 : %s年\n股利平均 : %s\n股利標準差 : %s\n股利價格 : %s\n" \
            % (id,10,mean,std,mean/0.15)
    print tss
    '''

    pcs=[PcsDiv,]
    for p in pcs:
        pp=p(id)
        print pp.getTS()

