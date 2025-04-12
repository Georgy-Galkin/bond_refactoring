﻿from main import ETL
from main_zip import extract
import os
import bondetl as be
import re

path = 'N:/NB/'
extracted_path = path+'extracted_data/'

print('extract')
extract() #Этот кусок разархивирует файлы, но не переименовывает!!!! Нужно убрать _arch или другие теги к файлу так как дальнейший скрипт обрабатывает чистые базы

#'''
print('SFF_BONFVDST')
headers = ['MKT_TAG','PROD_TAG','PER_TAG','F000000000000000000100000000000000000000','F000000000000000000200000000000000000000','F000000000000000000300000000000000000000',
    'F000000000000000006900000000000000000000', 'F000000000000000075500000000000000000000','F000000000000000075800000000000000000000',
    'F000000000000000006800000000000000000000']
base_name = 'Frozen'
nameslist = ['SFF_BONFVDST'] 
ETL(headers,base_name,nameslist,path,extracted_path,0,0) # 1 for skip split_CSV,second for skip bulk
#'''
'''
print('SFF_BONFVDPP')
headers = ['MKT_TAG','PROD_TAG','PER_TAG','SALES_A','VOL_A','VAL_A','SALES_C','VOL_C','VAL_C',
'CNT4_A','ASPS_A','PRICE2_A','PRICE1_A','AVOLPS_A','TDISCV','ND_D','WVOLD_D','SALES_E',
'VOL_E','VAL_E','ND_E','WVOLD_E','ND_A','WVOLD_A']
base_name = 'FRZP'
nameslist = ['SFF_BONFVDPP']
ETL(headers,base_name,nameslist,path,extracted_path,0,0) # 1 for skip split_CSV,second for skip bulk
'''
#'''
print('SFF_BONVEGST')
headers =  ['MKT_TAG','PROD_TAG','PER_TAG','F000000000000000000100000000000000000000','F000000000000000000200000000000000000000','F000000000000000000300000000000000000000',
'F000000000000000013400000000000000000000','F000000000000000013500000000000000000000','F000000000000000014500000000000000000000','F000000000000000014600000000000000000000',
    'F000000000000000006900000000000000000000', 'F000000000000000075500000000000000000000','F000000000000000075800000000000000000000',
    'F000000000000000006800000000000000000000']
base_name = 'ST'
nameslist = ['SFF_BONVEGST']
ETL(headers,base_name,nameslist,path,extracted_path,0,0) # 1 for skip split_CSV,second for skip bulk
#'''
'''
print('SFF_BONVEGPP')
headers = ['MKT_TAG','PROD_TAG','PER_TAG','SALES_A','VOL_A','VAL_A','SALES_C','VOL_C','VAL_C',
'CNT4_A','ASPS_A','PRICE2_A','PRICE1_A','AVOLPS_A','TDISCV','ND_D','WVOLD_D','SALES_E',
'VOL_E','VAL_E','ND_E','WVOLD_E','ND_A','WVOLD_A']
base_name = 'STP'
nameslist = ['SFF_BONVEGPP']
ETL(headers,base_name,nameslist,path,extracted_path,0,0) # 1 for skip split_CSV,second for skip bulk
'''

'''
print('SFF_BONVEGRI')
headers = ['MKT_TAG','PROD_TAG','PER_TAG','F000000000000000000100000000000000000000','F000000000000000000200000000000000000000','F000000000000000000300000000000000000000',
     'F000000000000000014500000000000000000000', 'F000000000000000014600000000000000000000', 'F000000000000000105300000000000000000000',
     'F000000000000000206900000000000000000000', 'F000000000000000207700000000000000000000', 'F000000000000000236000000000000000000000',
     'F000000000000000236400000000000000000000', 'F000000000000000551800000000000000000000', 'F000000000000000582200000000000000000000',
     'F000000000000000592300000000000000000000', 'F000000000000000652200000000000000000000']
base_name = 'TNU'
nameslist = ['SFF_BONVEGRI']
ETL(headers,base_name,nameslist,path,extracted_path,0,0) # 1 for skip split_CSV,second for skip bulk
'''
