import os
import csv
import pandas as pd
import bondetl as be



class ETL():
    def __init__(self, headers,base_name,nameslist,path,extracted_path,skip_split = 0,skip_bulk=0):
        super(ETL, self).__init__()
        self.headers = headers
        self.base_name = base_name
        self.nameslist = nameslist
        self.path = path
        self.extracted_path = extracted_path
    
        
        ### Fuction part ###
        def get_vocas():
            arr = []
            for name in nameslist:
                f = os.listdir(extracted_path+name)
                for el in f:
                    arr.append(extracted_path+name+'/'+el)
            return arr

        def get_csvs_from_folder():
            arr = []
            for name in nameslist:
                f = os.listdir(extracted_path+name+'/splited')
                for el in f:
                    arr.append(extracted_path+name+'/splited/'+el)
            return arr
        def split_csv():
            for name in nameslist:
                f = os.listdir(extracted_path+name)
                for el in f:
                    if el.find('_fact_dat')>-1:
                        file = extracted_path+name+'/'+el
                try:
                    os.mkdir(extracted_path+name+'/splited')
                except:
                    pass
                for i,chunk in enumerate(pd.read_csv(file, chunksize=1000000,sep="|")):
                    chunk.to_csv(extracted_path+name+'/splited/'+name+'_fact_data{}.csv'.format(i),sep="|",columns=headers, index=False)
        
        def bulk(Table_in,files,headers,row_separator=r'\r\n'):
            sqlTableIn = be.SQL('Nielsen',Table_in)
            for i,j in enumerate(headers):
                headers[i]= f'[{j}] [nvarchar](255) NULL'
            headers = str.join(', ', headers)
            sql_query_create = f'''
            if  object_id('{Table_in}') is not null  drop table {Table_in}
            ; 
            CREATE TABLE {Table_in} ({headers})'''
            sqlTableIn.Query(sql_query=sql_query_create)
            for i,pathCSV in enumerate(files):
                print(str(i)+'/'+str(len(files)))
                sqlTableIn.bulk(pathCSV,start_from_row = '2',delimiter = "|",row_separator=row_separator)
            print(str(len(files))+'/'+str(len(files)))

        if skip_split == 0:
            split_csv()
        if skip_bulk == 0:
            bulk('fact_'+base_name,get_csvs_from_folder(),headers,row_separator='0x0a')
        for f in get_vocas():
            name = f[-8:][:4].replace('_','') 
            full_name = 'voca_'+base_name+'_'+f[len(extracted_path)+19:][:4]+'_'+name
            try: 
                ['FCT','MKT','PER','PROD'].index(name)
                headers = list(pd.read_csv(f,sep="|",low_memory=False).columns)
                #print(full_name)
                bulk(full_name,[f],headers,row_separator='0x0a')    
            except:
                try:
                    name = f[-17:][:4].replace('_','')
                    full_name = 'voca_'+base_name+'_'+f[len(extracted_path)+19:][:4]+'_'+name
                    ['FCT','MKT','PER','PROD'].index(name)
                    headers = list(pd.read_csv(f,sep="|",low_memory=False).columns)
                    #print(full_name)
                    bulk(full_name,[f],headers,row_separator='0x0a')
                except:
                    pass




if __name__ == '__main__':
    pass