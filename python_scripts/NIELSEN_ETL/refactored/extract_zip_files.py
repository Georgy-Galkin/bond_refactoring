import zipfile as zf
import os
import  logging
import re


path = 'N:/NB/'
extracted_path = path+'extracted_data/'
try:
    os.mkdir(extracted_path)    
except:
    pass
zip_file_list = os.listdir(path)
list_of_files = []
temp_counter = 0
zip_file_list = [file[0:file.find('.')] for file in zip_file_list if file.find('.zip') > 0]
nameslist = zip_file_list[:]
#write_to_csv = csv.writer(open(extracted_path+'fact_data/MERGED_fact_data.csv','w',newline=""),delimiter="|")


# p = re.compile(r'_')
# arr = []
# for i in nameslist:
#     for q in range(len(nameslist)):
#         p.split(nameslist[q])
#     f = p.split(i)
#     c = f[0] + "_" + f[1]
#     arr.append(c)
# print(f, c, arr, zip_file_list)
# exit()

def extract():
    p = re.compile(r'_')
    arr = []
    for i in nameslist:
        for q in range(len(nameslist)):
            p.split(nameslist[q])
        f = p.split(i)
        c = f[0] + "_" + f[1]
        arr.append(c)
    progress_bar = 1 
    logging.info('Extracting data from ZIP')
    logging.info('0/%d'%len(zip_file_list))
    for i in range(len(zip_file_list)):
        zip_file_list[i] = path+zip_file_list[i]+'.zip'
        with zf.ZipFile(zip_file_list[i]) as f:
            temp_list = f.namelist()
            temp_counter = 0
            for j in temp_list:
                if j.find('.csv') == -1:
                    temp_list.pop(temp_counter)
                temp_counter = temp_counter+1
            list_of_files.append(temp_list)
            for file in temp_list:
               # print(nameslist[i].replace('BON_',''))
                f.extract(file,extracted_path+'/'+arr[i].replace('BON_',''))  
                os.rename(extracted_path+'/'+arr[i].replace('BON_','')+'/'+file,extracted_path+'/'+arr[i].replace('BON_','')+'/'+file.replace('BON_',''))  
            logging.info('%d/%d'%(progress_bar,len(zip_file_list)))            
            progress_bar = progress_bar+1
    logging.info('ZIP Extraction Done!')



if __name__ == '__main__':
    extract()
