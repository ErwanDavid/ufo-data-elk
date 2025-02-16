import csv, codecs, sys
import pprint as pp
from elasticsearch import Elasticsearch
import ufodata as ud


client = Elasticsearch("http://localhost:9200",  basic_auth=("elastic", 'changeme'))
index_name = "ufo-noforc-2013"

COLNAME = ['date','city','state','country','shape','duration_int','duration','desc','date_report','geo_lat','geo_long']

def get_csv_array(file):
    print(file)
    array_out = []
    
    with open(file, mode='r') as infile:
        reader = csv.reader(infile)
        for rows in reader:
            #print(rows)
            object_out = {}
            for i in range (0, len(COLNAME)):
                #print(COLNAME[i], ':', rows[i] )
                object_out[COLNAME[i]] = rows[i]
            array_out.append(object_out)
    return array_out
    
def curate_location(object_in):
    object_out = object_in
    object_out['location'] = f"{object_in['city']} {object_in['state']} {object_in['country']}" 
    try:
        object_out['calc_geo']=( float(object_in['geo_long']), float(object_in['geo_lat']))
    except:
         print('Error', object_in['geo_long'],object_in['geo_lat'])
    return object_out


def load_csv(file):
    #try:
        item_list = get_csv_array(file)
        my_id=0
        file_str=str(file)
        for item in item_list:
            my_id +=1
            pp.pprint(item)
            detailled_item = ud.curate_date(item)
            detailled_item = ud.curate_keyword(detailled_item)
            detailled_item = curate_location(detailled_item)
            detailled_item['calc_src'] = file_str.split('/')[1].replace('.json','').replace('.csv','')
            pp.pprint(detailled_item)
            client.index(index=index_name, id=my_id, document=detailled_item)
            print('____')

ud.create_index(client, index_name)


load_csv(sys.argv[1])