import csv, codecs, sys
import pprint as pp
from pathlib import Path
from datetime import datetime
from elasticsearch import Elasticsearch
from stop_words import get_stop_words

print(get_stop_words('english'))


client = Elasticsearch("http://localhost:9200",  basic_auth=("elastic", 'changeme'))
index_name = "ufo-complete-04"

COLNAME = ['date','city','state','contry','shape','duration_int','duration','desc','date_report','geo_lat','geo_long']

def get_csv_array(file):
    print(file)
    array_out = []
    
    with open(file, mode='r') as infile:
        reader = csv.reader(infile)
        for rows in reader:
            #print(rows)
            object_out = {}
            for i in range (0, len(COLNAME)):
                #print(COLNAME[i], ':', rows[i])
                object_out[COLNAME[i]] = rows[i]
            array_out.append(object_out)
    return array_out
    
def curate_location(object_in):
    object_out = object_in
    object_out['location'] = f"{object_in['city']} {object_in['state']} {object_in['contry']}" 
    object_out['calc_geo']=( float(object_in['geo_long']), float(object_in['geo_lat']))
    return object_out
    
def curate_keyword(object_in):
    object_out = object_in
    if 'desc' in object_in.keys():
        dataset = object_in['desc'].lower().replace(':',' ').replace(',',' ').split(' ') # todo remove stopwords
        str_list = [word for word in dataset if word not in get_stop_words('english')]
        object_out['calc_kw'] = list(filter(None, str_list))
    return object_out

def curate_date(object_in):
    object_out = object_in
    if 'date' in object_in.keys():
        print(str(len(object_in['date'])))
        # date look '02/28/2023' or ...
        date_format = '%m/%d/%Y %H:%M'
        try :
            date_obj = datetime.strptime(object_in['date'], date_format)
        except ValueError:
            date_obj = datetime.strptime("01/01/0001", '%m/%d/%Y')
        object_out['calc_date'] = date_obj
        try:
            object_out['calc_year'] = date_obj.year
        except:
            object_out['calc_year'] = 1
    return object_out

def create_index():
    mappings = { "mappings": {"properties": {
      "calc_geo": {
        "type": "geo_point"
      } }}}
    client.indices.create(index=index_name, body=mappings)

def load_csv(file):
    #try:
        item_list = get_csv_array(file)
        my_id=0
        file_str=str(file)
        for item in item_list:
            try:
                my_id +=1
                detailled_item = curate_date(item)
                detailled_item = curate_keyword(detailled_item)
                detailled_item = curate_location(detailled_item)
                detailled_item['calc_src'] = file_str.split('/')[1].replace('.json','')
                pp.pprint(detailled_item)
                client.index(index=index_name, id=my_id, document=detailled_item)
                print('____')
            except:
                print("Error inserting" + file_str)
    #except:
    #    print("Error parsing file")

create_index()


load_csv(sys.argv[1])