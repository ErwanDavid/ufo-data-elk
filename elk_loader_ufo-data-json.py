import json, codecs
import pprint as pp
from pathlib import Path
from datetime import datetime
from elasticsearch import Elasticsearch
from stop_words import get_stop_words
from geopy.geocoders import Nominatim

print(get_stop_words('english'))


client = Elasticsearch("http://localhost:9200",  basic_auth=("elastic", 'changeme'))
index_name = "ufo-index-13"

geolocator = Nominatim(user_agent=index_name)

def get_json_array(file):
    d = json.load(codecs.open(file, 'r', 'utf-8-sig'))
    for timeline in d.keys():
        print(timeline)
        return d[timeline]
    
def curate_location(object_in):
    object_out = object_in
    if 'location' in object_in.keys():
        location = geolocator.geocode(object_in['location'], timeout=20)
        if location : 
            object_out['calc_geo']=( location.longitude, location.latitude)
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
        if len(object_in['date']) == 2 or len(object_in['date']) == 3 :
            date_format = '%y'
        elif len(object_in['date']) == 4:
            date_format = '%Y'
        elif len(object_in['date']) == 5 or len(object_in['date']) == 6 or len(object_in['date']) == 7:
            date_format = '%m/%y'
        elif len(object_in['date']) == 8:
            date_format = '%m/%d/%y'
        else :
            date_format = '%m/%d/%Y'
        try :
            date_obj = datetime.strptime(object_in['date'], date_format)
        except ValueError:
            date_obj = datetime.strptime("01/01/0001", '%m/%d/%Y')
        object_out['calc_date'] = date_obj
        try:
            object_out['calc_year'] = object_in['date'].split('/')[-1]
        except:
            object_out['calc_year'] = 1
    return object_out

def create_index():
    mappings = { "mappings": {"properties": {
      "calc_geo": {
        "type": "geo_point"
      } }}}
    client.indices.create(index=index_name, body=mappings)

def load_json(file):
    try:
        item_list = get_json_array(file)
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
    except:
        print("Error parsing file")

create_index()

files = Path('./data/bin/').glob('*json')
for file in files :
    print(file)
    load_json(file)
