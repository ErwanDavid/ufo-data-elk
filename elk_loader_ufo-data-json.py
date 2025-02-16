import json, codecs
import pprint as pp
from pathlib import Path
from elasticsearch import Elasticsearch
from stop_words import get_stop_words
from geopy.geocoders import Nominatim
import ufodata as ud


client = Elasticsearch("http://localhost:9200",  basic_auth=("elastic", 'changeme'))
index_name = "ufo-data-all"

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


def load_json(file):
    try:
        item_list = get_json_array(file)
        my_id=0
        file_str=str(file)
        for item in item_list:
            try:
                my_id +=1
                detailled_item = ud.curate_date(item)
                detailled_item = ud.curate_keyword(detailled_item)
                detailled_item = curate_location(detailled_item)
                detailled_item['calc_src'] = file_str.split('/')[1].replace('.json','')
                pp.pprint(detailled_item)
                client.index(index=index_name, id=my_id, document=detailled_item)
                print('____')
            except:
                print("Error inserting" + file_str)
    except:
        print("Error parsing file")

ud.create_index()

files = Path('./data/bin/').glob('*json')
for file in files :
    print(file)
    load_json(file)
