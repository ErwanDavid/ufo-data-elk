from datetime import datetime
from stop_words import get_stop_words
import spacy 


nlp = spacy.load("en_core_web_sm")


def getNER(txt):
    doc = nlp(txt) 
    ent_array={}
    for ent in doc.ents:
        ent_array[ent.label_] = ent.text
        print(ent.text, ent.start_char, ent.end_char, ent.label_)
    return ent_array

def curate_keyword(object_in):
    object_out = object_in
    if 'desc' in object_in.keys():
        dataset = object_in['desc'].lower().replace(':',' ').replace(',',' ').split(' ') # todo remove stopwords
        str_list = [word for word in dataset if word not in get_stop_words('english')]
        object_out['calc_kw'] = list(filter(None, str_list))
        object_out['NER'] = getNER(object_in['desc'])
    return object_out

def create_index(client, index_name):
    mappings = { "mappings": {"properties": {
      "calc_geo": {
        "type": "geo_point"
      } }}}
    client.indices.create(index=index_name, body=mappings)

def curate_date(object_in, date_str):
    object_out = object_in
    if date_str in object_in.keys() and object_in[date_str] :
        print(str(len(object_in[date_str])))
        # date look '02/28/2023' or ...
        if len(object_in[date_str]) == 2 or len(object_in[date_str]) == 3 :
            date_format = '%y'
        elif len(object_in[date_str]) == 4:
            date_format = '%Y'
        elif len(object_in[date_str]) == 5 or len(object_in[date_str]) == 6 or len(object_in[date_str]) == 7:
            date_format = '%m/%y'
        elif len(object_in[date_str]) == 8:
            date_format = '%m/%d/%y'
        elif len(object_in[date_str]) == 15 or len(object_in[date_str]) == 16:
            date_format = '%m/%d/%Y %H:%M'
        else :
            date_format = '%m/%d/%Y'
        try :
            date_obj = datetime.strptime(object_in[date_str], date_format)
        except ValueError:
            date_obj = datetime.strptime("01/01/0001", '%m/%d/%Y')
        object_out['calc_date'] = date_obj
        try:
            object_out['calc_year'] = object_in[date_str].split('/')[-1]
        except:
            object_out['calc_year'] = 1
    return object_out
