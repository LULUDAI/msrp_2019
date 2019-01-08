import ruamel.yaml
import pymongo
from pymongo import MongoClient 
import pandas as pd

def load_config(cf_file):
    """
    This function loads the configuration file. 
    """
    with open(cf_file, 'r') as yaml:
        cf=ruamel.yaml.round_trip_load(yaml, preserve_quotes=True)
    return cf

def connect_mongo(cf):
    """
    This function creates a mongo connection. 
    """
    client = MongoClient(cf['mongo_uri'])
    db = client[cf['mongo_db']]
    return db

def get_collection(cf, collection):
    #set the key for the collection
    colkey="db-"+collection
    #count total number of documents
    count_docs = cf[colkey].count_documents({})
    cursor_proper = pd.DataFrame(index=range(0, count_docs))
    result= data_preparation(cf[colkey], cursor_proper, cf[collection])
    return   result

def data_preparation(collection, cursor_proper, collection_list):
    """
    This function creates a mongo connection. 
    collection= a Mongo collection object.
    cursor_proper = an empty dataframe
    collection_list = a list of items to get. 
    """
    for collections in collection_list:
        exec("cursor = collection.find({ }, { '%s': 1,'_id':0} )" %(collections))
        column_list = collections.split('.')
        column_data = 'x'
        for column in column_list:
            if(column != 'data'):
                column_data += "['%s']"%(column)
        exec("cursor_proper['%s'] = pd.DataFrame(list(cursor))['data'].apply(lambda x: %s)"%(collections,column_data))
    return cursor_proper