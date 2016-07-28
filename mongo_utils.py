#! usr/bin/python
#-*-coding:utf-8-*- 

from pymongo import MongoClient
import os
import re
import types
import config
import json

class MongoDriver:
    '''MongoDB Driver class'''
    database = ''
    client = ''
    db = ''
    #config = ConfigParser.ConfigParser()

    # change to read from configure file later;
    def __init__(self,database):
        host = config.get('mongo','host')
        port = config.get('mongo','port')
        self.client = MongoClient(host,int(port))
        self.database = database
        self.db = self.client[database]

    # insert a document; argument for document can be a dict or a list;
    # return object_id(s)
    def add(self,document,collection='col'):
        self.db.collection_names(include_system_collections=False)
        
        if type(document) is types.DictType:
            object_id = self.db[collection].insert_one(document).inserted_id
        elif type(document) is types.ListType:
            object_id = self.db[collection].insert(document).inserted_id
        else:
            object_id = ''
        return '\"' + str(object_id) + '\"'
       
    # delete 
    # return nums deleted
    def delete(self,query,collection='col'):
        result = self.db[collection].remove(query)
        return result.get('n')
        

    # query
    def get(self,query,sort='',collection='col'):
        print query,type(query)
        if sort == '':
            data = self.db[collection].find(query)
        else:
            data = self.db[collection].find(query).sort(sort)
        #data = self.db[collection].find(query).sort(sort)
        result = []
        for pice in data:
            #tranfer to json
            t = self.transform(pice)
            result.append(t)
        return result


    # update
    def update(self,query,document,collection='col'):
        result = self.db[collection].update(query,{"$set": document})
        return result.get('n')
    
    def transform(self,origin):
        '''
           replace ObjectId with id
        '''
        def _obj_deal(matched):
            matched_id = matched.group("id");
            return "\'" + matched_id + "\'"
        origin_str = str(origin)
        trans_str = re.sub("ObjectId\('(?P<id>.*)'\)",_obj_deal,origin_str).replace("u\'","\'")
        trans_dic = eval(trans_str)
        #return json.dumps(trans_dic,encoding="UTF-8",ensure_ascii=False)
        return trans_dic
        
            
# for test
if __name__ ==  '__main__':
    db = MongoDriver('runoob')
    #post = {"author": "Sherry"}
    post = {}
    insert_id = db.get(post)
    print insert_id
