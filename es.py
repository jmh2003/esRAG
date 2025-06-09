import math
from abc import ABC
from typing import Any, Dict, List, Literal, Union
from elasticsearch.helpers import BulkIndexError

from elasticsearch import Elasticsearch, helpers


BATCH_SIZE = 10000
# 在文件顶部添加查重功能的函数

import hashlib



class Es(ABC):
    """
    Connect to the ElasticSearch engine and perform related operations.
    """
    def __init__(
            self, 
            host: str, 
            port: Union[int, str], 
            user: str, 
            password: str,
            index: str
    ):
        self.host = 'https://' + host + ':' + (port if type(port) == str else str(port))
        self.es = Elasticsearch(hosts=[self.host],
                                basic_auth=(user, password),
                                verify_certs=False,
                                retry_on_timeout=True,
                                sniff_on_node_failure=True,)
        self.index = index
        
    def get_es(self):
        """Return to the native ES interface"""
        return self.es
    
    def get_all_index(self):
        """Retrieve all index information."""
        print(self.es.cat.indices())

    def get_count(self):
        """
        Return the number of documents indexed in the cluster:
        - index: None means to return the total number of documents across all indices,
        - str means to return the number of documents in a single index,
        - List[str] means to return the total number of documents in the specified indices.
        """
        return self.es.cat.count(index=self.index, format='json')
    
    def get_health_info(self):
        return self.es.cat.health(format='json')
    
    def get_ip(self):
        return self.es.cat.master(format='json')
    
    def create_index(self, index, settings, mappings):
        response = self.es.indices.create(index=index, 
                                          settings=settings, 
                                          mappings=mappings)
        return response
    
    def delete_index(self, index):
        return self.es.indices.delete(index=index)
    
    # 修改search方法，确保返回文档ID

    def search(self, query: dict) -> Dict[str, Any]:
        """
        单字段查询，修改以返回文档ID
        """
        method = query['method']
        size = query.get('size', 10)
        
        # 处理match_all查询
        if method == 'match_all':
            query_body = {"query": {"match_all": {}}}
            results = self.es.search(index=self.index, body=query_body, size=size)
        else:
            field = query['field']
            q = query['q']
            results = self.es.search(index=self.index, 
                                    query={method: {field: q}},
                                    size=size)
        
        items = []
        for item in results['hits']['hits']:
            title, text = item['_source']['text'].split('[title][text]')
            item_new = item['_source']
            item_new['title'] = title.replace('[title]', '')
            item_new['text'] = text.replace('[text]', '')
            item_new['_id'] = item['_id']  # 加入文档ID
            items.append(item_new)

        res_format = {
            'num': len(items),
            'items': items
            }
        
        return res_format
    
    def add_single_data(self, document):
        self.es.index(index=self.index,
                      document=document)
        
    def add_many_data(self, documents: list):
        """When a large amount of data is imported, a tuple (number of successes, list of failures) will be returned."""
        num = len(documents)
        batch_num = math.ceil(num / BATCH_SIZE)

        res = [0, []]
        for i in range(batch_num):
            docs = []
            for d in documents[i*BATCH_SIZE:(i+1)*BATCH_SIZE]:
                docs.append({
                    "_index": self.index,
                    "_source": d
                })
            batch_res = helpers.bulk(self.es, docs)
            res[0] += batch_res[0]
            res[1] += batch_res[1]
            
        return res
    # 在Es类中添加以下方法

    def delete_by_id(self, doc_id):
        """
        根据ID删除文档
        """
        try:
            result = self.es.delete(index=self.index, id=doc_id)
            return {'status': 'success', 'result': result}
        except Exception as e:
            print(f"删除文档时发生错误: {e}")
            return {'status': 'error', 'message': str(e)}

    def insert(self, document, doc_id=None):
        """
        插入单个文档，支持指定ID
        """
        try:
            if doc_id:
                return self.es.index(index=self.index, id=doc_id, document=document)
            else:
                return self.es.index(index=self.index, document=document)
        except Exception as e:
            print(f"插入文档失败: {e}")
            return None