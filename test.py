from pickle import PUT, GET

from elasticsearch import Elasticsearch
'''
es = Elasticsearch('http://localhost:9200/json1')

result = es.indices.create(index='json1', ignore = 400)
print(result)


'''

es = Elasticsearch('http://localhost:9200/')    # 默认连接本地elasticsearch
print(es.index(index='base1', doc_type='doc', id=112, body={'question': "", "answer": ""}))
print(es.get(index='base1', doc_type='doc', id=112))


'''
#创建索引，无报错，返回值正确，应该是elasticsearch里没插入内容的那个
from elasticsearch import Elasticsearch

es = Elasticsearch()

result = es.indices.create(index='index-name', ignore = 400)
print(result)

#删除索引
es = Elasticsearch()

result = es.indices.delete(index='index-name', ignore = 400)
print(result)
'''

'''
#by elastic官网
#自定义ID创建索引，报错website、blog未定义，忽略之后仍无法运行
PUT /website/blog/123
{
  "title": "My first blog entry",
  "text":  "Just trying this out...",
  "date":  "2014/01/01"
}


'''



'''

es = Elasticsearch('http://localhost:9200/newtest2')

mappings = {
            "mappings": {
                "type_doc_test": {                           #type_doc_test为doc_type
                    "properties": {
                        "id": {
                            "type": "long",
                            "index": "false"
                        },
                        "serial": {
                            "type": "keyword",  # keyword不会进行分词,text会分词
                            "index": "false"  # 不建索引
                        },
                        #tags可以存json格式，访问tags.content
                        "tags": {
                            "type": "object",
                            "properties": {
                                "content": {"type": "keyword", "index": True},
                                "dominant_color_name": {"type": "keyword", "index": True},
                                "skill": {"type": "keyword", "index": True},
                            }
                        },
                        "hasTag": {
                            "type": "long",
                            "index": True
                        },
                        "status": {
                            "type": "long",
                            "index": True
                        },
                        "createTime": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                        },
                        "updateTime": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                        }
                    }
                }
            }
        }

res = es.indices.create(index = 'index_test',body =mappings)

'''



'''
#把UDC导进来（？）
PUT job
{
  "settings":{
    "index":{
      "number_of_shards":5,
      "number_of_replicas":1
    }
  }
}
'''
'''
PUT /my_index/blog/1
{ "title": "I'm happy for this fox" }

PUT /my_index/blog/2
{ "title": "I'm not happy about my fox problem" }

PUT /my_index
{
  "mappings": {
    "blog": {
      "properties": {
        "title": {
          "type": "string",
          "fields": {
            "standard": {
              "type":     "string",
              "analyzer": "chinese"
            }
          }
        }
      }
    }
  }
}

GET /_search
{
  "query": {
    "multi_match": {
      "type":     "most_fields",
      "query":    "not happy foxes",
      "fields": [ "title", "title.english" ]
    }
  }
}
'''




#下载中文分词器，将UDC导入elasticsearch，向量模型