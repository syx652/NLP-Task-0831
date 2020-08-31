from bert_serving.client import BertClient
worker=1
#需要安装bertServer

import csv
import jieba #需要安装jieba
import math

from elasticsearch import Elasticsearch

es = Elasticsearch(http_auth=("elastic", "elastic"),port=9200)



class printBackground:
    def print1(self):
        print("**************************************************************************************")
        #for i in range(0,10):
           #print("***                                                                                ***")

   # def print2(self):
       # for i in range(0,10):
            #print("***                                                                                ***")
    def print3(self):
        #for i in range(0,10):
            #print("***                                                                                ***")
        print("******************************对话已经结束**********************************************")
        print("**************************************************************************************")

#获得句向量
class sentenceEmbedding:
    def getEmbedding(self,userSentence):
        bc = BertClient()
        self.temp = userSentence
        vecs = bc.encode(userSentence)
        #print(vecs)
        return vecs


#获得elasticsearch中的回答并写入指定的csv文件中
class writeToCsv:
    def getAndWrite(self):
        query = es.search(index="match_review", body={"query": {"match_all": {}}}, scroll='5m', size=100)

        results = query['hits']['hits']  # es查询出的结果第一页
        total = query['hits']['total']  # es查询出的结果总量
        scroll_id = query['_scroll_id']  # 游标用于输出es查询出的所有结果

        for i in range(0, int(total / 100) + 1):
            # scroll参数必须指定否则会报错
            query_scroll = es.scroll(scroll_id=scroll_id, scroll='5m')['hits']['hits']
            results += query_scroll

        with open('./data/event_title.csv', 'w', newline='', encoding='utf-8') as flow:
            csv_writer = csv.writer(flow)
            for res in results:
                # print(res)
                csv_writer.writerow([res['_id'] + ',' + res['_source']['title']])

'''
从csv文件中取出回答
每次返回一行数据（一个回答的编号和回答）
'''
class getAnswer:
    def getAnswer(self,docmentName,lineNumber):
        file = open(docmentName+".csv","r")
        for i in range(0,lineNumber):
            line = file.readline()
            if not line:
                line = "ItIsOver"
        file.close()
        return line

'''
class getCosSimilarity:
    def CosSimilarity(UL, p1, p2):
        si = GetSameItem(UL, p1, p2)
        n = len(si)
        if n == 0:
            return 0

        s = sum([UL[p1][item] * UL[p2][item] for item in si])
        den1 = math.sqrt(sum([pow(UL[p1][item], 2) for item in si]))
        den2 = math.sqrt(sum([pow(UL[p2][item], 2) for item in si]))
        return s / (den1 * den2)
#一种较为简易的方法，但是使用时报错，不单单是那个函数的问题
'''


class toGetCosSimilarity:
    def cosSimilarity(self,embedding1,embedding2):
        #
        s1 = embedding1
        s1_cut = [i for i in jieba.cut(s1, cut_all=True) if i != '']
        s2 = embedding2
        s2_cut = [i for i in jieba.cut(s2, cut_all=True) if i != '']
        #print(s1_cut)
        #print(s2_cut)
        word_set = set(s1_cut).union(set(s2_cut))
        #print(word_set)

        word_dict = dict()
        i = 0
        for word in word_set:
            word_dict[word] = i
            i += 1
        #print(word_dict)

        s1_cut_code = [word_dict[word] for word in s1_cut]
        #print(s1_cut_code)
        s1_cut_code = [0] * len(word_dict)

        for word in s1_cut:
            s1_cut_code[word_dict[word]] += 1
        #print(s1_cut_code)

        s2_cut_code = [word_dict[word] for word in s2_cut]
        #print(s2_cut_code)
        s2_cut_code = [0] * len(word_dict)
        for word in s2_cut:
            s2_cut_code[word_dict[word]] += 1

        sum = 0
        sq1 = 0
        sq2 = 0
        for i in range(len(s1_cut_code)):
            sum += s1_cut_code[i] * s2_cut_code[i]
            sq1 += pow(s1_cut_code[i], 2)
            sq2 += pow(s2_cut_code[i], 2)

        try:
            result = round(float(sum) / (math.sqrt(sq1) * math.sqrt(sq2)), 2)
        except ZeroDivisionError:
            result = 0.0
        return result










test = printBackground()
embedding = sentenceEmbedding()

strJudge = "再见"
test.print1()
print ("你好。")
strUser = input("                  ")
docCSVName = './data/event_title.csv'

#print(embedding.getEmbedding(strUser))
#用于确定向量化成功，返回值给到匹配算法即可
nowEmbedding = embedding.getEmbedding(strUser)#使用者的回答的句向量
ifOut = 1
i = 1
writeToCsv.getAndWrite()
beforeEmbedding = sentenceEmbedding.getEmbedding(getAnswer.getAnswer(docCSVName,i))   #获取elasticsearch中回答的句向量
i = i+2


while(ifOut != getAnswer.getAnswer(docCSVName,i)):
    nextEmbedding = sentenceEmbedding.getEmbedding(getAnswer.getAnswer(docCSVName,i))
    beforeCosSimiliarity=toGetCosSimilarity.cosSimilarity(nowEmbedding,beforeEmbedding)
    nextCosSimilarity = toGetCosSimilarity.cosSimilarity(nowEmbedding,nextEmbedding)
    if beforeCosSimiliarity <= nextCosSimilarity :
        beforeEmbedding = nextEmbedding
        finalLineNumber = i


reply = getAnswer.getAnswer(docCSVName,i+1)
#reply = "系统检索到的回答"
print(reply)
print("输入“再见”以结束对话")
while(strUser != strJudge):
    strUser = input("                  ")
    #nowEmbeddding = embedding.getEmbedding(strUser)#使用者的回答的句向量
    nowEmbedding = embedding.getEmbedding(strUser)  # 使用者的回答的句向量
    ifOut = 1
    i = 1
    writeToCsv.getAndWrite()
    beforeEmbedding = sentenceEmbedding.getEmbedding(getAnswer.getAnswer(docCSVName, i))  # 获取elasticsearch中回答的句向量
    i = i + 2

    while (ifOut != getAnswer.getAnswer(docCSVName, i)):
        nextEmbedding = sentenceEmbedding.getEmbedding(getAnswer.getAnswer(docCSVName, i))
        beforeCosSimiliarity = toGetCosSimilarity.cosSimilarity(nowEmbedding, beforeEmbedding)
        nextCosSimilarity = toGetCosSimilarity.cosSimilarity(nowEmbedding, nextEmbedding)
        if beforeCosSimiliarity <= nextCosSimilarity:
            beforeEmbedding = nextEmbedding
            finalLineNumber = i

    reply = getAnswer.getAnswer(docCSVName, i + 1)
    print(reply)
    print("输入“再见”以结束对话")

test.print3()



