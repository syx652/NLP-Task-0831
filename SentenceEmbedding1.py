from bert_serving.client import BertClient
worker=1


class sentenceTmbedding:
    def getEmbedding(self,userSentence):
        bc = BertClient()
        self.temp = userSentence
        vecs = bc.encode(userSentence)
        #print(vecs)
        return vecs






