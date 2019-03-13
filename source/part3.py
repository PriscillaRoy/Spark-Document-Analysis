
from pyspark import SparkContext,SparkConf
import sys
from operator import mul

outputfile   = sys.argv[1]
inputfile   = sys.argv[2]
def similarity_Matrix(postings):
    sim_matrix = list()
    doc_weight   = postings[1] # list of postings excluding the word
    for i in range(len(doc_weight)):
        for j in range(i+1,len(doc_weight)):
            if(len(doc_weight) == 1):
                break
            else:
                weight_i = doc_weight[i][1]
                weight_j = doc_weight[j][1]
                sim = ((doc_weight[i][0],doc_weight[j][0]),weight_i*weight_j)
                sim_matrix.append(sim)
    return sim_matrix

conf = SparkConf().setAppName("part3")
sc = SparkContext(conf = conf)


#/bigd29/output_hw2/small/p2/22
#/bigd29/output_hw2/big/p2/12
#/bigd29/output_hw2/medium/p2/9
#dataframe.rdd
RDD_InvertedIndex  = sc.wholeTextFiles("/bigd29/output_hw2/big/p2/65",use_unicode=False)
RDD_InvertedIndex_splitted = RDD_InvertedIndex.map(lambda (file, invertedIndex) : invertedIndex.split("\n")).flatMap(lambda x : x)
RDD_InvertedIndex_processed = RDD_InvertedIndex_splitted.filter(lambda x : len(x) > 0).map(lambda x : (eval(x)))


SimilarData = RDD_InvertedIndex_processed.map(similarity_Matrix).flatMap(lambda x : x)
SimilarData_reduced = SimilarData.reduceByKey(lambda x,y : x + y)
SimilarData_sorted = SimilarData_reduced.sortBy(lambda sim : sim[1],ascending = False)


SimilarData_sorted.saveAsTextFile(outputfile)
