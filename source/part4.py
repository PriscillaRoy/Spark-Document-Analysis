from pyspark import SparkContext,SparkConf
import sys

conf = SparkConf().setAppName("part4")
sc = SparkContext(conf = conf)

#/bigd29/output_hw2/big/p3/3
#/bigd29/output_hw2/small/p3/1
#/bigd29/output_hw2/medium/p3/6
RDD_Similardocs  = sc.wholeTextFiles("/bigd29/output_hw2/big/p3/1",use_unicode=False).map(lambda (file,invertedIndex) : invertedIndex.split("\n"))
RDD_mapped = RDD_Similardocs.flatMap(lambda x : x)
RDD_processed = RDD_mapped.filter(lambda x : len(x) > 0).map(lambda x : eval(x))

file_data = RDD_processed.takeOrdered(10, key = lambda x: -x[1])

file_data = [i[0] for i in file_data]

finalFile = sc.parallelize(file_data)
finalFile.saveAsTextFile(sys.argv[1])
