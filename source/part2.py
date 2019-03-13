
from pyspark import SparkContext,SparkConf
from operator import add
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer
import ntpath
import sys
import re

conf = SparkConf().setAppName("part2 parquet")
sc = SparkContext(conf = conf)
ps = PorterStemmer()
lemmatiser = WordNetLemmatizer()
#Arguments from user
outputfile   = sys.argv[1]
inputfile   = sys.argv[2]
#popwordsfile   = sys.argv[3]
#List of popular words
#/bigd29/output_hw2/big/p1/6/
#/bigd29/output_hw2/big/p1/11/
#/bigd29/output_hw2/medium/p1/1
RDD_popwords  = sc.wholeTextFiles("/bigd29/output_hw2/medium/p1/1",use_unicode=False).map(lambda (file,popularWords) : popularWords.split("\n")).flatMap(lambda x : x)
List_popwords = RDD_popwords.collect()

#Reading the file
fileData = sc.wholeTextFiles(inputfile,use_unicode=False)

stopwordsList = set(stopwords.words('english'))
fileData_base = fileData.map(lambda(filename,content) : (ntpath.basename(filename),content) )

# Removing special characters
#Pre-process the words and convert to lower case
fileData_preprocess = fileData_base.map(lambda(file, content) :(file, re.sub('[^a-z| |0-9-]', '', content.strip().lower())))
#Convert the file to lowercase

fileData_split = fileData_preprocess.map(lambda (file,content) : (file,content, len(content.split(" "))))
#Finding the data for popular words
file_popwords = fileData_split.map(lambda (file,content,length) : [((file,word,length),1) for word in content.split(" ") if word in List_popwords and len(word) > 0 ])
#Converting to a RDD
fileData_RDD = file_popwords.flatMap(lambda met : met)
#Reducing to word counts
fileData_reduced = fileData_RDD.reduceByKey(add, numPartitions=1)
#Finding the weight of each word - dividing the number of times the word occurs by total number of words present in the document
fileData_invindex = fileData_reduced.map(lambda indexval : (indexval[0][1],[(indexval[0][0],float(indexval[1])/indexval[0][2])]))
#Reducing the file based on the weight
fileData_final = fileData_invindex.reduceByKey(lambda x,y : x + y)



fileData_final.saveAsTextFile(outputfile)

