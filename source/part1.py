from pyspark import SparkContext,SparkConf
from operator import add
from nltk.corpus import stopwords
import sys
import re
import ntpath

#Text Preprocessing
def PreProcess(text):
    return re.sub('[^a-z| |0-9-]', '', text.strip().lower())

conf = SparkConf()
conf.setAppName("Part-1")
sc = SparkContext(conf=conf)
#Arguments from User
outputfile   = sys.argv[1]
inputfile   = sys.argv[2]
#List of stop words
stopwordsList = set(stopwords.words('english'))
#Reading the whole folder
fileData = sc.wholeTextFiles(inputfile, use_unicode= False)
#Trimming the full-filename
fileData_base = fileData.map(lambda(filename,content) : (ntpath.basename(filename),content) )
#Pre-processing the data and converting it to lower case
fileData_preprocess = fileData_base.map(lambda(file, content) :(file, re.sub('[^a-z| |0-9-]', '', content.strip().lower())))

#Removing a list of list structure to make it Reducer compatible - checking if the word is not in stopwords
fileData_split = fileData_preprocess.flatMap(lambda (file, content) : [(word,1) for word in content.split(" ") if not word in stopwordsList])
#Stemmming
#fileData_stemmed = fileData_split.map(lambda (word, count) : (ps.stem(word), count))

#Counting the words
fileData_reduced = fileData_split.reduceByKey(add, numPartitions=1)
#Finding the top 1000 words - by index 1 - which has count of the word
fileData_topwords = fileData_reduced.takeOrdered(1000, key = lambda x: -x[1])
#fileData_sorted = fileData_reduced.sortByValue(False).map(lambda x : x[0]).top(1000)
fileData_topwords = [i[0] for i in fileData_topwords]
#Converting it to RDD
finalFile = sc.parallelize(fileData_topwords)
#Saving the file to a location
finalFile.saveAsTextFile(outputfile)