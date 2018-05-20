# Exercise 9
MapReduce with Apache Spark.


```python
# SOME IMPORTS
import os
import subprocess
import sys
import time
import multiprocessing
import random
import re
```


```python
# SET SOME ENVIRONMENTAL VARIABLES
os.environ['PYSPARK_PYTHON']="python3.6"
os.environ['SPARK_LOCAL_HOSTNAME']="localhost"
os.environ['SPARK_HOME']="/WiPZI/Exercise_9/spark-2.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME']="/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home"
```


```python
# CHECK IF FINDSPARK WORKS CORRECTLY
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
```


```python
# START SPARK CONTEXT ON LOCAL MACHINE
sc = SparkContext("local", appName="Test")
```


```python
# STOP SPARK CONTEXT
sc.stop()
```


```python
# OBTAIN THE NUMBER OF LOGICAL CPUs
cpus = multiprocessing.cpu_count()
print("The number of logical CPUs is " + str(cpus))
```

    The number of logical CPUs is 4


# Exercise 1: Compute the value of PI using Monte Carlo Simulation

This exercise is solved. Your task is to read and analyse the code.


```python
# this method generates one sample point and verifies whether it is inside a circle or not.
# The input is passed via filter method, however, we do not need it here
def inside(inValue):
    x, y = random.random(), random.random()
    return x*x + y*y < 1.0
```


```python
# This method estimates the value of PI
def computePI_MonteCarlo_v1(sc, samples, partitions):
    # Create Resilient Distributed Dataset (RDDs) containing SAMPLES elements.
    # This data is distributed (parallelized) among available nodes (here, CPUs - partitions).
    dff = sc.parallelize(range(0, samples), partitions)
    # Filter out these samples that are not inside a circle.
    # For this purpose, Inside method is run and returns
    # true/false (for each data element) with appropriate probability distribution
    # Why do we generate samples "on fly"?
    filtered = dff.filter(inside)
    # count the number of hits
    left = filtered.count()
    # Estimate the value of PI and return it
    return 4.0 * float(left) / float(samples)
```


```python
### ESTIMATE VALUE OF PI
samples = 10000000

print("Monte Carlo simulation for " + str(samples) + " samples")
print("True value of PI = 3.1415926535...")

## i = number of nodes (CPUs)
for i in range(1, cpus + 1):
    master = "local["+str(i)+"]"
    sc = SparkContext(master, appName="PI_MonteCarlo")
    start_time = time.time()
    piValue = computePI_MonteCarlo_v1(sc, samples, i)
    elapsed = time.time() - start_time
    print("  Number of CPUs = %i | Time = %.4f s | Result(PI) = %.8f" % (i, elapsed, piValue))  
    sc.stop()
```

    Monte Carlo simulation for 10000000 samples
    True value of PI = 3.1415926535...
      Number of CPUs = 1 | Time = 8.7398 s | Result(PI) = 3.14158360
      Number of CPUs = 2 | Time = 4.1961 s | Result(PI) = 3.14297120
      Number of CPUs = 3 | Time = 4.0732 s | Result(PI) = 3.14193880
      Number of CPUs = 4 | Time = 4.1777 s | Result(PI) = 3.14229440


# Exercise 2: Wordcount


```python
# Dummy collection 1: 3 short documents
# create RDD divided into n-partitions
def getSmallCollection_EX1(sc, partitions):
    doc1 = "Roses,are red "
    doc2 = "Roses are roses"
    doc3 = "The Sun is red."
    rdd1 = sc.parallelize([doc1, doc2, doc3], partitions)
    return rdd1
```

1) Dummy collection 2: ~200 documents about animals (ant.html, dog.html, panda.html, hedgehog.html, etc.). For this purpose, download www.cs.put.poznan.pl/mtomczyk/ir/lab8/pages.zip, unzip, and copy "pages" folder into your working directory.


```python
def getLargeCollection_EX1(sc, partitions):
    DOCS = sc.wholeTextFiles("pages/", partitions)
    rdd1 = DOCS.map(lambda x: x[1])
    return rdd1
```


```python
# For a given text "x", this method performs simple tokenization and normalization (returns a list of terms)
def tokenizeAndNormalize(x):
    return [s.lower() for s in re.split(' |;|,|\t|\n|\.', x) if len(s) > 0]
```

2) Init spark context (1 core):


```python
sc = SparkContext("local[1]", appName="Word_count")
```

3) TODO: Collect the data (getSmallCollection_EX1):


```python
rdd1 = getSmallCollection_EX1(sc, 1)
# if you whish to print data stored in rdd, use print(rdd.collect())
print(rdd1.collect())
```

    ['Roses,are red ', 'Roses are roses', 'The Sun is red.']


4) TODO: Firslty, you should tokenize all documents. For this purpose use flatMap function (rdd2 = rdd1.flatMap) where you pass tokenizeAndNormalize method. There are two methods: map and flatMap. Both produce an output for each element of RDD object. The difference is that map keeps produced elements organised and flatMap puts them into a single list, e.g.:


```python
tempRDD = sc.parallelize([("a", 1), ("b", 2)])
print(tempRDD.map(lambda x: (x[0], x[1]+1)).collect())
print(tempRDD.flatMap(lambda x: (x[0], x[1]+1)).collect())
```

    [('a', 2), ('b', 3)]
    ['a', 2, 'b', 3]



```python
# Complete the task here (flatMap with tokenizeAndNormalize):
rdd2 = rdd1.flatMap(lambda x: tokenizeAndNormalize(x))
print(rdd2.collect())
```

    ['roses', 'are', 'red', 'roses', 'are', 'roses', 'the', 'sun', 'is', 'red']


5) TODO: Now for each term produce (term, 1). Use map (why not flatMap?) with lambda function:


```python
rdd3 = rdd2.map(lambda x: (x, 1))
print(rdd3.collect())
```

    [('roses', 1), ('are', 1), ('red', 1), ('roses', 1), ('are', 1), ('roses', 1), ('the', 1), ('sun', 1), ('is', 1), ('red', 1)]


6) TODO: Now it is time to group the results. Use groupByKey method. When any "...byKey" method is invoked, the first element of a stored object is treated as a key. When invoking this method, you should also invoke .mapValues(list) so that all corresponding values will be stored in a single list. E.g.:


```python
tempRDD = sc.parallelize([("a", 1), ("a", 1)])
print(tempRDD.groupByKey().mapValues(list).collect())
```

    [('a', [1, 1])]



```python
# Complete the task here:
rdd4 = rdd3.groupByKey().mapValues(list)
print(rdd4.collect())
```

    [('roses', [1, 1, 1]), ('are', [1, 1]), ('red', [1, 1]), ('the', [1]), ('sun', [1]), ('is', [1])]


7) TODO: Now you could use countByKey method but it returns a dictionarty. Use map function again to sum the elements of a list:


```python
rdd5 = rdd4.map(lambda x: (x[0], sum(x[1])))
print(rdd5.collect())
```

    [('roses', 3), ('are', 2), ('red', 2), ('the', 1), ('sun', 1), ('is', 1)]


8) TODO: It is almost done but we wish the objects to be sorted (alphabetically). You can use sortByKey method:


```python
rdd6 = rdd5.sortByKey()
print(rdd6.collect())
```

    [('are', 2), ('is', 1), ('red', 2), ('roses', 3), ('sun', 1), ('the', 1)]


9) TODO: Done. Bout it could be done in another way. Instead of grouping by key (rdd4) and counting the number of "1"s (rdd5), you could use reduceByKey method. reduceByKey "merges" all object with the same key. Similar to groupByKey, however, instead of grouping, a new value is computed by provided function, e.g.:


```python
tempRDD = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
print(tempRDD.reduceByKey(lambda x, y: x + y).collect())
```

    [('a', 4), ('b', 2)]



```python
# Complete the task here. Use rdd3 object to compute rdd7.
rdd7 = rdd3.reduceByKey(lambda x, y: x + y)
print(rdd7.collect())
```

    [('roses', 3), ('are', 2), ('red', 2), ('the', 1), ('sun', 1), ('is', 1)]


10) TODO: Sort the results:


```python
rdd8 = rdd7.sortByKey()
print(rdd8.collect())
```

    [('are', 2), ('is', 1), ('red', 2), ('roses', 3), ('sun', 1), ('the', 1)]



```python
sc.stop()
```

11) TODO: Complete the method doWordCount (just copy your code, use groupByKey + map(sum) version; should return last rdd object):


```python
def doWordCount(sc, collection, partitions):
    r1 = collection
    r2 = r1.flatMap(lambda x: tokenizeAndNormalize(x))
    r3 = r2.map(lambda x: (x, 1))
    r4 = r3.groupByKey().mapValues(list)
    r5 = r4.map(lambda x: (x[0], sum(x[1])))
    r6 = r5.sortByKey()
    return r6
```

12) TODO: Run the script and observe the results (why is the best time for 1CPU?):


```python
## i = number of nodes (CPUs).
for i in range(1, cpus + 1):
    master = "local["+str(i)+"]"
    sc = SparkContext(master, appName="WordCount")
    start_time = time.time()
    rdd1 = getSmallCollection_EX1(sc, i)
    computedData = doWordCount(sc, rdd1, i)
    elapsed = time.time() - start_time
    print("Number of CPUs = %i | Time = %.4f s " % (i, elapsed))  
    sc.stop()
```

    Number of CPUs = 1 | Time = 0.0272 s
    Number of CPUs = 2 | Time = 0.8426 s
    Number of CPUs = 3 | Time = 0.7997 s
    Number of CPUs = 4 | Time = 0.9669 s


13) TODO: Modyfy the above script (work on a copy, use the cell below) so that the top 3 most common words are printed. Use 1-2CPUs. computedData is an RDD object so you can use sortBy function to resort the elements.


```python
# ORIGINAL
for i in [1,2]:
    master = "local["+str(i)+"]"
    sc = SparkContext(master, appName="WordCount")
    start_time = time.time()
    rdd1 = getSmallCollection_EX1(sc, i)
    computedData = doWordCount(sc, rdd1, i)
    rddSort = computedData.sortBy(lambda x: -x[1])
    elapsed = time.time() - start_time
    print("Number of CPUs = %i | Time = %.4f s " % (i, elapsed))  
    ### PRINT HERE
    sortedData = rddSort.collect()
    for i in range(0, 3): #print top 3
        print("   %i : '%s' occured %d times" % (i+1, sortedData[i][0], sortedData[i][1]))
    ###
    sc.stop()
```

    Number of CPUs = 1 | Time = 0.0339 s
       1 : 'roses' occured 3 times
       2 : 'are' occured 2 times
       3 : 'red' occured 2 times
    Number of CPUs = 2 | Time = 1.2313 s
       1 : 'roses' occured 3 times
       2 : 'are' occured 2 times
       3 : 'red' occured 2 times



```python
# COPY
for i in [1,2]:
    master = "local["+str(i)+"]"
    sc = SparkContext(master, appName="WordCount")
    start_time = time.time()
    rdd1 = getSmallCollection_EX1(sc, i)
    computedData = doWordCount(sc, rdd1, i)
    rddSort = computedData.sortBy(lambda x: -x[1])
    elapsed = time.time() - start_time
    print("Number of CPUs = %i | Time = %.4f s " % (i, elapsed))  

    if i == 2:
        ### PRINT HERE
        sortedData = rddSort.collect()
        for i in range(0, 3): #print top 3
            print("   %i : '%s' occured %d times" % (i+1, sortedData[i][0], sortedData[i][1]))
        ###
    sc.stop()
```

    Number of CPUs = 1 | Time = 0.0538 s
    Number of CPUs = 2 | Time = 1.0129 s
       1 : 'roses' occured 3 times
       2 : 'are' occured 2 times
       3 : 'red' occured 2 times


14) TODO: Repeat the experiment for 1-2CPUs and for 2nd collection (much larger). Compare computation times and print the top 20 most common words. Are the results (the most frequent words) similar to the list of english stop words? Why is the difference in time not as big as in "PI" example?


```python
# do the task here
for i in [1,2]:
    master = "local["+str(i)+"]"
    sc = SparkContext(master, appName="WordCount")
    start_time = time.time()
    rdd1 = getLargeCollection_EX1(sc, i)
    computedData = doWordCount(sc, rdd1, i)
    rddSort = computedData.sortBy(lambda x: -x[1])
    elapsed = time.time() - start_time
    print("Number of CPUs = %i | Time = %.4f s " % (i, elapsed))  

    if i == 2:
        ### PRINT HERE
        sortedData = rddSort.collect()
        for j in range(0, 20): #print top 20
            print("   {0:2d} : '{1}' occured {2} times".format(j+1, sortedData[j][0], sortedData[j][1]))
        ###
    sc.stop()
```

    Number of CPUs = 1 | Time = 4.4481 s
    Number of CPUs = 2 | Time = 8.0419 s
        1 : 'the' occured 3027 times
        2 : 'and' occured 1910 times
        3 : 'of' occured 1553 times
        4 : 'in' occured 1165 times
        5 : 'are' occured 1031 times
        6 : 'to' occured 962 times
        7 : 'a' occured 769 times
        8 : 'is' occured 622 times
        9 : 'as' occured 560 times
       10 : 'species' occured 558 times
       11 : 'they' occured 370 times
       12 : 'for' occured 362 times
       13 : 'with' occured 352 times
       14 : 'have' occured 344 times
       15 : 'their' occured 326 times
       16 : 'or' occured 306 times
       17 : 'from' occured 269 times
       18 : 'by' occured 244 times
       19 : 'on' occured 230 times
       20 : 'which' occured 214 times


# Exercise 3: Inverted Index + Word Count

In this exercise you are asked to construct inverted index in the following form: (term, the number of doccuments in which the term occurs , sorted list of docIDs]. For instance: [...,("roses", 2, [0, 1]),...] -> term "roses" occurs in two documents: termIDs = 0 and 1. The "get...Collection" methods are slightly modified. Both return: rdd object, list of the names of the documents, and a dictionary (docID -> document name):


```python
def getSmallCollection_EX2(sc, partitions):
    doc1 = "Roses,are red "
    doc2 = "Roses are roses"
    doc3 = "The Sun in red."
    rdd1 = sc.parallelize([doc1, doc2, doc3], partitions)
    docNames = ["doc1", "doc2", "doc3"]
    docIDs = {0: docNames[0], 1: docNames[1], 2: docNames[2]}
    return rdd1, docNames, docIDs
```


```python
def getLargeCollection_EX2(sc, partitions):
    DOCS = sc.wholeTextFiles("./pages/", partitions)
    rdd1 = DOCS.map(lambda x: x[1])
    rdd2 = DOCS.map(lambda x: x[0])
    docNames = rdd2.collect()
    docIDs = [i for i in range(0, len(docNames))]
    return rdd1, docNames, docIDs
```

TODO: do the task and verify the results using the small collection.


```python
def doInvertedIndex(sc, collection, partitions):
    r1 = collection
    r2 = r1.zipWithIndex()
    r3 = r2.map(lambda x: (x[1], tokenizeAndNormalize(x[0])))
    r4 = r3.flatMapValues(f)
    r5 = r4.map(lambda x: (x[1], x[0]))
    r6 = r5.groupByKey().mapValues(list)
    r7 = r6.map(lambda x: (x[0], len(set(x[1])), list(set(x[1]))))
    return r7

def f(x): return x

# sc.stop()
# sc = SparkContext(master, appName="InvertedIndex")
# data, names, ids = getSmallCollection_EX2(sc, 2)
# print(doInvertedIndex(sc, data, 2).collect())
# sc.stop()
```

12) Run the following script and verify the results.


```python
## i = number of nodes (CPUs).
#Why the best time is for 1CPU???
for i in [1,2]:
    master = "local["+str(i)+"]"
    sc = SparkContext(master, appName="InvertedIndex")
    start_time = time.time()
    rdd1, docNames, docIDs = getSmallCollection_EX2(sc, i)
    computedData = doInvertedIndex(sc, rdd1, i)
    rddSort = computedData.sortBy(lambda x: -x[1])
    elapsed = time.time() - start_time
    print("Number of CPUs = %i | Time = %.4f s " % (i, elapsed))  
    ### PRINT HERE
    sortedData = rddSort.collect()
    for i in range(0, 5): #print top 5
        print("   %i : '%s' occured in %i documents" % (i, sortedData[i][0], sortedData[i][1]))
    ###
    sc.stop()
```

    Number of CPUs = 1 | Time = 0.0476 s
       0 : 'roses' occured in 2 documents
       1 : 'are' occured in 2 documents
       2 : 'red' occured in 2 documents
       3 : 'the' occured in 1 documents
       4 : 'sun' occured in 1 documents
    Number of CPUs = 2 | Time = 1.0224 s
       0 : 'roses' occured in 2 documents
       1 : 'are' occured in 2 documents
       2 : 'red' occured in 2 documents
       3 : 'sun' occured in 1 documents
       4 : 'in' occured in 1 documents


12) Run the following script and verify if it is faster for 2 cores. Lastly, compare the obtained results with the results of exercise 2 (word count). Are the rankings corellated?


```python
## i = number of nodes (CPUs).
#Why the best time is for 1CPU???
sc.stop()
for i in [1,2]:
    master = "local["+str(i)+"]"
    sc = SparkContext(master, appName="InvertedIndex")
    start_time = time.time()
    rdd1, docNames, docIDs = getLargeCollection_EX2(sc, i)
    computedData = doInvertedIndex(sc, rdd1, i)
    rddSort = computedData.sortBy(lambda x: -x[1])
    elapsed = time.time() - start_time
    print("Number of CPUs = %i | Time = %.4f s " % (i, elapsed))  
    ### PRINT HERE
    sortedData = rddSort.collect()
    for i in range(0, 20): #print top 20
        print("   %i : '%s' occured in %i documents" % (i, sortedData[i][0], sortedData[i][1]))
    ###
    sc.stop()
```

    Number of CPUs = 1 | Time = 7.8396 s
       0 : 'the' occured in 206 documents
       1 : 'of' occured in 204 documents
       2 : 'and' occured in 199 documents
       3 : 'in' occured in 194 documents
       4 : 'a' occured in 189 documents
       5 : 'are' occured in 189 documents
       6 : 'to' occured in 187 documents
       7 : 'is' occured in 180 documents
       8 : 'species' occured in 179 documents
       9 : 'as' occured in 155 documents
       10 : 'with' occured in 143 documents
       11 : 'for' occured in 141 documents
       12 : 'or' occured in 138 documents
       13 : 'they' occured in 131 documents
       14 : 'from' occured in 128 documents
       15 : 'their' occured in 127 documents
       16 : 'have' occured in 123 documents
       17 : 'family' occured in 116 documents
       18 : 'which' occured in 116 documents
       19 : 'by' occured in 115 documents
    Number of CPUs = 2 | Time = 10.5456 s
       0 : 'the' occured in 206 documents
       1 : 'of' occured in 204 documents
       2 : 'and' occured in 199 documents
       3 : 'in' occured in 194 documents
       4 : 'are' occured in 189 documents
       5 : 'a' occured in 189 documents
       6 : 'to' occured in 187 documents
       7 : 'is' occured in 180 documents
       8 : 'species' occured in 179 documents
       9 : 'as' occured in 155 documents
       10 : 'with' occured in 143 documents
       11 : 'for' occured in 141 documents
       12 : 'or' occured in 138 documents
       13 : 'they' occured in 131 documents
       14 : 'from' occured in 128 documents
       15 : 'their' occured in 127 documents
       16 : 'have' occured in 123 documents
       17 : 'family' occured in 116 documents
       18 : 'which' occured in 116 documents
       19 : 'by' occured in 115 documents
