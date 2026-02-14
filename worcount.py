rdd = spark.sparkContext.textFile("/Users/sreejeshos/MLBD/MLBD_Mapreduce_old/200.txt")
word = rdd.flatMap(lambda line:line.split(" ")).flatMap(lambda line:line.split(" "))
words = word.map(lambda word: (word, 1))

wordCounts = words.reduceByKey(lambda a, b: a + b)
wordCounts.collect()
