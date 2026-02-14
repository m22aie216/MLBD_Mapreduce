#Load Dataset into Spark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GutenbergAnalysis").getOrCreate()

# Load text files into DataFrame
books_df = spark.read.text("hdfs:///user/iitj/*.txt") \
    .withColumnRenamed("value", "text") \
    .withColumn("file_name", F.input_file_name())


#Metadata Extraction

import pyspark.sql.functions as F

books_meta = books_df \
    .withColumn("title", F.regexp_extract("text", "(?<=Title:\\s)(.*)", 1)) \
    .withColumn("release_date", F.regexp_extract("text", "(?<=Release Date:\\s)(.*)", 1)) \
    .withColumn("language", F.regexp_extract("text", "(?<=Language:\\s)(.*)", 1)) \
    .withColumn("encoding", F.regexp_extract("text", "(?<=Character set encoding:\\s)(.*)", 1))


# TF‑IDF and Book Similarity

from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF

tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="raw_features", numFeatures=20000)
idf = IDF(inputCol="raw_features", outputCol="features")


# Find Top 5 Similar Books

from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
import numpy as np

def cosine_similarity(v1, v2):
    return float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))

# Compute similarities for "10.txt"
target_vector = books_tfidf.filter(F.col("file_name")=="10.txt").select("features").first()[0]
similarities = books_tfidf.rdd.map(lambda row: (row.file_name, cosine_similarity(target_vector.toArray(), row.features.toArray())))
top5 = similarities.takeOrdered(5, key=lambda x: -x[1])


# Author Influence Network

from pyspark.sql import Row

# Example: build edges
edges = books_meta.rdd.cartesian(books_meta.rdd) \
    .filter(lambda x: abs(int(x[0].release_year) - int(x[1].release_year)) <= 5) \
    .map(lambda x: (x[0].author, x[1].author))



