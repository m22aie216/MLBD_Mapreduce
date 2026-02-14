# MLBD_Mapreduce
Load Dataset into Spark
======================
Schema:
=======
file_name (string): e.g., "200.txt"

text (string): raw text content

Metadata Extraction
===================
regular expressions to extract metadata from the text column.

Regex Patterns
===============
Title: (?<=Title:\s)(.*)

Release Date: (?<=Release Date:\s)(.*)

Language: (?<=Language:\s)(.*)

Encoding: (?<=Character set encoding:\s)(.*)

Inconsistencies: Some books may lack metadata or use different formatting.

Missing values: Regex may fail if headers are malformed.

Analysis
========
Books per year: Extract year from release_date and group by year.

Most common language: groupBy("language").count().orderBy("count", descending=True)

Average title length: books_meta.select(F.avg(F.length("title")))

TF‑IDF and Book Similarity
=========================
Preprocessing
============
Remove Gutenberg headers/footers (regex: \*\*\* START.*\*\*\*|\*\*\* END.*\*\*\*)

Lowercase, remove punctuation, tokenize, remove stopwords.

TF-IDF Explanation
==================
TF (Term Frequency): Frequency of a word in a document.

IDF (Inverse Document Frequency): Penalizes common words across all documents.

TF-IDF: Highlights words that are frequent in one book but rare across others.


Cosine Similarity

================
Measures angle between vectors.

Appropriate because it normalizes length differences between books.
Author Influence Network
Preprocessing
==============
Extract author and release_date with regex:

Author: (?<=Author:\s)(.*)

Release Date: same as before.

Influence Definition
Edge (author1, author2) if release dates are within X years (e.g., 5 years).

Analysis
==========
In-degree: Count of incoming edges.

Out-degree: Count of outgoing edges.

Identify top 5 authors with highest influence.
Spark Solution:
===============
Use broadcast joins for smaller vectors.

Apply LSH (Locality Sensitive Hashing) to approximate nearest neighbors.

Partition data across clusters to parallelize similarity computation.
