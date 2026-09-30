from pyspark.sql import SparkSession
import random


# Start the Spark application and connect to the cluster.
spark = SparkSession.builder.appName("SentenceGenerator").getOrCreate()
sc = spark.sparkContext

# Use the same seed so I can reproduce the generated sentences if needed.
random.seed(650)

# Create random sentences from the assignment word list.
words = [
    "apple",
    "banana",
    "cherry",
    "date",
    "elderberry",
    "fig",
    "grape",
    "honeydew",
]

num_sentences = 1000
sentences = [
    " ".join(random.sample(words, random.randint(1, 6))) + "."
    for _ in range(num_sentences)
]

# Parallelize the sentences so Spark can transform the partitions.
sentences_rdd = sc.parallelize(sentences)


# Count the words, reverse their order, and convert them to uppercase.
def transform_sentence(sentence):
    sentence_words = sentence.rstrip(".").split()
    reversed_words = " -> ".join(
        word.upper() for word in reversed(sentence_words)
    )
    return f"WORDS={len(sentence_words)} | {reversed_words}."


transformed = sentences_rdd.map(transform_sentence)

# Save the distributed output to HDFS for validation.
output_path = "hdfs:///tmp/week4_output"
transformed.saveAsTextFile(output_path)

spark.stop()
