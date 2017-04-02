import string
from itertools import chain
from src import context_dictionary
from src.prepare_court_data import import_dataframe
from pyspark.sql.functions import udf, explode, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType
from nltk.tokenize import sent_tokenize, word_tokenize


# import all the opinion files from the archive
opinion_df = import_dataframe(spark, 'opinion')

# save just 1000 rows into a parquet file
spark.createDataFrame(
        opinion_df
        .select('resource_id', 'parsed_text')
        .take(1000)) \
                .write \
                .save('data/wash_state_1000_opinions.parquet', format='parquet', mode='overwrite')

# load parquet file into Spark
df_opinions_unparsed = spark.read.load('data/wash_state_1000_opinions.parquet')

# one time only download required for sent_tokenize
import nltk; nltk.download('punkt')

# use a list generator in a spark UDF to first separate into sentences, and then word tokens
# this is important because the GloVe implementation will find relationship ratios based on colocation within sentences.
token_lists = udf(lambda doc: [
    word_tokenize(                                              # NLTK word tokenizer is smarter (can separate contractions)
        sentence.translate(                                     # translate can change one character into another
            str.maketrans(string.punctuation, ' '*len(string.punctuation))  # a translator that changes punctuation within words
            )
        ) 
    for sentence in sent_tokenize(doc.replace('\n', ' ').strip().lower())],         # bring the documents in divided into sentences
    ArrayType(ArrayType(StringType())))                                     # declare nested array of strings for Spark
df_words = df_opinions_unparsed.withColumn('sents', token_lists('parsed_text'))
df_words.persist()

# Vocabulary list of distinct terms
vocab_list = df_words \
        .withColumn('lists', explode('sents')) \
        .withColumn('words', explode('lists')) \
        .select('words') \
        .distinct() \
        .withColumn('id', monotonically_increasing_id())
vocab_list.persist()

# build the context dictionary for each word
udf_contexts = udf(lambda doc: context_dictionary.context(doc), MapType(StringType(), MapType(StringType(), IntegerType())))
df_word_dicts = df_words.withColumn('cooccurrence_dicts', udf_contexts('sents'))

