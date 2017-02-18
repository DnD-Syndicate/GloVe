import string
from itertools import chain
from src import context_dictionary
from src.prepare_court_data import import_dataframe
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType
from nltk.tokenize import sent_tokenize, word_tokenize


# import all the opinion files from the archive
opinion_df = import_dataframe(spark, 'opinion')

# save just 1000 rows into a parquet file
spark.createDataFrame(
        opinion_df
        .select('resource_id', 'parsed_text')
        .take(1000)) \
                .write
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
    for sentence in sent_tokenize(doc.replace('\n', ' ').strip())],         # bring the documents in divided into sentences
    ArrayType(ArrayType(StringType())))                                     # declare nested array of strings for Spark
df_words = df_opinions_unparsed.withColumn('sents', token_lists('parsed_text'))

# create a cooccurrence dictionary for each document
udf_contexts = udf(lambda sentences: context(sentences), MapType(StringType(), MapType(StringType(), IntegerType())))
df_word_dicts = df_words.withColumn('cooccurrence_dicts', udf_contexts('sents'))
df_word_dicts.first()

wl_udf = udf(lambda doc: chain(doc), ArrayType(StringType()))
df_words.withColumn('words', wl_udf(df_words['sents'])).select('words').first()

