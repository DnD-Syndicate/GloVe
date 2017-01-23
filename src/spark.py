from src.prepare_court_data import import_dataframe


# import all the opinion files from the archive
opinion_df = import_dataframe(spark, 'opinion')

# save just 1000 rows into a parquet file
spark.createDataFrame(opinion_df.select('resource_id', 'parsed_text').take(1000)).write.save('data/wash_state_1000_opinions.parquet', format='parquet', mode='overwrite')

# load parquet file into Spark
df = spark.read.load('data/wash_state_1000_opinions.parquet')

# split each document into sentences
sent_tokens = udf(lambda doc: sent_tokenize(doc), ArrayType(StringType()))
df_sentences = df.withColumn('sents', sent_tokens('parsed_text'))

