# GloVe
An implementation of the Stanford GloVe word embedding algorithm using Spark. Our sample data for this project is taken from a set of Washington State Supreme Court opinions. We are basing the algorithm on the information published in the paper "GloVe: Global Vectors for Word Representation" http://www-nlp.stanford.edu/pubs/glove.pdf.

The goal will be to parallelize the construction of the cooccurence matrix and the computation of the cost function in the GloVe algorithm using the features of spark.
