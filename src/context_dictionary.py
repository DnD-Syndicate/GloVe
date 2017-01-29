from collections import Counter
from collections import defaultdict
from itertools import chain

def context(sentence):
    """
    INPUT:
    - sentence:  list, A list of words which represents the sentence.

    OUTPUT:
    - context_dictionary:  dict, A dictionary where the key is the context word and
    the value is a Counter dictionary of the words which show up in the context window.
    We will assume a default window size of 5 which will be truncated at the beginning
    and end of the sentence.  For example, {'dog': {'cat': 2, 'mouse': 4}}
    """
    context_dictionary = defaultdict(Counter)
    for idx, word in enumerate(sentence):
        left_window = sentence[max(0, idx - 5): idx]
        right_window = sentence[idx + 1: idx + 6]
        context_dictionary[word].update(chain(left_window, right_window))
    return context_dictionary
