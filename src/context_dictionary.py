from itertools import chain

def context(document, window_size=10):
    """
    INPUT
    ------
    document : list
        A list of lists where each list consists of the words which constitute a
        sentence.
    window_size : int
        The size of the symmetric context window to each side of the word.


    OUTPUT
    -------
    context_dictionary : dict
        A dictionary where the key is the context word and the value
        is a Counter dictionary of the words which show up in the context window.
        We will assume a default window size of 5 which will be truncated at the
        beginning and end of the sentence.  For example, {'dog': {'cat': 2, 'mouse': 4}}.
    """
    context_dictionary = {}
    for sentence in document:
        for idx, word in enumerate(sentence):
            left_window = sentence[max(0, idx - window_size // 2): idx]
            right_window = sentence[idx + 1: idx + window_size // 2]
            word_context = {}
            for word in chain(left_window, right_window):
                if word_context.get(word) == None:
                    word_context[word] = 1
                else:
                    word_context[word] += 1
            if context_dictionary.get(word) == None:
                context_dictionary[word] = word_context
            else:
                context_dictionary[word].update(word_context)
    return context_dictionary

def corpus_cooccurence(document_dicts):
    """
    INPUT
    ------
    document_dicts : list
        A list of document context dictionaries.

    OUTPUT
    -------
    corpus_dictionary : dict
        A dictionary encoding the cooccurence matrix of the entire corpus.
    """
    corpus_dictionary = {}
    for dictionary in document_dicts:
        for key in dictionary.keys():
            corpus_dictionary[key].update(dictionary[key])
    return corpus_dictionary
