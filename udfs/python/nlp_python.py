import re
from collections import Counter
from pig_util import outputSchema

invalid_character_pattern = re.compile("[^a-z']")

def is_valid_word(word, min_word_length):
    return len(word) >= min_word_length and not bool(invalid_character_pattern.search(word))

@outputSchema("term_counts: {t: (term: chararray, count: int)}")
def filter_and_count_words(bag_of_words, min_word_length, min_word_count):
    return  [t for t in Counter(
                [t[0] for t in bag_of_words if is_valid_word(t[0], min_word_length)]
            ).items() if t[1] >= min_word_count]

@outputSchema("term_counts: {t: (term: (w1: chararray, w2: chararray), count: int)}")
def filter_and_count_2grams(bag_of_ngrams, min_word_length, min_term_count):
    return  [t for t in Counter(
                [t for t in bag_of_ngrams
                   if (is_valid_word(t[0], min_word_length) and is_valid_word(t[1], min_word_length))]
             ).items() if t[1] >= min_term_count]
