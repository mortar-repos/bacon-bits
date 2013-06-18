# -*- coding: utf-8 -*-

import re

# not robust, but faster than a real HTML parser
cleanse_with_space   = re.compile("\r|\n|&.*?;")
cleanse_with_nothing = re.compile(u"[!@#$*().,;:\"“”]|['’]s", re.UNICODE)
paragraph_pattern    = re.compile("<p.*?>(.*?)</p>")
tag_pattern          = re.compile("<.*?>")
whitespace_pattern   = re.compile("\\s+")

@outputSchema("text: {t: (word: chararray)}")
def html_to_text(html, min_paragraph_length):
    cleansed   = re.sub(cleanse_with_nothing, "", re.sub(cleanse_with_space, " ", html.lower()))
    paragraphs = [re.split(whitespace_pattern, re.sub(tag_pattern, "", ptext).strip())
                  for ptext in paragraph_pattern.findall(cleansed)]
    return [(word,) for p in paragraphs if len(p) >= min_paragraph_length for word in p]

@outputSchema("ngrams: {t: (w1: chararray, w2: chararray)}")
def text_to_2grams(bag):
    words = [t[0] for t in bag]
    return zip(words, words[1:])
