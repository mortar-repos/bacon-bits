%default INPUT_PATH '/Users/jpacker/data/common_crawl/538/*'
%default MIN_PARAGRAPH_LENGTH 10
%default MIN_WORD_LENGTH 4
%default MIN_TERM_COUNT 2
%default NUM_FEATURES 25

REGISTER 's3n://mortar-prod-sandbox/jpacker/jar/datafu-0.0.9.jar';
DEFINE Quantiles datafu.pig.stats.StreamingQuantile('21');

IMPORT '../macros/nlp.pig';
IMPORT '../macros/stats.pig';
IMPORT '../macros/ml.pig';
IMPORT '../macros/matrix.pig';

pages                   =   NLP__CommonCrawl_LoadPages('$INPUT_PATH');
pages_with_ids, id_map  =   NLP__CommonCrawl_AssignIntegerIds(pages);
texts                   =   NLP__HTMLToText(pages_with_ids, $MIN_PARAGRAPH_LENGTH);

/*
bags_of_ngrams          =   NLP__TextToWords(texts);
tf_idfs                 =   NLP__TFIDF(
                                bags_of_ngrams,
                                'NLP__TermFrequenciesFromWords',
                                id_map,
                                $MIN_WORD_LENGTH,
                                $MIN_WORD_COUNT
                            );
*/

bags_of_ngrams          =   NLP__TextTo2Grams(texts);
tf_idfs                 =   NLP__TFIDF(
                                bags_of_ngrams,
                                'NLP__TermFrequenciesFrom2Grams',
                                id_map,
                                $MIN_WORD_LENGTH,
                                $MIN_TERM_COUNT
                            );

tf_idfs                 =   Stats__QuadraticNormalization(tf_idfs, 'tf_idf', 'id, term');
page_features           =   NLP__TFIDF_To_Features(tf_idfs, $NUM_FEATURES);

similarities            =   ML__CosineSimilarityMatrix(page_features);
visualization           =   Matrix__VisualizeByRow(similarities, id_map, 'DESC', 10);

rmf ../test_output/test;
STORE visualization INTO '../test_output/test' USING PigStorage();
