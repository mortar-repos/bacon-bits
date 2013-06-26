REGISTER 'datafu-0.0.10.jar';
REGISTER 'nlp_python.py' USING streaming_python AS nlp_python_udfs;
REGISTER 'nlp_jython.py' USING jython AS nlp_jython_udfs;

DEFINE EnumerateFromOne datafu.pig.bags.Enumerate('1');

----------------------------------------------------------------------------------------------------

DEFINE NLP__CommonCrawl_LoadPages(input_path)
RETURNS pages {
    $pages  =   LOAD '$input_path' 
                USING org.commoncrawl.pig.ArcLoader()
                AS (
                    date:        chararray,
                    length:      long,
                    type:        chararray, 
                    status_code: int,
                    ip_address:  chararray, 
                    url:         chararray, 
                    html:        chararray
                );
};

DEFINE NLP__CommonCrawl_AssignIntegerIds(pages)
RETURNS pages_with_ids, id_map {
    urls                    =   FOREACH $pages GENERATE url;
    id_map                  =   FOREACH (GROUP urls ALL) GENERATE
                                    FLATTEN(EnumerateFromOne(urls))
                                    AS (url, id);
    $id_map                 =   FOREACH id_map GENERATE id, url;

    $pages_with_ids         =   FOREACH (JOIN $pages BY url, $id_map BY url) GENERATE
                                    id          AS id,
                                    date        AS date,
                                    length      AS length,
                                    type        AS type,
                                    status_code AS status_code,
                                    ip_address  AS ip_address,
                                    html        AS html;
};

----------------------------------------------------------------------------------------------------

DEFINE NLP__HTMLToText(pages, min_paragraph_length)
RETURNS paragraphs {
    -- extract text from <p> tags (does some basic cleansing)
    $paragraphs             =   FOREACH $pages GENERATE
                                    id,
                                    nlp_jython_udfs.html_to_text(html, $min_paragraph_length);
};

----------------------------------------------------------------------------------------------------

DEFINE NLP__TextTo2Grams(texts)
RETURNS bags_of_ngrams {
    $bags_of_ngrams         =   FOREACH $texts GENERATE
                                    id, nlp_jython_udfs.text_to_2grams(text);
};

----------------------------------------------------------------------------------------------------

DEFINE NLP__TermFrequenciesFromWords(bags_of_words, min_word_length, min_word_count) 
RETURNS tfs {
    tfs_tmp                 =   FOREACH $bags_of_words GENERATE
                                    id,
                                    FLATTEN(nlp_python_udfs.filter_and_count_words(
                                        words, $min_word_length, $min_word_count
                                    )) AS (term, tf);
    $tfs                    =   FOREACH (GROUP tfs_tmp BY (id, term)) GENERATE
                                    FLATTEN(group) AS (id, term),
                                    SUM(tfs_tmp.tf) AS tf;
};

DEFINE NLP__TermFrequenciesFrom2Grams(bags_of_ngrams, min_word_length, min_term_count)
RETURNS tfs {
    tfs_tmp                 =   FOREACH $bags_of_ngrams GENERATE
                                    id,
                                    FLATTEN(nlp_python_udfs.filter_and_count_2grams(
                                        ngrams, $min_word_length, $min_term_count
                                    )) AS (term, tf);
    $tfs                    =   FOREACH (GROUP tfs_tmp BY (id, term)) GENERATE
                                    FLATTEN(group) AS (id, term),
                                    SUM(tfs_tmp.tf) AS tf;
};

DEFINE NLP__InverseDocumentFrequencies(ids, tfs)
RETURNS idfs {
    idf_numerator           =   FOREACH (GROUP $ids ALL) GENERATE COUNT($ids) AS num_documents;
    idf_denominators        =   FOREACH (GROUP $tfs BY term) GENERATE
                                    group AS term, COUNT($tfs) AS documents_with_term;
    $idfs                   =   FOREACH idf_denominators GENERATE
                                    term,
                                    LOG(1.0 + (double) idf_numerator.num_documents / (double) documents_with_term)
                                    AS idf;
};

DEFINE NLP__TFIDF(bags_of_words, tf_macro, id_map, min_word_length, min_word_count)
RETURNS tf_idfs {
    tfs                     =   $tf_macro($bags_of_words, $min_word_length, $min_word_count);
    idfs                    =   NLP__InverseDocumentFrequencies($id_map, tfs);
    $tf_idfs                =   FOREACH (JOIN tfs BY term, idfs BY term) GENERATE
                                    id AS id, tfs::term AS term,
                                    tfs::tf * idfs::idf AS tf_idf;
};

DEFINE NLP__TFIDF_To_Features(tf_idfs, num_features)
RETURNS features {
    $features               =   FOREACH (GROUP $tf_idfs BY id) {
                                    sorted  =   ORDER $tf_idfs BY tf_idf DESC;
                                    top     =   LIMIT sorted $num_features;
                                    GENERATE FLATTEN(top) AS (id, feature, weight);
                                }
};
