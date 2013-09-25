-- SANDBOX FOR NLP ALGORITHMS
-- NOT READY FOR PUBLIC CONSUMPTION

REGISTER 'datafu-0.0.10.jar';
REGISTER 'bacon-bits-0.1.0.jar';
REGISTER 'lucene-core-4.4.0.jar';
REGISTER 'lucene-analyzers-common-4.4.0.jar';

DEFINE NLP__EnumerateFromOne           datafu.pig.bags.Enumerate('1');

DEFINE NLP__HTMLToText                 com.mortardata.pig.nlp.HTMLToText();
DEFINE NLP__TextToWordFrequencies      com.mortardata.pig.nlp.TextToWordFrequencies('$NLP__MIN_WORD_LENGTH');
Define NLP__InverseDocumentFrequencies com.mortardata.pig.nlp.InverseDocumentFrequencies();

DEFINE NLP__ElementwiseProduct         com.mortardata.pig.collections.ElementwiseProduct();
DEFINE NLP__TopN                       com.mortardata.pig.collections.TopN('$NLP__TFIDF_NUM_FEATURES');
DEFINE NLP__FromPigCollectionToBag     com.mortardata.pig.collections.FromPigCollectionToBag();

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

DEFINE NLP__CommonCrawl_AssignIntegerIds(pages, key_field)
RETURNS pages_with_ids, id_map {
    keys            =   FOREACH $pages GENERATE $key_field;
    keys            =   DISTINCT keys;
    id_map          =   FOREACH (GROUP keys ALL) GENERATE
                            FLATTEN(NLP__EnumerateFromOne(keys))
                            AS (name, id);
    $id_map         =   FOREACH id_map GENERATE id, name;
    $pages_with_ids =   FOREACH (JOIN $pages BY $key_field, $id_map BY name) GENERATE
                            id   AS id,
                            html AS html;
};

----------------------------------------------------------------------------------------------------

DEFINE NLP__HTMLToText(documents)
RETURNS text {
    -- extract text from <p> tags (does some basic cleansing)
    $text   =   FOREACH $documents GENERATE id, NLP__HTMLToText(html);
};

----------------------------------------------------------------------------------------------------


-- DEFINE NLP__TFIDF(documents)
-- RETURNS tfidfs {
--     tfs     =   FOREACH $documents GENERATE id, NLP__TextToWordFrequencies(text);
--     idfs    =   FOREACH (GROUP tfs ALL) GENERATE NLP__InverseDocumentFrequencies(tfs.word_frequencies);
--     $tfidfs =   FOREACH tfs GENERATE
--                     id,
--                     NLP__TopN(
--                         NLP__ElementwiseProduct(idfs.idfs, word_frequencies)
--                     );
-- };


/*
 * Given a set of documents, returns tf-idf feature vectors for those documents.
 *
 * documents:   { id, text:chararray }  Document set.
 * minWordSize: int                     Words with less characters than this will not be considered
 * minGramSize: int                     Smallest number of words allowable in an n-gram
 * maxGramSize: int                     Largest number of words allowable in an n-gram
 * maxFeatures: int                     Maximum number of features to return per document        
 * ==>
 * vectors: { id, features:{(token:chararray, weight:float)} } Ordered by weight desc.
 */
define NLP__TFIDF(documents, minWordSize, minGramSize, maxGramSize, maxFeatures) returns vectors {

  define NGramTokenize com.mortardata.pig.nlp.NGramTokenize('$minGramSize','$maxGramSize', '$minWordSize');
  
  --
  -- Get corpus size first
  --
  uniq     = distinct (foreach $documents generate id);
  num_docs = foreach (group uniq all) generate COUNT(uniq) as N; -- ugh.
  
  --
  -- Tokenize the documents
  --
  tokenized = foreach $documents generate
                id,
                flatten(NGramTokenize(text)) as (token:chararray);

  --
  -- Next, get raw term frequencies. Combiners will be made use of here to reduce some of the
  -- token explosion
  --
  term_freqs = foreach (group tokenized by (id, token)) generate
                 flatten(group)   as (id, token),
                 COUNT(tokenized) as term_freq;

  --
  -- Now, compute the 'augmented' frequency to prevent bias toward long docs
  --
  max_term_freqs = foreach (group term_freqs by id) generate
                     flatten(term_freqs)       as (id, token, term_freq),
                     MAX(term_freqs.term_freq) as max_term_freq;

  aug_term_freqs = foreach max_term_freqs {
                     -- see: http://www.cs.odu.edu/~jbollen/IR04/readings/article1-29-03.pdf
                     aug_freq = 0.5f + (0.5f * term_freq)/max_term_freq;
                     generate
                       id       as id,
                       token    as token,
                       aug_freq as term_freq;
                    };
  
  --
  -- Next, get document frequency; how many documents does a term appear in.
  --
  doc_freqs = foreach (group aug_term_freqs by token) {
                raw_doc_freq = COUNT(aug_term_freqs);
                idf          = LOG((float)num_docs.N/(float)raw_doc_freq);
                generate
                  flatten(aug_term_freqs) as (id, token, term_freq),
                  idf                     as idf;
              };
  
  --
  -- Finally, compute tf-idf
  --
  weights = foreach doc_freqs generate
              id            as id,
              token         as token,
              term_freq*idf as weight;

  $vectors = foreach (group weights by id) {
               ordered = order weights by weight desc;
               top_N   = limit ordered $maxFeatures; -- use this instead of top to maintain ordering
               generate 
                 group                as id,
                 top_N.(token,weight) as features;
             };
};
