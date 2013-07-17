-- SANDBOX FOR NLP ALGORITHMS
-- NOT READY FOR PUBLIC CONSUMPTION

REGISTER 'datafu-0.0.10.jar';
REGISTER 'bacon-bits-0.1.0.jar';

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


DEFINE NLP__TFIDF(documents)
RETURNS tfidfs {
    tfs     =   FOREACH $documents GENERATE id, NLP__TextToWordFrequencies(text);
    idfs    =   FOREACH (GROUP tfs ALL) GENERATE NLP__InverseDocumentFrequencies(tfs.word_frequencies);
    $tfidfs =   FOREACH tfs GENERATE
                    id,
                    NLP__TopN(
                        NLP__ElementwiseProduct(idfs.idfs, word_frequencies)
                    );
};
