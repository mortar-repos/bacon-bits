%default INPUT_PATH  '../../../data/common_crawl/538/*'
%default OUTPUT_PATH '../example_output/538_tfidf'

-- should set NLP__MIN_WORD_LENGTH and NLP__TFIDF_NUM_FEATURES,
-- but defaults aren't being passed to macro define statements properly

REGISTER 'bacon-bits-0.1.0.jar';
REGISTER 'datafu-0.0.10.jar';

IMPORT 'nlp.pig';

pages       =   NLP__CommonCrawl_LoadPages('$INPUT_PATH');
pages       =   FOREACH pages GENERATE
                    FLATTEN(com.mortardata.pig.nlp.ArticleInfoFromURL(url)) AS (
                        article_domain,
                        article_year, article_month_of_year, article_day_of_month,
                        article_name),
                    html;
pages       =   FILTER pages BY (
                    article_domain        IS NOT NULL AND
                    article_year          IS NOT NULL AND
                    article_month_of_year IS NOT NULL AND
                    article_day_of_month  IS NOT NULL AND
                    article_name          IS NOT NULL
                );

pages_with_ids, id_map  =   NLP__CommonCrawl_AssignIntegerIds(pages, 'article_name');

documents   =   NLP__HTMLToText(pages_with_ids);
tfidfs      =   NLP__TFIDF(documents);
tfidfs      =   FOREACH tfidfs GENERATE id, com.mortardata.pig.collections.FromPigCollectionToBag($1);
tfidfs      =   ORDER tfidfs BY id ASC;


rmf $OUTPUT_PATH/id_map;
rmf $OUTPUT_PATH/tfidf;

STORE id_map INTO '$OUTPUT_PATH/id_map' USING PigStorage();
STORE tfidfs INTO '$OUTPUT_PATH/tfidf'  USING PigStorage();
