/*
    Copyright 2013 Mortar Data Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

/*
    This pig script will return some basic information about a MongoDB collection.  Output is:

    1. Field Name.  Embedded fields have their parent's field name prepended to their name.
            Every field that appears in any document in the collection is listed.
    2. Unique value count.  The number of unique values associated with the field.
    3. Example value.  An example value for the field.
    4. Example value type.  The data type of the example value.
    5. Value count.  The number of times the example value appeared for this field in the collection

    Each field is listed up to five times with their five most common example values.
 */

REGISTER '../udfs/python/mongo_util.py' USING streaming_python AS mongo_util;

REGISTER '../udfs/java/datafu-0.0.10.jar' 
REGISTER '../udfs/java/target/bacon-bits-0.1.0.jar';

define Quartile datafu.pig.stats.Quantile('0.0','0.25','0.5','0.75','1.0');

-- data = LOAD 's3n://twitter-gardenhose-mortar/example'
--         USING org.apache.pig.piggybank.storage.JsonLoader();

data = LOAD 's3n://twitter-gardenhose-mortar/example' 
                 USING org.apache.pig.piggybank.storage.JsonLoader(  
                 'coordinates:map[], created_at:chararray, current_user_retweet:map[], entities:map[], favorited:chararray, id_str:chararray, in_reply_to_screen_name:chararray, in_reply_to_status_id_str:chararray, place:map[], possibly_sensitive:chararray, retweet_count:int, source:chararray, text:chararray, truncated:chararray, user:map[], withheld_copyright:chararray, withheld_in_countries:{t:(country:chararray)}, withheld_scope:chararray');

-- data = LOAD 'mongodb://readonly:readonly@ds035147.mongolab.com:35147/twitter.tweets'
--        USING com.mongodb.hadoop.pig.MongoLoader();

--data = LOAD '../example_input/twitter.csv/part-r-00000' USING PigStorage();

limited = LIMIT data 25;

-- Create one row for every field in the document
raw_fields =  FOREACH limited
             GENERATE FLATTEN(com.mortardata.pig.ExtractFields(*)) as (keyname:chararray, type:chararray, val:double, orig:chararray);

-- dump raw_fields;

-- Group the rows by field name and find the number of unique values for each field in the collection
key_groups = GROUP raw_fields BY (keyname);
unique_vals = FOREACH key_groups {
    v = raw_fields.val;
    unique_v = distinct v;
    null_fields = filter v by val is null;
    GENERATE flatten(group)  as keyname:chararray,
             COUNT(unique_v) as num_distinct_vals_count:long, COUNT(v) as num_vals:long, COUNT(null_fields) as num_null:long,
             (double) COUNT(null_fields) / (double) COUNT(v) as percentage_null:double, MIN(unique_v.val) as min_val:double, MAX(unique_v.val) as max_val:double;
}

-- Find the number of times each value occurs for each field
key_val_groups = GROUP raw_fields BY (keyname, type, val, orig);
key_val_groups_with_counts =  FOREACH key_val_groups
                             GENERATE flatten(group),
                                      COUNT($1) as val_count:long;

-- Find the top 5 most common values for each field
key_vals = GROUP key_val_groups_with_counts BY (keyname);
top_5_vals = FOREACH key_vals {
    ordered_vals = ORDER key_val_groups_with_counts BY val_count DESC;
    limited_vals = LIMIT ordered_vals 5;
    GENERATE flatten(limited_vals);
}

-- Join unique vals with top 5 values
join_result = JOIN unique_vals BY keyname,
                   top_5_vals  BY keyname;


-- Clean up columns (remove duplicate keyname field)
result =  FOREACH join_result
         GENERATE unique_vals::keyname as Key,
                  num_distinct_vals_count as NDistinct,
                  num_vals as NVals,
                  num_null as NNull,
                  percentage_null as PctNull,
                  min_val as Min,
                  max_val as Max,
                  max_val - min_val as Range,
                  val as Val,
                  orig as OrigVal,
                  type as Type,
                  val_count as VCount;

-- Sort by field name and number of values
out = ORDER result BY Key, VCount DESC;

STORE out INTO '../example_output/twitter_characterize' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/meta_out;
-- STORE out INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/meta_out'
--          USING PigStorage('\t');