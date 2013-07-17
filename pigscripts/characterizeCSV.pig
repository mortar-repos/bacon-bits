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
    This pig script will return some basic information about some tabular data (CSV).  Output is:

    1. Field Name.  Embedded fields have their parent's field name prepended to their name.
            Every field that appears in any document in the collection is listed.
    2. Unique value count.  The number of unique values associated with the field.
    3. Example value.  An example value for the field.
    4. Example value type.  The data type of the example value.
    5. Value count.  The number of times the example value appeared for this field in the collection

    Each field is listed up to five times with their five most common example values.
 */


--REGISTER '../udfs/python/csv_util.py' USING streaming_python AS mongo_util;

REGISTER '../udfs/java/target/bacon-bits-0.1.0.jar';

-- load some bogus CSV
data = LOAD '../example_input/bogusmaps.csv' 
       USING PigStorage() as (f1:int, f2:chararray, f3:int, f4:int, f5:map[map[]], f6:bag{}, f7:tuple());

dump data;

raw_fields = FOREACH data
             GENERATE flatten(com.mortardata.pig.ExtractFields(*))
             as (keyname:chararray, type:chararray, val:chararray);

dump raw_fields;

-- key_groups = GROUP raw_fields BY (keyname);
-- unique_vals = FOREACH key_groups {
--     v = raw_fields.val;
--     unique_v = distinct v;
--     null_fields = filter v by val is null;
--     GENERATE flatten(group)  as keyname:chararray,
--              COUNT(unique_v) as num_distinct_vals_count:long, COUNT(v) as num_vals:long, COUNT(null_fields) as num_null:long,
--              (double) COUNT(null_fields) / (double) COUNT(v) as percentage_null:double, MIN(unique_v.val) as min_val, MAX(unique_v.val) as max_val;
-- }

-- -- Find the number of times each value occurs for each field
-- key_val_groups = GROUP raw_fields BY (keyname, type, val);
-- key_val_groups_with_counts =  FOREACH key_val_groups
--                              GENERATE flatten(group),
--                                       COUNT($1) as val_count:long;

-- -- Find the top 5 most common values for each field
-- key_vals = GROUP key_val_groups_with_counts BY (keyname);
-- top_5_vals = FOREACH key_vals {
--     ordered_vals = ORDER key_val_groups_with_counts BY val_count DESC;
--     limited_vals = LIMIT ordered_vals 5;
--     GENERATE flatten(limited_vals);
-- }

-- -- Join unique vals with top 5 values
-- join_result = JOIN unique_vals BY keyname,
--                    top_5_vals  BY keyname;

-- -- Clean up columns (remove duplicate keyname field)
-- result =  FOREACH join_result
--          GENERATE unique_vals::keyname,
--                   num_distinct_vals_count,
--                   num_vals,
--                   num_null,
--                   percentage_null,
--                   min_val,
--                   max_val,
--                   val,
--                   type,
--                   val_count;

-- -- Sort by field name and number of values
-- out = ORDER result BY unique_vals::keyname, val_count DESC;

-- -- rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/meta_out;
-- STORE out INTO '../example_output/bogusstats'
--          USING PigStorage('\t');
