package com.mortardata.pig.nlp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 'http://techcrunch.com/2013/02/13/melodrama'
 *  --> ('techcrunch.com', '2013', '02', '13', 'melodrama')
 *
 * 'http://allthingsd.com/20130213/business-money-yay'
 *  --> ('allthingsd.com', '2013', '02', '13', 'business-money-yay')
 */
public class ArticleInfoFromURL extends EvalFunc<Tuple> {
    private Pattern splitDatePat;
    private Pattern unifiedDatePat;
    private static final TupleFactory tf = TupleFactory.getInstance();

    public ArticleInfoFromURL() {
        splitDatePat   = Pattern.compile("http(?:s)?://(.*)/(\\d{4})/(\\d{2})/(\\d{2})/(.*?)/.*");
        unifiedDatePat = Pattern.compile("http(?:s)?://(.*)/(\\d{8})/(.*?)/.*");
    }

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>();
            tupleFields.add(new Schema.FieldSchema("article_domain", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("article_year", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("article_month_of_year", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("article_day_of_month", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("article_name", DataType.CHARARRAY));

            return new Schema(
                new Schema.FieldSchema("article_info", new Schema(tupleFields), DataType.TUPLE)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public Tuple exec(Tuple input) throws IOException {
        String url = (String) input.get(0);
        
        Matcher splitDateMatcher = splitDatePat.matcher(url);
        if (splitDateMatcher.find()) {
            Tuple t = tf.newTuple(5);
            t.set(0, splitDateMatcher.group(1));
            t.set(1, splitDateMatcher.group(2));
            t.set(2, splitDateMatcher.group(3));
            t.set(3, splitDateMatcher.group(4));
            t.set(4, splitDateMatcher.group(5));
            return t;
        }

        Matcher unifiedDateMatcher = unifiedDatePat.matcher(url);
        if (unifiedDateMatcher.find()) {
            String dateStr = unifiedDateMatcher.group(2);
            Tuple t = tf.newTuple(3);
            t.set(0, unifiedDateMatcher.group(1));
            t.set(1, dateStr.substring(0, 4));
            t.set(2, dateStr.substring(4, 6));
            t.set(3, dateStr.substring(6, 8));
            t.set(4, unifiedDateMatcher.group(3));
            return t;
        }

        return null;
    }
}
