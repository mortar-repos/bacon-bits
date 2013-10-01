package com.mortardata.pig.recsys;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TimeSeriesItemLinker extends EvalFunc<DataBag> implements Accumulator<DataBag> {
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();
    private static final Log log = LogFactory.getLog(TimeSeriesItemLinker.class);

    private int windowSize;
    private Deque<Tuple> window = null;
    private DataBag output = null;

    public TimeSeriesItemLinker(String windowSize) {
        this.windowSize = Integer.parseInt(windowSize);
    }

    /**
       Given a tuple schema, appends scope to each field alias
     */
    public List<Schema.FieldSchema> appendScope(String scope, Schema s) throws FrontendException {
        List<Schema.FieldSchema> fields = s.getFields();
        List<Schema.FieldSchema> resultFields = new ArrayList<Schema.FieldSchema>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Schema.FieldSchema resultField = new Schema.FieldSchema(fields.get(i));
            resultField.alias = scope + "::" + resultField.alias;
            resultFields.add(resultField);
        }
        return resultFields;            
    }
    
    public Schema outputSchema(Schema input) {
        try {

            Schema bagTupleSchema = input.getField(0).schema;
            Schema tupleSchema = bagTupleSchema.getField(0).schema;

            // Now attach prefixes
            List<Schema.FieldSchema> first = appendScope("first", tupleSchema);
            List<Schema.FieldSchema> second = appendScope("second", tupleSchema);
            for (Schema.FieldSchema sec : second) {
                first.add(sec);
            }
            Schema merged = new Schema(new Schema.FieldSchema(null, new Schema(first), DataType.TUPLE));
            Schema.FieldSchema outputBagSchema = new Schema.FieldSchema("item_links", merged, DataType.BAG);

            return new Schema(outputBagSchema);
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public void accumulate(Tuple input) throws IOException {
        DataBag bag = (DataBag) input.get(0);
        if (window == null) {
            window = new ArrayDeque<Tuple>();
        }
        if (output == null) {
            output = bf.newDefaultBag();
        }

        for (Tuple u : bag) {
            for (Tuple v : window) {
                output.add(unionTuples(u,v));
                output.add(unionTuples(v,u));
            }

            window.addLast(u);
            if (window.size() > windowSize) {
                window.removeFirst();
            }
        }
    }

    /**
       Creates a new tuple containing all the fields from
       both x and y in order
     */
    public Tuple unionTuples(Tuple x, Tuple y) throws IOException {
        Tuple result = tf.newTuple(x.size() + y.size());
        int i = 0;
        for (; i < x.size(); i++) {
            result.set(i, x.get(i));
        }

        for (int j = 0; j < y.size(); j++,i++) {
            result.set(i, y.get(j));
        }
        
        return result;
    }
    
    public void cleanup() {
        window = new ArrayDeque<Tuple>();
        output = bf.newDefaultBag();
    }

    public DataBag getValue() {
        return output;
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        accumulate(input);
        DataBag result = getValue();
        cleanup();
        return result;
    }
}
