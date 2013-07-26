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

    public Schema outputSchema(Schema input) {
        try {
            List<Schema.FieldSchema> outputTupleFields = new ArrayList<Schema.FieldSchema>();
            outputTupleFields.add(new Schema.FieldSchema("row", DataType.INTEGER));
            outputTupleFields.add(new Schema.FieldSchema("col", DataType.INTEGER));
            outputTupleFields.add(new Schema.FieldSchema("val", DataType.FLOAT));
            
            return new Schema(new Schema.FieldSchema(
                "b", new Schema(new Schema.FieldSchema(
                    "t", new Schema(outputTupleFields), DataType.TUPLE
                )), DataType.BAG
            ));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    // input schema: ({(item: int, score: float, timestamp: chararray)})
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
                // TODO: weight link inversely proportional to delta-t, using a logistic scale
                // You can access timestamp with (String) u.get(2) or (String) v.get(2)

                Tuple w = tf.newTuple(3);
                w.set(0, u.get(0));
                w.set(1, v.get(0));
                w.set(2, Math.min((Float) u.get(1), (Float) v.get(1)));
                output.add(w);

                w = tf.newTuple(3);
                w.set(0, v.get(0));
                w.set(1, u.get(0));
                w.set(2, Math.min((Float) u.get(1), (Float) v.get(1)));
                output.add(w);
            }

            window.addLast(u);
            if (window.size() > windowSize) {
                window.removeFirst();
            }
        }
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