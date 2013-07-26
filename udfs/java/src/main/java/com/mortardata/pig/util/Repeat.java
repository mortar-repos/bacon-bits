package com.mortardata.pig.util;

import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.Algebraic;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/*
 * Generate K enumerated copies of each tuple in a bag.
 *
 * Ex:
 *
 * ids_repeated =   FOREACH (GROUP ids ALL) GENERATE
 *                      FLATTEN(Repeat(ids, 5))
 *                      AS (id, bin);
 *
 */
public class Repeat extends EvalFunc<DataBag> implements Algebraic, Accumulator<DataBag> {

    private static TupleFactory tf = TupleFactory.getInstance();
    private static BagFactory bf = BagFactory.getInstance();

    private DataBag outputBag;

    public Repeat() {
        outputBag = bf.newDefaultBag();
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema bagSchema   = input.getField(0).schema;
            Schema tupleSchema = bagSchema.getField(0).schema;
            List<Schema.FieldSchema> fields = tupleSchema.getFields();
            fields.add(new Schema.FieldSchema("bin", DataType.INTEGER));

            return new Schema(
                new Schema.FieldSchema("repeated_tuples",
                    new Schema(
                        new Schema.FieldSchema("t",
                            new Schema(fields),
                        DataType.TUPLE)
                    ),
                DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public DataBag exec(Tuple input) throws IOException {
        accumulate(input);
        DataBag outputBag = getValue();
        cleanup();
        return outputBag;
    }

    public void accumulate(Tuple input) throws IOException {
        DataBag bag = (DataBag) input.get(0);
        int k = ((Integer) input.get(1)).intValue();

        for (Tuple t : bag) {
            for (int i = 1; i < k + 1; i++) {
                List<Object> fields = t.getAll();
                fields.add(new Integer(i));
                outputBag.add(tf.newTupleNoCopy(fields));
            }
        }
    }

    public DataBag getValue() {
        return outputBag;
    }

    public void cleanup() {
        outputBag = bf.newDefaultBag();
    }

    public String getInitial()  { return Initial.class.getName();      }
    public String getIntermed() { return Intermediate.class.getName(); }
    public String getFinal()    { return Final.class.getName();        }

    static public class Initial extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            DataBag inBag  = (DataBag) input.get(0);
            int k = ((Integer) input.get(1)).intValue();

            DataBag outBag = bf.newDefaultBag();
            for (Tuple t : inBag) {
                for (int i = 1; i < k + 1; i++) {
                    Tuple t2 = tf.newTuple(t.size() + 1);
                    for (int j = 0; j < t.size(); j++) {
                        t2.set(j, t.get(j));
                    }
                    t2.set(t.size(), i);
                    outBag.add(t2);
                }
            }
            return tf.newTuple(outBag);
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            DataBag inBag  = (DataBag) input.get(0);
            DataBag outBag = bf.newDefaultBag();
            for (Tuple t : inBag) {
                DataBag nestedBag = (DataBag) t.get(0);
                for (Tuple t2 : nestedBag) {
                    outBag.add(t2);
                }
            }
            return tf.newTuple(outBag);
        }
    }

    static public class Final extends EvalFunc<DataBag> {
        public DataBag exec(Tuple input) throws IOException {
            DataBag inBag  = (DataBag) input.get(0);
            DataBag outBag = bf.newDefaultBag();
            for (Tuple t : inBag) {
                DataBag nestedBag = (DataBag) t.get(0);
                for (Tuple t2 : nestedBag) {
                    outBag.add(t2);
                }
            }
            return outBag;
        }
    }
}
