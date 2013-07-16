package com.mortardata.pig.sampling;

import java.util.ArrayList;
import java.util.Random;

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

import java.io.IOException;

/*
 * Sample exactly K elements from a bag, N times,
 * using the Accumulator interface to reduce memory overhead
 * and enumerating each sample.
 *
 * Ex:
 *
 * DEFINE RepeatedResevoirSample com.mortardata.pig.sampling.RepeatedResevoirSample('3', '2');
 * my_samples   =   FOREACH (GROUP data ALL) GENERATE
 *                      FLATTEN(RepeatedResevoirSample(data));
 *
 * --> (1, {(f), (b), (i)})
 *     (2, {(c), (i), (a)})
 */
public class RepeatedResevoirSample extends EvalFunc<DataBag> implements Accumulator<DataBag> {

    private TupleFactory tf = TupleFactory.getInstance();
    private   BagFactory bf = BagFactory.getInstance();

    private int k;
    private int n;
    private ArrayList<ArrayList<Tuple>> resevoirs;
    private Random rand;
    private int count;

    public RepeatedResevoirSample(String k, String n) {
        this.k    = Integer.parseInt(k);
        this.n    = Integer.parseInt(n);
        resevoirs = new ArrayList<ArrayList<Tuple>>(this.n);
        for (int i = 0; i < this.n; i++) {
            resevoirs.add(new ArrayList<Tuple>(this.k));
        }
        rand      = new Random();
        count     = 0;
    }

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<Schema.FieldSchema> tupleSchema = new ArrayList<Schema.FieldSchema>();
            tupleSchema.add(new Schema.FieldSchema("sample_id", DataType.INTEGER));
            tupleSchema.add(input.getField(0));

            return new Schema(
                new Schema.FieldSchema("samples",
                    new Schema(
                        new Schema.FieldSchema("t",
                            new Schema(tupleSchema)
                        , DataType.TUPLE)
                    )
                , DataType.BAG)
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
        for (Tuple t : bag) {
            if (count < k) {
                for (ArrayList<Tuple> r : resevoirs) {
                    r.add(t);
                }
            } else {
                for (ArrayList<Tuple> r : resevoirs) {
                    int j = rand.nextInt(count + 1);
                    if (j < k) {
                        r.set(j, t);
                    }
                }
            }
            count++;
        }
    }

    public DataBag getValue() {
        try {
            DataBag outputBag = bf.newDefaultBag();
            for (int i = 0; i < this.n; i++) {
                ArrayList<Tuple> r = resevoirs.get(i);
                
                Tuple t = tf.newTuple(2);
                t.set(0, new Integer(i));
                
                DataBag b = bf.newDefaultBag();
                for (Tuple t2 : r) {
                    b.add(t2);
                }
                t.set(1, b);

                outputBag.add(t);
            }
            return outputBag;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanup() {
        resevoirs = new ArrayList<ArrayList<Tuple>>(this.n);
        for (int i = 0; i < this.n; i++) {
            resevoirs.add(new ArrayList<Tuple>(this.k));
        }
        count    = 0;
    }
}
