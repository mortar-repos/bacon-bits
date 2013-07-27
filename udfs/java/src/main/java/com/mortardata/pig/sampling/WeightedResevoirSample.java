package com.mortardata.pig.sampling;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
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
 * Sample exactly K elements from a bag,
 * using the Accumulator interface to reduce memory overhead.
 *
 * The last value in every tuple is the "weight" of the tuple.
 * The probability of a weight-W tuple being selected over
 * a weight-1 tuple is W / (W + 1);
 *
 * DEFINE WeightedResevoirSample com.mortardata.pig.sampling.WeightedResevoirSample('5');
 * my_samples   =   FOREACH (GROUP data BY key) GENERATE
 *                      FLATTEN(WeightedResevoirSample(data))
 *                      AS (key, field, other_field, weight);
 *
 */
public class WeightedResevoirSample extends EvalFunc<DataBag> implements Accumulator<DataBag> {

    private TupleFactory tf = TupleFactory.getInstance();
    private   BagFactory bf = BagFactory.getInstance();

    private class TupleAndKey implements Comparable<TupleAndKey> {
        private double key;
        private Tuple t;

        private TupleAndKey(Tuple t) {
            try {
                this.t = t;
                this.key = Math.pow(rand.nextDouble(), 1.0 / (Double) t.get(t.size() - 1));
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }

        public int compareTo(TupleAndKey other) {
                 if (this.key < other.key) { return -1; }
            else if (this.key > other.key) { return +1; }
                                      else { return  0; }
        }
    }

    private int k;
    private PriorityQueue<TupleAndKey> resevoir;
    private Random rand;

    public WeightedResevoirSample(String k) {
        this.k   = Integer.parseInt(k);
        resevoir = new PriorityQueue<TupleAndKey>(this.k + 1);
        rand     = new Random();
    }

    public Schema outputSchema(Schema input) {
        try {
            List<Schema.FieldSchema> tupleFields
                = input.getField(0).schema.getField(0).schema.getFields();

            tupleFields.add(new Schema.FieldSchema("rank", DataType.INTEGER));

            return new Schema(
                new Schema.FieldSchema("sample",
                    new Schema(
                        new Schema.FieldSchema("t",
                            new Schema(tupleFields),
                        DataType.TUPLE)
                    ),
                DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanup() {
        resevoir = new PriorityQueue<TupleAndKey>(k);
    }

    public DataBag getValue() {
        DataBag outputBag = bf.newDefaultBag();
        int rank = 1;
        while (resevoir.size() > 0) {
            TupleAndKey tak = resevoir.poll();
            ArrayList<Object> fields = new ArrayList<Object>(tak.t.size() + 1);
            fields.addAll(tak.t.getAll());
            fields.add(rank);
            outputBag.add(tf.newTupleNoCopy(fields));
            rank++;
        }
        return outputBag;
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
            if (resevoir.size() < k) {
                resevoir.add(new TupleAndKey(t));
            } else {
                resevoir.add(new TupleAndKey(t));
                resevoir.poll(); // remove the smallest element since the resevoir is full
            }
        }
    }
}
