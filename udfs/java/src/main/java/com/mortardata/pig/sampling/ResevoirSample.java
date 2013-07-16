package com.mortardata.pig.sampling;

import java.util.ArrayList;
import java.util.Random;

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
 * Sample exactly K elements from a bag,
 * using the Accumulator interface to reduce memory overhead.
 *
 * Ex:
 *
 * DEFINE ResevoirSample com.mortardata.pig.sampling.ResevoirSample('5');
 * my_samples   =   FOREACH (GROUP data BY key) GENERATE
 *                      group AS key,
 *                      ResevoirSample(data) AS sample;
 *
 */
public class ResevoirSample extends EvalFunc<DataBag> implements Accumulator<DataBag> {

    private TupleFactory tf = TupleFactory.getInstance();
    private   BagFactory bf = BagFactory.getInstance();

    private int k;
    private ArrayList<Tuple> resevoir;
    private Random rand;
    private int count;

    public ResevoirSample(String k) {
        this.k   = Integer.parseInt(k);
        resevoir = new ArrayList<Tuple>(this.k);
        rand     = new Random();
        count    = 0;
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
                resevoir.add(t);
            } else {
                int j = rand.nextInt(count + 1);
                if (j < k) {
                    resevoir.set(j, t);
                }
            }
            count++;
        }
    }

    public DataBag getValue() {
        DataBag outputBag = bf.newDefaultBag();
        for (Tuple t : resevoir) {
            outputBag.add(t);
        }
        return outputBag;
    }

    public void cleanup() {
        resevoir = new ArrayList<Tuple>(k);
        count    = 0;
    }

    public Schema outputSchema(Schema input) {
        return input;
    }
}
