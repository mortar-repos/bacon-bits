package com.mortardata.pig.collections;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/*
 * Deserializes an array-type PigCollection into a tuple.
 */
public class FromPigCollectionToTuple extends EvalFunc<Tuple> {
    public static final TupleFactory tf = TupleFactory.getInstance();

    public Schema outputSchema(Schema input) {
        try {
            String alias = input.getField(0).alias;
            return new Schema(new Schema.FieldSchema(alias, DataType.TUPLE));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public Tuple exec(Tuple input) throws IOException {
        try {
            Object o = PigCollection.deserialize((DataByteArray) input.get(0));
            Tuple t;

            if (o instanceof PigCollection.IntArray) {
                int[] arr = ((PigCollection.IntArray) o).array;
                t = tf.newTuple(arr.length);
                for (int i = 0; i < arr.length; i++) {
                    t.set(i, new Integer(arr[i]));
                }
            } else if (o instanceof PigCollection.FloatArray) {
                float[] arr = ((PigCollection.FloatArray) o).array;
                t = tf.newTuple(arr.length);
                for (int i = 0; i < arr.length; i++) {
                    t.set(i, new Float(arr[i]));
                }
            } else {
                throw new RuntimeException("Input PigArray is not of a dense type");
            }

            return t;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }
}
