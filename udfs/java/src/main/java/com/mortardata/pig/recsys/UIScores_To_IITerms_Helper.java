package com.mortardata.pig.recsys;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.ImmutableList;

// input:  { user: int, item: int, score: float }
// output: { item_A: int, item_B: int, weight: float }
public class UIScores_To_IITerms_Helper extends EvalFunc<DataBag> {
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>(3);
            tupleFields.add(new Schema.FieldSchema("item_A", DataType.INTEGER));
            tupleFields.add(new Schema.FieldSchema("item_B", DataType.INTEGER));
            tupleFields.add(new Schema.FieldSchema("weight", DataType.FLOAT));

            return new Schema(
                new Schema.FieldSchema("ii_terms",
                    new Schema(
                        new Schema.FieldSchema(null,
                            new Schema(tupleFields),
                        DataType.TUPLE)),
                DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public DataBag exec(Tuple input) throws IOException {
        DataBag inputBag = (DataBag) input.get(0);
        DataBag outputBag = bf.newDefaultBag();

        int i = 0;
        for (Tuple u : inputBag) {
            int j = 0;
            for (Tuple v : inputBag) {
                if (j > i) {
                    outputBag.add(
                        tf.newTupleNoCopy(
                            ImmutableList.of(
                                u.get(1), // item_A
                                v.get(1), // item_B
                                // weight
                                new Float(Math.min(
                                    (Float) u.get(2),
                                    (Float) v.get(2))
                                )
                            )
                        )
                    );
                }
                j++;
            }
            i++;
        }

        return outputBag;
    }
}
