package com.mortardata.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Collections;

/**
 * Function solves a very specific use case of having a tuple with unknown values, and wanting to create a bag
 * with each of those values in its own tuple.
 *
 * This comes up with STRSPLIT() which returns a tuple with all the resulting splits; if this result is unknown
 * it can be hard to treat without effectively putting each result into its own "row"
 */
public class TupleToBag extends EvalFunc<DataBag> {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple tuple) throws IOException {

        DataBag output = bagFactory.newDefaultBag();
        Tuple t = DataType.toTuple(tuple.get(0));
        for (Object o : t.getAll()) {
            output.add(tupleFactory.newTuple(Collections.singletonList(o)));
        }

        return output;

    }

    public Schema outputSchema(Schema input) {
        try {
            Schema bagSchema = new Schema();
            bagSchema.add(input.getField(0));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                    bagSchema, DataType.BAG));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }
}
