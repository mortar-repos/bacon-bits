package com.mortardata.pig.collections;

import java.io.IOException;
import java.util.ArrayList;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.mortardata.pig.collections.PigCollection;
import com.mortardata.pig.collections.UnaryCollectionEvalFunc;

/*
 * Replicate a map-type PigCollection for each key it has.
 * Intended to precede a GROUP BY statement.
 * You must specify the key datatype yourself in an AS clause if it is not CHARARRAY.
 *
 * ex:
 *
 * replicated = FOREACH items GENERATE
 *                  id,
 *                  FLATTEN(com.mortardata.pig.collections.ReplicateByKey(features))
 *                  AS (key: int, features: bytearray);
 * by_feature = GROUP replicated BY key;
 */
public class ReplicateByKey extends UnaryCollectionEvalFunc<DataBag> {
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();

    public Schema outputSchema(Schema input) {
        try {
            String featuresAlias = input.getField(0).alias;

            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>();
            // not sure how best to generalize this for both int and string keys
            // I'm pretty sure a Pig AS clause as described above will implicitly cat
            // the returned CHARARRAY to an INTEGER
            tupleFields.add(new Schema.FieldSchema("key", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema(featuresAlias, DataType.BYTEARRAY));

            return new Schema(
                new Schema.FieldSchema("replicated",
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

    public DataBag execOnIntIntMap(TIntIntHashMap map) {
        try {
            DataByteArray reserialized = PigCollection.serialize(map);
            DataBag bag = bf.newDefaultBag();

            TIntIntIterator it = map.iterator();
            for (int i = 0; i < map.size(); i++) {
                it.advance();
                Tuple t = tf.newTuple(2);
                t.set(0, new Integer(it.key()).toString());
                t.set(1, reserialized);
                bag.add(t);
            }

            return bag;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    };

    public DataBag execOnIntFloatMap(TIntFloatHashMap map) {
        try {
            DataByteArray reserialized = PigCollection.serialize(map);
            DataBag bag = bf.newDefaultBag();

            TIntFloatIterator it = map.iterator();
            for (int i = 0; i < map.size(); i++) {
                it.advance();
                Tuple t = tf.newTuple(2);
                t.set(0, new Integer(it.key()).toString());
                t.set(1, reserialized);
                bag.add(t);
            }

            return bag;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    };

    public DataBag execOnStringIntMap(TObjectIntHashMap map) {
        try {
            DataByteArray reserialized = PigCollection.serialize(map);
            DataBag bag = bf.newDefaultBag();

            TObjectIntIterator it = map.iterator();
            for (int i = 0; i < map.size(); i++) {
                it.advance();
                Tuple t = tf.newTuple(2);
                t.set(0, it.key());
                t.set(1, reserialized);
                bag.add(t);
            }

            return bag;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    };

    public DataBag execOnStringFloatMap(TObjectFloatHashMap map) {
        try {
            DataByteArray reserialized = PigCollection.serialize(map);
            DataBag bag = bf.newDefaultBag();

            TObjectFloatIterator it = map.iterator();
            for (int i = 0; i < map.size(); i++) {
                it.advance();
                Tuple t = tf.newTuple(2);
                t.set(0, it.key());
                t.set(1, reserialized);
                bag.add(t);
            }

            return bag;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    };
}
