package com.mortardata.pig.collections;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

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

/*
 * Deserializes a map-type or set-type PigCollection into a bag.
 */
public class FromPigCollectionToBag extends EvalFunc<DataBag> {
    public static final BagFactory bf = BagFactory.getInstance();
    public static final TupleFactory tf = TupleFactory.getInstance();

    public Schema outputSchema(Schema input) {
        try {
            String alias = input.getField(0).alias;
            return new Schema(new Schema.FieldSchema(alias, DataType.BAG));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public DataBag exec(Tuple input) throws IOException {
        try {
            Object o = PigCollection.deserialize((DataByteArray) input.get(0));
            DataBag bag = bf.newDefaultBag();

            // there's a lot of code repetition ahead,
            // but it is necessary because Trove primitive iterators
            // can't have a "key()" and "value()" interface,
            // since they are returning primitive types, not objects.

            if (o instanceof TIntIntHashMap) {
                TIntIntHashMap map = (TIntIntHashMap) o;
                TIntIntIterator it = map.iterator();
                ArrayList<Object> fields;
                for (int i = 0; i < map.size(); i++) {
                    it.advance();
                    fields = new ArrayList<Object>(2);
                    fields.add(it.key());
                    fields.add(it.value());
                    bag.add(tf.newTupleNoCopy(fields));
                }
            } else if (o instanceof TIntFloatHashMap) {
                TIntFloatHashMap map = (TIntFloatHashMap) o;
                TIntFloatIterator it = map.iterator();
                ArrayList<Object> fields;
                for (int i = 0; i < map.size(); i++) {
                    it.advance();
                    fields = new ArrayList<Object>(2);
                    fields.add(it.key());
                    fields.add(it.value());
                    bag.add(tf.newTupleNoCopy(fields));
                }
            } else if (o instanceof TObjectIntHashMap) {
                TObjectIntHashMap map = (TObjectIntHashMap) o;
                TObjectIntIterator it = map.iterator();
                ArrayList<Object> fields;
                for (int i = 0; i < map.size(); i++) {
                    it.advance();
                    fields = new ArrayList<Object>(2);
                    fields.add(it.key());
                    fields.add(it.value());
                    bag.add(tf.newTupleNoCopy(fields));
                }
            } else if (o instanceof TObjectFloatHashMap) {
                TObjectFloatHashMap map = (TObjectFloatHashMap) o;
                TObjectFloatIterator it = map.iterator();
                ArrayList<Object> fields;
                for (int i = 0; i < map.size(); i++) {
                    it.advance();
                    fields = new ArrayList<Object>(2);
                    fields.add(it.key());
                    fields.add(it.value());
                    bag.add(tf.newTupleNoCopy(fields));
                }
            } else if (o instanceof TIntHashSet) {
                TIntHashSet set = (TIntHashSet) o;
                TIntIterator it = set.iterator();
                for (int i = 0; i < set.size(); i++) {
                    bag.add(tf.newTuple(it.next()));
                }
            } else if (o instanceof Set) {
                Set<String> set = (Set<String>) o;
                for (String s : set) {
                    bag.add(tf.newTuple(s));
                }
            } else {
                throw new RuntimeException("Input PigArray is not of a sparse or set type");
            }

            return bag;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }
}
