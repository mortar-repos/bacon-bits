package com.mortardata.pig.collections;

import java.io.IOException;
import java.util.Arrays;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.mortardata.pig.collections.PigCollection;
import com.mortardata.pig.collections.UnaryCollectionEvalFunc;

/*
 * If given an map-type PigCollection, filters out all but the n keys with the greatest values.
 * Ties are broken randomly.
 */
public class TopN extends UnaryCollectionEvalFunc<DataByteArray> {
    private int numFeatures;

    public TopN(String numFeatures) {
        this.numFeatures = Integer.parseInt(numFeatures);
    }

    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("product", DataType.BYTEARRAY));
    }

    public DataByteArray execOnIntIntMap(TIntIntHashMap map) {
        if (map.size() <= numFeatures) {
            return PigCollection.serialize(map);
        }

        int[] values = map.values();
        Arrays.sort(values);
        int minTopValue = values[values.length - numFeatures];
        boolean tookMinTopKey = false;

        TIntIntHashMap topN = new TIntIntHashMap(numFeatures);
        TIntIntIterator it = map.iterator();
        for (int i = 0; i < map.size(); i++) {
            it.advance();
            if (it.value() > minTopValue) {
                topN.put(it.key(), it.value());
            } else if (!tookMinTopKey && it.value() == minTopValue) {
                topN.put(it.key(), it.value());
                tookMinTopKey = true;
            }
        }

        return PigCollection.serialize(topN);
    };

    public DataByteArray execOnIntFloatMap(TIntFloatHashMap map) {
        if (map.size() <= numFeatures) {
            return PigCollection.serialize(map);
        }

        float[] values = map.values();
        Arrays.sort(values);
        float minTopValue = values[values.length - numFeatures];
        boolean tookMinTopKey = false;

        TIntFloatHashMap topN = new TIntFloatHashMap(numFeatures);
        TIntFloatIterator it = map.iterator();
        for (int i = 0; i < map.size(); i++) {
            it.advance();
            if (it.value() > minTopValue) {
                topN.put(it.key(), it.value());
            } else if (!tookMinTopKey && it.value() == minTopValue) {
                topN.put(it.key(), it.value());
                tookMinTopKey = true;
            }
        }

        return PigCollection.serialize(topN);
    };

    public DataByteArray execOnStringIntMap(TObjectIntHashMap map) {
        if (map.size() <= numFeatures) {
            return PigCollection.serialize(map);
        }

        int[] values = map.values();
        Arrays.sort(values);
        int minTopValue = values[values.length - numFeatures];
        boolean tookMinTopKey = false;

        TObjectIntHashMap topN = new TObjectIntHashMap(numFeatures);
        TObjectIntIterator it = map.iterator();
        for (int i = 0; i < map.size(); i++) {
            it.advance();
            if (it.value() > minTopValue) {
                topN.put(it.key(), it.value());
            } else if (!tookMinTopKey && it.value() == minTopValue) {
                topN.put(it.key(), it.value());
                tookMinTopKey = true;
            }
        }

        return PigCollection.serialize(topN);
    };

    public DataByteArray execOnStringFloatMap(TObjectFloatHashMap map) {
        if (map.size() <= numFeatures) {
            return PigCollection.serialize(map);
        }

        float[] values = map.values();
        Arrays.sort(values);
        float minTopValue = values[values.length - numFeatures];
        boolean tookMinTopKey = false;

        TObjectFloatHashMap topN = new TObjectFloatHashMap(numFeatures);
        TObjectFloatIterator it = map.iterator();
        for (int i = 0; i < map.size(); i++) {
            it.advance();
            if (it.value() > minTopValue) {
                topN.put(it.key(), it.value());
            } else if (!tookMinTopKey && it.value() == minTopValue) {
                topN.put(it.key(), it.value());
                tookMinTopKey = true;
            }
        }

        return PigCollection.serialize(topN);
    };
}
