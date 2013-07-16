package com.mortardata.pig.collections;

import java.io.IOException;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ElementwiseProduct extends BinaryCollectionEvalFunc<DataByteArray> {
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("product", DataType.BYTEARRAY));
    }

    public DataByteArray execOnIntArrays(int[] u, int[] v) {
        int[] p = new int[u.length];
        for (int i = 0; i < u.length; i++) {
            p[i] = u[i] * v[i];
        }
        return PigCollection.serialize(p);
    }

    public DataByteArray execOnFloatArrays(float[] u, float[] v) {
        float[] p = new float[u.length];
        for (int i = 0; i < u.length; i++) {
            p[i] = u[i] * v[i];
        }
        return PigCollection.serialize(p);
    }

    public DataByteArray execOnIntIntMaps(TIntIntHashMap u, TIntIntHashMap v) {
        TIntIntHashMap p = new TIntIntHashMap();
        TIntIntIterator it = u.iterator();
        for (int i = 0; i < u.size(); i++) {
            it.advance();
            int vVal = v.get(it.key());
            if (vVal != 0) {
                p.put(it.key(), it.value() * vVal);
            }
        }
        return PigCollection.serialize(p);
    };

    public DataByteArray execOnIntFloatMaps(TIntFloatHashMap u, TIntFloatHashMap v) {
        TIntFloatHashMap p = new TIntFloatHashMap();
        TIntFloatIterator it = u.iterator();
        for (int i = 0; i < u.size(); i++) {
            it.advance();
            float vVal = v.get(it.key());
            if (vVal != 0) {
                p.put(it.key(), it.value() * vVal);
            }
        }
        return PigCollection.serialize(p);
    };

    public DataByteArray execOnStringIntMaps(TObjectIntHashMap u, TObjectIntHashMap v) {
        TObjectIntHashMap p = new TObjectIntHashMap();
        TObjectIntIterator it = u.iterator();
        for (int i = 0; i < u.size(); i++) {
            it.advance();
            int vVal = v.get(it.key());
            if (vVal != 0) {
                p.put(it.key(), it.value() * vVal);
            }
        }
        return PigCollection.serialize(p);
    };
    
    public DataByteArray execOnStringFloatMaps(TObjectFloatHashMap u, TObjectFloatHashMap v) {
        TObjectFloatHashMap p = new TObjectFloatHashMap();
        TObjectFloatIterator it = u.iterator();
        for (int i = 0; i < u.size(); i++) {
            it.advance();
            float vVal = v.get(it.key());
            if (vVal != 0) {
                p.put(it.key(), it.value() * vVal);
            }
        }
        return PigCollection.serialize(p);
    };
}
