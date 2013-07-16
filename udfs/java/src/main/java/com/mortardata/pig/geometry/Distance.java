package com.mortardata.pig.geometry;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

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

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.mortardata.pig.collections.BinaryCollectionEvalFunc;

/*
 * Given two PigCollection objects (encoded as bytearrays), returns the distance between them.
 * 
 * For array types, calculates the Euclidean distance.
 * For map types, calculates the angular distance, 1 - (2 * angle between vectors / pi).
 * For set types, calculates Jaccard distance, 1 - |A ∩ B| / |A ∪ B|.
 */
public class Distance extends BinaryCollectionEvalFunc<Float> {

    public Schema outputSchema(Schema input) {
        return new Schema(
            new Schema.FieldSchema("distance", DataType.DOUBLE)
        );
    }

    public Float execOnIntArrays(int[] u, int[] v) {
        long sumSq = 0;
        for (int i = 0; i < u.length; i++) {
            int diff = u[i] - v[i];
            sumSq += diff * diff;
        }
        return (float) Math.sqrt((double) sumSq);
    }

    public Float execOnFloatArrays(float[] u, float[] v) {
        double sumSq = 0.0;
        for (int i = 0; i < u.length; i++) {
            sumSq += Math.pow(u[i] - v[i], 2);
        }
        return (float) Math.sqrt(sumSq);
    }

    public Float execOnIntIntMaps(TIntIntHashMap u, TIntIntHashMap v) {
        long dotProd  = 0;
        double uSumSq = 0.0;
        double vSumSq = 0.0;

        TIntIntIterator it = u.iterator();
        for (int i = 0; i < u.size(); i++) {
            it.advance();
            uSumSq += Math.pow((double) it.value(), 2);
            int vVal = v.get(it.key());
            if (vVal != 0) {
                dotProd += it.value() * vVal;
            }
        }

        it = v.iterator();
        for (int i = 0; i < v.size(); i++) {
            it.advance();
            vSumSq += Math.pow((double) it.value(), 2);
        }

        return angularDistance((double) dotProd, uSumSq, vSumSq);
    };

    public Float execOnIntFloatMaps(TIntFloatHashMap u, TIntFloatHashMap v) {
        double dotProd = 0.0;
        double uSumSq  = 0.0;
        double vSumSq  = 0.0;

        TIntFloatIterator it = u.iterator();
        for (int i = 0; i < u.size(); i++) {
            it.advance();
            uSumSq += Math.pow(it.value(), 2);
            double vVal = v.get(it.key());
            if (vVal != 0) {
                dotProd += it.value() * vVal;
            }
        }

        it = v.iterator();
        for (int i = 0; i < v.size(); i++) {
            it.advance();
            vSumSq += Math.pow(it.value(), 2);
        }

        return angularDistance(dotProd, uSumSq, vSumSq);
    };

    public Float execOnStringIntMaps(TObjectIntHashMap u, TObjectIntHashMap v) {
        long dotProd  = 0;
        double uSumSq = 0.0;
        double vSumSq = 0.0;

        TObjectIntIterator it = u.iterator();
        for (int i = 0; i < u.size(); i++) {
            it.advance();
            uSumSq += Math.pow(it.value(), 2);
            int vVal = v.get(it.key());
            if (vVal != 0) {
                dotProd += it.value() * vVal;
            }
        }

        it = v.iterator();
        for (int i = 0; i < v.size(); i++) {
            it.advance();
            vSumSq += Math.pow(it.value(), 2);
        }

        return angularDistance((double) dotProd, uSumSq, vSumSq);
    };
    
    public Float execOnStringFloatMaps(TObjectFloatHashMap u, TObjectFloatHashMap v) {
        double dotProd = 0.0;
        double uSumSq  = 0.0;
        double vSumSq  = 0.0;

        TObjectFloatIterator it = u.iterator();
        for (int i = 0; i < u.size(); i++) {
            it.advance();
            uSumSq += Math.pow(it.value(), 2);
            double vVal = v.get(it.key());
            if (vVal != 0) {
                dotProd += it.value() * vVal;
            }
        }

        it = v.iterator();
        for (int i = 0; i < v.size(); i++) {
            it.advance();
            vSumSq += Math.pow(it.value(), 2);
        }

        return angularDistance(dotProd, uSumSq, vSumSq);
    };

    public Float execOnIntSets(TIntHashSet u, TIntHashSet v) {
        TIntHashSet intersection = new TIntHashSet(u);
        intersection.retainAll(v);
        TIntHashSet union = new TIntHashSet(u);
        union.addAll(v);
        return (float) (1.0 - ((double) intersection.size()) / ((double) union.size()));
    }

    public Float execOnStringSets(Set<String> u, Set<String> v) {
        Set<String> intersection = new HashSet<String>(u);
        intersection.retainAll(v);
        Set<String> union = new HashSet<String>(v);
        union.addAll(v);
        return (float) (1.0 - ((double) intersection.size()) / ((double) union.size()));
    }

    private Float angularDistance(double dotProd, double uSumSq, double vSumSq) {
        double angle = Math.acos(dotProd / (Math.sqrt(uSumSq) * Math.sqrt(vSumSq)));
        return (float) (1.0 - (2.0 * angle / Math.PI));
    }
}
