package com.mortardata.pig.collections;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/*
 * An EvalFunc that takes two PigCollections a and b
 * and returns a new PigCollection of the same type as a.
 * ints will be cast to floats when a is float-valued and b is int-valued.
 */
public abstract class BinaryCollectionEvalFunc<T> extends EvalFunc<T> {
    public T exec(Tuple input) {
        try {
            Object a = PigCollection.deserialize((DataByteArray) input.get(0));
            Object b = PigCollection.deserialize((DataByteArray) input.get(1));

            if (a instanceof PigCollection.IntArray) {
                int[] u = ((PigCollection.IntArray) a).array;
                if (! (b instanceof PigCollection.IntArray)) {
                    throw new RuntimeException("Incompatible PigCollection types");
                }
                int[] v = ((PigCollection.IntArray) b).array;
                if (u.length != v.length) {
                    throw new RuntimeException("Array-type PigCollection inputs must have equal lengths");
                }
                return execOnIntArrays(u, v);
            } else if (a instanceof PigCollection.FloatArray) {
                float[] u = ((PigCollection.FloatArray) a).array;
                float[] v;
                if (b instanceof PigCollection.FloatArray) {
                    v = ((PigCollection.FloatArray) b).array;
                } else if (b instanceof PigCollection.IntArray) {
                    int[] vTemp = ((PigCollection.IntArray) b).array;
                    v = new float[vTemp.length];
                    for (int i = 0; i < vTemp.length; i++) {
                        v[i] = (float) vTemp[i];
                    }
                } else {
                    throw new RuntimeException("Incompatible PigCollection types");
                }
                if (u.length != v.length) {
                    throw new RuntimeException("Array-type PigCollection inputs must have equal lengths");
                }
                return execOnFloatArrays(u, v);
            } else if (a instanceof TIntIntHashMap) {
                TIntIntHashMap u = (TIntIntHashMap) a;
                if (! (b instanceof TIntIntHashMap)) {
                    throw new RuntimeException("Incompatible PigCollection types");
                }
                TIntIntHashMap v = (TIntIntHashMap) b;
                return execOnIntIntMaps(u, v);
            } else if (a instanceof TIntFloatHashMap) {
                TIntFloatHashMap u = (TIntFloatHashMap) a;
                TIntFloatHashMap v;
                if (b instanceof TIntFloatHashMap) {
                    v = (TIntFloatHashMap) b;
                } else if (b instanceof TIntIntHashMap) {
                    TIntIntHashMap vTemp = (TIntIntHashMap) b;
                    v = new TIntFloatHashMap(vTemp.size());
                    TIntIntIterator it = vTemp.iterator();
                    for (int i = 0; i < vTemp.size(); i++) {
                        it.advance();
                        v.put(it.key(), (float) it.value());
                    }
                } else {
                    throw new RuntimeException("Incompatible PigCollection types");
                }
                return execOnIntFloatMaps(u, v);
            } else if (a instanceof TObjectIntHashMap) {
                TObjectIntHashMap u = (TObjectIntHashMap) a;
                if (! (b instanceof TObjectIntHashMap)) {
                    throw new RuntimeException("Incompatible PigCollection types");
                }
                TObjectIntHashMap v = (TObjectIntHashMap) b;
                return execOnStringIntMaps(u, v);
            } else if (a instanceof TObjectFloatHashMap) {
                TObjectFloatHashMap u = (TObjectFloatHashMap) a;
                TObjectFloatHashMap v;
                if (b instanceof TObjectFloatHashMap) {
                    v = (TObjectFloatHashMap) b;
                } else if (b instanceof TObjectIntHashMap) {
                    TObjectIntHashMap vTemp = (TObjectIntHashMap) b;
                    v = new TObjectFloatHashMap(vTemp.size());
                    TObjectIntIterator it = vTemp.iterator();
                    for (int i = 0; i < vTemp.size(); i++) {
                        it.advance();
                        v.put(it.key(), (float) it.value());
                    }
                } else {
                    throw new RuntimeException("Incompatible PigCollection types");
                }
                return execOnStringFloatMaps(u, v);
            } else if (a instanceof TIntHashSet) {
                TIntHashSet u = (TIntHashSet) a;
                if (! (b instanceof TIntHashSet)) {
                    throw new RuntimeException("Incompatible PigCollection types");
                }
                TIntHashSet v = (TIntHashSet) b;
                return execOnIntSets(u, v);
            } else if (a instanceof Set) {
                Set<String> u = (Set<String>) a;
                if (! (b instanceof Set)) {
                    throw new RuntimeException("Incompatible PigCollection types");
                }
                Set<String> v = (Set<String>) b;
                return execOnStringSets(u, v);
            } else {
                throw new RuntimeException("Invalid PigArray type byte. The input might not be an actual PigArray");
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    public T execOnIntArrays(int[] u, int[] v) throws ExecException {
        throw new RuntimeException("Function not implemented for int arrays");
    };

    public T execOnFloatArrays(float[] u, float[] v) throws ExecException {
        throw new RuntimeException("Function not implemented for float arrays");
    };

    public T execOnIntIntMaps(TIntIntHashMap u, TIntIntHashMap v) throws ExecException {
        throw new RuntimeException("Function not implemented for int-int maps");
    };

    public T execOnIntFloatMaps(TIntFloatHashMap u, TIntFloatHashMap v) throws ExecException {
        throw new RuntimeException("Function not implemented for int-float maps");
    };

    public T execOnStringIntMaps(TObjectIntHashMap u, TObjectIntHashMap v) throws ExecException {
        throw new RuntimeException("Function not implemented for string-int maps");
    };

    public T execOnStringFloatMaps(TObjectFloatHashMap u, TObjectFloatHashMap v) throws ExecException {
        throw new RuntimeException("Function not implemented for string-float maps");
    };

    public T execOnIntSets(TIntHashSet u, TIntHashSet v) throws ExecException {
        throw new RuntimeException("Function not implemented for int sets");
    };

    public T execOnStringSets(Set<String> u, Set<String> v) throws ExecException {
        throw new RuntimeException("Function not implemented for string sets");
    };
}
