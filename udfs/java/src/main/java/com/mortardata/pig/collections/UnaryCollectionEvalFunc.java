package com.mortardata.pig.collections;

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

public abstract class UnaryCollectionEvalFunc<T> extends EvalFunc<T> {
    public T exec(Tuple input) {
        try {
            Object o = PigCollection.deserialize((DataByteArray) input.get(0));

            if (o instanceof PigCollection.IntArray) {
                return execOnIntArray(((PigCollection.IntArray) o).array);
            } else if (o instanceof PigCollection.FloatArray) {
                return execOnFloatArray(((PigCollection.FloatArray) o).array);
            } else if (o instanceof TIntIntHashMap) {
                return execOnIntIntMap((TIntIntHashMap) o);
            } else if (o instanceof TIntFloatHashMap) {
                return execOnIntFloatMap((TIntFloatHashMap) o);
            } else if (o instanceof TObjectIntHashMap) {
                return execOnStringIntMap((TObjectIntHashMap) o);
            } else if (o instanceof TObjectFloatHashMap) {
                return execOnStringFloatMap((TObjectFloatHashMap) o);
            } else if (o instanceof TIntHashSet) {
                return execOnIntSet((TIntHashSet) o);
            } else if (o instanceof Set) {
                return execOnStringSet((Set<String>) o);
            } else {
                throw new RuntimeException(
                    "Invalid PigArray type byte. The input might not be an actual PigArray"
                );
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    public T execOnIntArray(int[] arr) throws ExecException {
        throw new RuntimeException("Function not implemented for int arrays");
    };

    public T execOnFloatArray(float[] arr) throws ExecException {
        throw new RuntimeException("Function not implemented for float arrays");
    };

    public T execOnIntIntMap(TIntIntHashMap map) throws ExecException {
        throw new RuntimeException("Function not implemented for int-int maps");
    };

    public T execOnIntFloatMap(TIntFloatHashMap map) throws ExecException {
        throw new RuntimeException("Function not implemented for int-float maps");
    };

    public T execOnStringIntMap(TObjectIntHashMap map) throws ExecException {
        throw new RuntimeException("Function not implemented for string-int maps");
    };

    public T execOnStringFloatMap(TObjectFloatHashMap map) throws ExecException {
        throw new RuntimeException("Function not implemented for string-float maps");
    };

    public T execOnIntSet(TIntHashSet set) throws ExecException {
        throw new RuntimeException("Function not implemented for int sets");
    };

    public T execOnStringSet(Set<String> set) throws ExecException {
        throw new RuntimeException("Function not implemented for string sets");
    };
}
