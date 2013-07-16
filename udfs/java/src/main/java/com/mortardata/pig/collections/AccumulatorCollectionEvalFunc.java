package com.mortardata.pig.collections;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/*
 * {(features: PigCollection)} --> T
 */
public abstract class AccumulatorCollectionEvalFunc<T> extends EvalFunc<T> implements Accumulator<T> {
    public T exec(Tuple input) throws IOException {
        accumulate(input);
        T output = getValue();
        cleanup();
        return output;
    }

    public abstract Schema outputSchema(Schema input);
    public abstract T getValue();
    public abstract void cleanup();

    private Object deserialize(Object o) throws ExecException {
        return PigCollection.deserialize((DataByteArray) o);
    }

    public void accumulate(Tuple input) throws IOException {
        try {
            DataBag bag = (DataBag) input.get(0);
            Iterator<Tuple> peekIt = bag.iterator();

            if (!peekIt.hasNext()) {
                return;
            }

            Tuple firstTuple = peekIt.next();
            Object firstItem = PigCollection.deserialize((DataByteArray) firstTuple.get(0));
            final Iterator<Tuple> bagIt = bag.iterator();

            if (firstItem instanceof PigCollection.IntArray) {
                accumulateIntArrays(new Iterator<PigCollection.IntArray>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public PigCollection.IntArray next() {
                        try {
                            return (PigCollection.IntArray) deserialize(bagIt.next().get(0));
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof PigCollection.FloatArray) {
                accumulateFloatArrays(new Iterator<PigCollection.FloatArray>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public PigCollection.FloatArray next() {
                        try {
                            return (PigCollection.FloatArray) deserialize(bagIt.next().get(0));
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TIntIntHashMap) {
                accumulateIntIntMaps(new Iterator<TIntIntHashMap>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public TIntIntHashMap next() {
                        try {
                            return (TIntIntHashMap) deserialize(bagIt.next().get(0));
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TIntFloatHashMap) {
                accumulateIntFloatMaps(new Iterator<TIntFloatHashMap>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public TIntFloatHashMap next() {
                        try {
                            return (TIntFloatHashMap) deserialize(bagIt.next().get(0));
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TObjectIntHashMap) {
                accumulateStringIntMaps(new Iterator<TObjectIntHashMap>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public TObjectIntHashMap next() {
                        try {
                            return (TObjectIntHashMap) deserialize(bagIt.next().get(0));
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TObjectFloatHashMap) {
                accumulateStringFloatMaps(new Iterator<TObjectFloatHashMap>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public TObjectFloatHashMap next() {
                        try {
                            return (TObjectFloatHashMap) deserialize(bagIt.next().get(0));
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TIntHashSet) {
                accumulateIntSets(new Iterator<TIntHashSet>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public TIntHashSet next() {
                        try {
                            return (TIntHashSet) deserialize(bagIt.next().get(0));
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof Set) {
                accumulateStringSets(new Iterator<Set<String>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public Set<String> next() {
                        try {
                            return (Set<String>) deserialize(bagIt.next().get(0));
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else {
                throw new RuntimeException(
                    "Invalid PigArray type byte. The input might not be an actual PigArray"
                );
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    public void accumulateIntArrays(Iterator<PigCollection.IntArray> it) throws ExecException {
        throw new RuntimeException("Function not implemented for int arrays");
    }

    public void accumulateFloatArrays(Iterator<PigCollection.FloatArray> it) throws ExecException {
        throw new RuntimeException("Function not implemented for float arrays");
    }

    public void accumulateIntIntMaps(Iterator<TIntIntHashMap> it) throws ExecException {
        throw new RuntimeException("Function not implemented for int-int maps");
    }

    public void accumulateIntFloatMaps(Iterator<TIntFloatHashMap> it) throws ExecException {
        throw new RuntimeException("Function not implemented for int-float maps");
    }

    public void accumulateStringIntMaps(Iterator<TObjectIntHashMap> it) throws ExecException {
        throw new RuntimeException("Function not implemented for string-int maps");
    }

    public void accumulateStringFloatMaps(Iterator<TObjectFloatHashMap> it) throws ExecException {
        throw new RuntimeException("Function not implemented for string-float maps");
    }

    public void accumulateIntSets(Iterator<TIntHashSet> it) throws ExecException {
        throw new RuntimeException("Function not implemented for int sets");
    }

    public void accumulateStringSets(Iterator<Set<String>> it) throws ExecException {
        throw new RuntimeException("Function not implmented for string sets");
    }
}