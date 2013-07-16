package com.mortardata.pig.collections;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
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
 * {(key: int/chararray, features: PigCollection)} --> T
 */
public abstract class AccumulatorCollectionEvalFuncWithItemIds<T> extends EvalFunc<T> implements Accumulator<T> {
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
            Object firstItem = PigCollection.deserialize((DataByteArray) firstTuple.get(1));
            final Iterator<Tuple> bagIt = bag.iterator();

            if (firstItem instanceof PigCollection.IntArray) {
                accumulateIntArrays(new Iterator<ImmutablePair<Integer, PigCollection.IntArray>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public ImmutablePair<Integer, PigCollection.IntArray> next() {
                        try {
                            Tuple t = bagIt.next();
                            return ImmutablePair.of(
                                (Integer) t.get(0),
                                (PigCollection.IntArray) deserialize(t.get(1))
                            );
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof PigCollection.FloatArray) {
                accumulateFloatArrays(new Iterator<ImmutablePair<Integer, PigCollection.FloatArray>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public ImmutablePair<Integer, PigCollection.FloatArray> next() {
                        try {
                            Tuple t = bagIt.next();
                            return ImmutablePair.of(
                                (Integer) t.get(0),
                                (PigCollection.FloatArray) deserialize(t.get(1))
                            );
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TIntIntHashMap) {
                accumulateIntIntMaps(new Iterator<ImmutablePair<Integer, TIntIntHashMap>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public ImmutablePair<Integer, TIntIntHashMap> next() {
                        try {
                            Tuple t = bagIt.next();
                            return ImmutablePair.of(
                                (Integer) t.get(0),
                                (TIntIntHashMap) deserialize(t.get(1))
                            );
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TIntFloatHashMap) {
                accumulateIntFloatMaps(new Iterator<ImmutablePair<Integer, TIntFloatHashMap>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public ImmutablePair<Integer, TIntFloatHashMap> next() {
                        try {
                            Tuple t = bagIt.next();
                            return ImmutablePair.of(
                                (Integer) t.get(0),
                                (TIntFloatHashMap) deserialize(t.get(1))
                            );
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TObjectIntHashMap) {
                accumulateStringIntMaps(new Iterator<ImmutablePair<Integer, TObjectIntHashMap>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public ImmutablePair<Integer, TObjectIntHashMap> next() {
                        try {
                            Tuple t = bagIt.next();
                            return ImmutablePair.of(
                                (Integer) t.get(0),
                                (TObjectIntHashMap) deserialize(t.get(1))
                            );
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TObjectFloatHashMap) {
                accumulateStringFloatMaps(new Iterator<ImmutablePair<Integer, TObjectFloatHashMap>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public ImmutablePair<Integer, TObjectFloatHashMap> next() {
                        try {
                            Tuple t = bagIt.next();
                            return ImmutablePair.of(
                                (Integer) t.get(0),
                                (TObjectFloatHashMap) deserialize(t.get(1))
                            );
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof TIntHashSet) {
                accumulateIntSets(new Iterator<ImmutablePair<Integer, TIntHashSet>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public ImmutablePair<Integer, TIntHashSet> next() {
                        try {
                            Tuple t = bagIt.next();
                            return ImmutablePair.of(
                                (Integer) t.get(0),
                                (TIntHashSet) deserialize(t.get(1))
                            );
                        } catch (ExecException e) { throw new RuntimeException(e); }
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                });
            } else if (firstItem instanceof Set) {
                accumulateStringSets(new Iterator<ImmutablePair<Integer, Set<String>>>() {
                    public boolean hasNext() { return bagIt.hasNext(); }
                    public ImmutablePair<Integer, Set<String>> next() {
                        try {
                            Tuple t = bagIt.next();
                            return ImmutablePair.of(
                                (Integer) t.get(0),
                                (Set<String>) deserialize(t.get(1))
                            );
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

    public void accumulateIntArrays(Iterator<ImmutablePair<Integer, PigCollection.IntArray>> it) throws ExecException {
        throw new RuntimeException("Function not implemented for int arrays");
    }

    public void accumulateFloatArrays(Iterator<ImmutablePair<Integer, PigCollection.FloatArray>> it) throws ExecException {
        throw new RuntimeException("Function not implemented for float arrays");
    }

    public void accumulateIntIntMaps(Iterator<ImmutablePair<Integer, TIntIntHashMap>> it) throws ExecException {
        throw new RuntimeException("Function not implemented for int-int maps");
    }

    public void accumulateIntFloatMaps(Iterator<ImmutablePair<Integer, TIntFloatHashMap>> it) throws ExecException {
        throw new RuntimeException("Function not implemented for int-float maps");
    }

    public void accumulateStringIntMaps(Iterator<ImmutablePair<Integer, TObjectIntHashMap>> it) throws ExecException {
        throw new RuntimeException("Function not implemented for string-int maps");
    }

    public void accumulateStringFloatMaps(Iterator<ImmutablePair<Integer, TObjectFloatHashMap>> it) throws ExecException {
        throw new RuntimeException("Function not implemented for string-float maps");
    }

    public void accumulateIntSets(Iterator<ImmutablePair<Integer, TIntHashSet>> it) throws ExecException {
        throw new RuntimeException("Function not implemented for int sets");
    }

    public void accumulateStringSets(Iterator<ImmutablePair<Integer, Set<String>>> it) throws ExecException {
        throw new RuntimeException("Function not implmented for string sets");
    }
}