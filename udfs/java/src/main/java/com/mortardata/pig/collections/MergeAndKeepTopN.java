package com.mortardata.pig.collections;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;

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
import com.mortardata.pig.collections.AccumulatorCollectionEvalFunc;

/*
 * Given a bag of map-type PigCollection, merges the maps
 * and keeps only the keys with the top N values.
 * Ties are broken randomly.
 */
public class MergeAndKeepTopN extends AccumulatorCollectionEvalFunc<DataByteArray> {
    public class IntIntPair implements Comparable<IntIntPair> {
        public int key, value;
        public IntIntPair(int key, int value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(IntIntPair other) {
                 if (value < other.value) { return -1; }
            else if (value > other.value) { return +1; }
                                     else { return  0; }
        }

        public int hashCode() { return key; }
    }

    public class IntFloatPair implements Comparable<IntFloatPair> {
        public int key;
        public float value;
        public IntFloatPair(int key, float value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(IntFloatPair other) {
                 if (value < other.value) { return -1; }
            else if (value > other.value) { return +1; }
                                     else { return  0; }
        }

        public int hashCode() { return key; }
    }

    public class ObjectIntPair implements Comparable<ObjectIntPair> {
        public Object key;
        public int value;
        public ObjectIntPair(Object key, int value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(ObjectIntPair other) {
                 if (value < other.value) { return -1; }
            else if (value > other.value) { return +1; }
                                     else { return  0; }
        }

        public int hashCode() { return key.hashCode(); }
    }

    public class ObjectFloatPair implements Comparable<ObjectFloatPair> {
        public Object key;
        public float value;
        public ObjectFloatPair(Object key, float value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(ObjectFloatPair other) {
                 if (value < other.value) { return -1; }
            else if (value > other.value) { return +1; }
                                     else { return  0; }
        }

        public int hashCode() { return key.hashCode(); }
    }

    private int numFeatures;

    // we'll initialize all of these, but use only one of them depending on the input type
    // this is necessary since we're using primitives and not objects
    private PriorityQueue<IntIntPair> topN_IntInt;
    private PriorityQueue<IntFloatPair> topN_IntFloat;
    private PriorityQueue<ObjectIntPair> topN_StringInt;
    private PriorityQueue<ObjectFloatPair> topN_StringFloat;
    int minTopVal_Int;
    float minTopVal_Float;
    
    public MergeAndKeepTopN(String numFeatures) {
        this.numFeatures = Integer.parseInt(numFeatures);
        cleanup();
    }

    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("merged", DataType.BYTEARRAY));
    }

    public DataByteArray getValue() {
        if (topN_IntInt != null) {
            TIntIntHashMap merged = new TIntIntHashMap(numFeatures);
            for (IntIntPair pair : topN_IntInt) {
                merged.put(pair.key, pair.value);
            }
            return PigCollection.serialize(merged);
        } else if (topN_IntFloat != null) {
            TIntFloatHashMap merged = new TIntFloatHashMap(numFeatures);
            for (IntFloatPair pair : topN_IntFloat) {
                merged.put(pair.key, pair.value);
            }
            return PigCollection.serialize(merged);
        } else if (topN_StringInt != null) {
            TObjectIntHashMap merged = new TObjectIntHashMap(numFeatures);
            for (ObjectIntPair pair : topN_StringInt) {
                merged.put(pair.key, pair.value);
            }
            return PigCollection.serialize(merged);
        } else if (topN_StringFloat != null) {
            TObjectFloatHashMap merged = new TObjectFloatHashMap(numFeatures);
            for (ObjectFloatPair pair : topN_StringFloat) {
                merged.put(pair.key, pair.value);
            }
            return PigCollection.serialize(merged);
        } else {
            throw new RuntimeException("No input accumulated...something strange is going on");
        }
    }

    public void cleanup() {
        topN_IntInt = null;
        topN_IntFloat = null;
        topN_StringInt = null;
        topN_StringFloat = null;
        minTopVal_Int = Integer.MAX_VALUE;
        minTopVal_Float = Float.MAX_VALUE;
    }

    public void accumulateIntIntMaps(Iterator<TIntIntHashMap> it) {
        if (topN_IntInt == null) {
            topN_IntInt = new PriorityQueue<IntIntPair>(numFeatures);
        }

        while (it.hasNext()) {
            TIntIntIterator mapIt = it.next().iterator();
            while (mapIt.hasNext()) {
                mapIt.advance();
                if (topN_IntInt.size() < numFeatures) {
                    topN_IntInt.offer(new IntIntPair(mapIt.key(), mapIt.value()));
                    if (mapIt.value() < minTopVal_Int) {
                        minTopVal_Int = mapIt.value();
                    }
                } else if (mapIt.value() > minTopVal_Int) {
                    topN_IntInt.poll();
                    topN_IntInt.offer(new IntIntPair(mapIt.key(), mapIt.value()));
                    minTopVal_Int = topN_IntInt.peek().value;
                }
            }
        }
    };

    public void accumulateIntFloatMaps(Iterator<TIntFloatHashMap> it) {
        if (topN_IntFloat == null) {
            topN_IntFloat = new PriorityQueue<IntFloatPair>(numFeatures);
        }

        while (it.hasNext()) {
            TIntFloatIterator mapIt = it.next().iterator();
            while (mapIt.hasNext()) {
                mapIt.advance();
                if (topN_IntFloat.size() < numFeatures) {
                    topN_IntFloat.offer(new IntFloatPair(mapIt.key(), mapIt.value()));
                    if (mapIt.value() < minTopVal_Float) {
                        minTopVal_Float = mapIt.value();
                    }
                } else if (mapIt.value() > minTopVal_Float) {
                    topN_IntFloat.poll();
                    topN_IntFloat.offer(new IntFloatPair(mapIt.key(), mapIt.value()));
                    minTopVal_Float = topN_IntFloat.peek().value;
                }
            }
        }
    };

    public void accumulateStringIntMaps(Iterator<TObjectIntHashMap> it) {
        if (topN_StringInt == null) {
            topN_StringInt = new PriorityQueue<ObjectIntPair>(numFeatures);
        }

        while (it.hasNext()) {
            TObjectIntIterator mapIt = it.next().iterator();
            while (mapIt.hasNext()) {
                mapIt.advance();
                if (topN_StringInt.size() < numFeatures) {
                    topN_StringInt.offer(new ObjectIntPair(mapIt.key(), mapIt.value()));
                    if (mapIt.value() < minTopVal_Int) {
                        minTopVal_Int = mapIt.value();
                    }
                } else if (mapIt.value() > minTopVal_Int) {
                    topN_StringInt.poll();
                    topN_StringInt.offer(new ObjectIntPair(mapIt.key(), mapIt.value()));
                    minTopVal_Int = topN_StringInt.peek().value;
                }
            }
        }
    };

    public void accumulateStringFloatMaps(Iterator<TObjectFloatHashMap> it) {
        if (topN_StringFloat == null) {
            topN_StringFloat = new PriorityQueue<ObjectFloatPair>(numFeatures);
        }

        while (it.hasNext()) {
            TObjectFloatIterator mapIt = it.next().iterator();
            while (mapIt.hasNext()) {
                mapIt.advance();
                if (topN_StringFloat.size() < numFeatures) {
                    topN_StringFloat.offer(new ObjectFloatPair(mapIt.key(), mapIt.value()));
                    if (mapIt.value() < minTopVal_Float) {
                        minTopVal_Float = mapIt.value();
                    }
                } else if (mapIt.value() > minTopVal_Float) {
                    topN_StringFloat.poll();
                    topN_StringFloat.offer(new ObjectFloatPair(mapIt.key(), mapIt.value()));
                    minTopVal_Float = topN_StringFloat.peek().value;
                }
            }
        }
    };
}
