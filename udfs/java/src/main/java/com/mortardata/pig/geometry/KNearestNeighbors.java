package com.mortardata.pig.geometry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.mortardata.pig.collections.AccumulatorCollectionEvalFuncWithItemIds;
import com.mortardata.pig.collections.PigCollection;
import com.mortardata.pig.geometry.Distance;

/*
 * Input schema: {(key: int/string, features: PigCollection serialized as bytearray)}
 * Output schema: {(key: int/string, knn: {(key: int, distance: float)})} 
 *
 * Field names do not have to be the same as listed here
 * Output item key field name will be the same as the input
 */
public class KNearestNeighbors extends AccumulatorCollectionEvalFuncWithItemIds<DataBag> {
    private static final Log log = LogFactory.getLog(KNearestNeighbors.class);
    private static final BagFactory bf = BagFactory.getInstance();
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final Distance distanceCalc = new Distance();

    private int k;
    public KNearestNeighbors(String kStr) {
        k = Integer.parseInt(kStr);
    }

    public class ItemWithFeatures {
        public int id;
        public Object features;

        public ItemWithFeatures(int id, Object features) {
            this.id = id;
            this.features = features;
        }

        public int hashCode() {
            return id;
        }
    }

    public class ItemIdAndDistance implements Comparable<ItemIdAndDistance> {
        public int id;
        public float distance;

        public ItemIdAndDistance(int id, float distance) {
            this.id = id;
            this.distance = distance;
        }

        public int compareTo(ItemIdAndDistance other) {
            // smaller distance is "greater",
            // so larger distaces are at the head of the Priority Queue (see notes with KNNInfo)
            if (distance < other.distance) {
                return 1;
            } else if (distance > other.distance) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    public class KNNInfo {
        public float maxNNDistance;
        public PriorityQueue<ItemIdAndDistance> queue;

        public KNNInfo() {
            maxNNDistance = Float.MIN_VALUE;
            queue = new PriorityQueue<ItemIdAndDistance>();
        }

        public void suggestItem(int id, float distance) {
            if (queue.size() < k) {
                // for the first k items, just add to the queue
                // make note of the farthest of these
                queue.offer(new ItemIdAndDistance(id, distance));
                if (distance > maxNNDistance) {
                    maxNNDistance = distance;
                }
            } else if (distance < maxNNDistance) {
                // now if you find another item that's closer than the farthest in the top k,
                // replace that item with it and find the new farthest in the top k
                // the PQ keeps the *farthest* item at the head for this reason (see compareTo impl above)
                queue.poll();
                queue.offer(new ItemIdAndDistance(id, distance));
                maxNNDistance = queue.peek().distance;
            }
        }
    }

    private Map<ItemWithFeatures, KNNInfo> knn;
    private DataBag output;

    public KNearestNeighbors() {
        cleanup();
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema.FieldSchema keyField = input.getField(0).schema.getField(0).schema.getField(0);

            ArrayList<Schema.FieldSchema> outputTupleFields = new ArrayList<Schema.FieldSchema>(1);
            outputTupleFields.add(keyField);
            outputTupleFields.add(new Schema.FieldSchema("knn", DataType.BYTEARRAY));

            return new Schema(
                new Schema.FieldSchema("knn",
                    new Schema(
                        new Schema.FieldSchema("t",
                            new Schema(outputTupleFields),
                        DataType.TUPLE)
                    ),
                DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public DataBag getValue() {
        try {
            output = bf.newDefaultBag();
            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                Tuple t = tf.newTuple(2);
                
                t.set(0, e.getKey().id);

                TIntFloatHashMap map = new TIntFloatHashMap();
                ItemIdAndDistance[] vals = e.getValue().queue.toArray(new ItemIdAndDistance[0]);
                for (ItemIdAndDistance val : vals) {
                    map.put(val.id, val.distance);
                }

                t.set(1, PigCollection.serialize(map));

                output.add(t);
            }
            return output;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanup() {
        knn = new HashMap<ItemWithFeatures, KNNInfo>();
        output = bf.newDefaultBag();
    }

    public void accumulateIntArrays(Iterator<ImmutablePair<Integer, PigCollection.IntArray>> it) throws ExecException {
        if (knn == null) { knn = new HashMap<ItemWithFeatures, KNNInfo>(); }
        while (it.hasNext()) {
            ImmutablePair<Integer, PigCollection.IntArray> p = it.next();
            int thisItemId = p.getKey();
            int[] theseFeatures = p.getValue().array;
            KNNInfo infoForThisItem = new KNNInfo();

            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                ItemWithFeatures otherItem = e.getKey();
                KNNInfo infoForOtherItem = e.getValue();

                int[] thoseFeatures = ((PigCollection.IntArray) otherItem.features).array;
                float distance = distanceCalc.execOnIntArrays(theseFeatures, thoseFeatures);

                infoForThisItem.suggestItem(otherItem.id, distance);
                infoForOtherItem.suggestItem(thisItemId, distance);
            }

            knn.put(new ItemWithFeatures(thisItemId, theseFeatures), infoForThisItem);
        }
    }

    public void accumulateFloatArrays(Iterator<ImmutablePair<Integer, PigCollection.FloatArray>> it) throws ExecException {
        if (knn == null) { knn = new HashMap<ItemWithFeatures, KNNInfo>(); }
        while (it.hasNext()) {
            ImmutablePair<Integer, PigCollection.FloatArray> p = it.next();
            int thisItemId = p.getKey();
            float[] theseFeatures = p.getValue().array;
            KNNInfo infoForThisItem = new KNNInfo();

            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                ItemWithFeatures otherItem = e.getKey();
                KNNInfo infoForOtherItem = e.getValue();

                float[] thoseFeatures = ((PigCollection.FloatArray) otherItem.features).array;
                float distance = distanceCalc.execOnFloatArrays(theseFeatures, thoseFeatures);

                infoForThisItem.suggestItem(otherItem.id, distance);
                infoForOtherItem.suggestItem(thisItemId, distance);
            }

            knn.put(new ItemWithFeatures(thisItemId, theseFeatures), infoForThisItem);
        }
    }

    public void accumulateIntIntMaps(Iterator<ImmutablePair<Integer, TIntIntHashMap>> it) throws ExecException {
        if (knn == null) { knn = new HashMap<ItemWithFeatures, KNNInfo>(); }
        while (it.hasNext()) {
            ImmutablePair<Integer, TIntIntHashMap> p = it.next();
            int thisItemId = p.getKey();
            TIntIntHashMap theseFeatures = p.getValue();
            KNNInfo infoForThisItem = new KNNInfo();

            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                ItemWithFeatures otherItem = e.getKey();
                KNNInfo infoForOtherItem = e.getValue();

                TIntIntHashMap thoseFeatures = (TIntIntHashMap) otherItem.features;
                float distance = distanceCalc.execOnIntIntMaps(theseFeatures, thoseFeatures);

                infoForThisItem.suggestItem(otherItem.id, distance);
                infoForOtherItem.suggestItem(thisItemId, distance);
            }

            knn.put(new ItemWithFeatures(thisItemId, theseFeatures), infoForThisItem);
        }
    }

    public void accumulateIntFloatMaps(Iterator<ImmutablePair<Integer, TIntFloatHashMap>> it) throws ExecException {
        if (knn == null) { knn = new HashMap<ItemWithFeatures, KNNInfo>(); }
        while (it.hasNext()) {
            ImmutablePair<Integer, TIntFloatHashMap> p = it.next();
            int thisItemId = p.getKey();
            TIntFloatHashMap theseFeatures = p.getValue();
            KNNInfo infoForThisItem = new KNNInfo();

            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                ItemWithFeatures otherItem = e.getKey();
                KNNInfo infoForOtherItem = e.getValue();

                TIntFloatHashMap thoseFeatures = (TIntFloatHashMap) otherItem.features;
                float distance = distanceCalc.execOnIntFloatMaps(theseFeatures, thoseFeatures);

                infoForThisItem.suggestItem(otherItem.id, distance);
                infoForOtherItem.suggestItem(thisItemId, distance);
            }

            knn.put(new ItemWithFeatures(thisItemId, theseFeatures), infoForThisItem);
        }
    }

    public void accumulateStringIntMaps(Iterator<ImmutablePair<Integer, TObjectIntHashMap>> it) throws ExecException {
        if (knn == null) { knn = new HashMap<ItemWithFeatures, KNNInfo>(); }
        while (it.hasNext()) {
            ImmutablePair<Integer, TObjectIntHashMap> p = it.next();
            int thisItemId = p.getKey();
            TObjectIntHashMap theseFeatures = p.getValue();
            KNNInfo infoForThisItem = new KNNInfo();

            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                ItemWithFeatures otherItem = e.getKey();
                KNNInfo infoForOtherItem = e.getValue();

                TObjectIntHashMap thoseFeatures = (TObjectIntHashMap) otherItem.features;
                float distance = distanceCalc.execOnStringIntMaps(theseFeatures, thoseFeatures);

                infoForThisItem.suggestItem(otherItem.id, distance);
                infoForOtherItem.suggestItem(thisItemId, distance);
            }

            knn.put(new ItemWithFeatures(thisItemId, theseFeatures), infoForThisItem);
        }
    }

    public void accumulateStringFloatMaps(Iterator<ImmutablePair<Integer, TObjectFloatHashMap>> it) throws ExecException {
        if (knn == null) { knn = new HashMap<ItemWithFeatures, KNNInfo>(); }
        while (it.hasNext()) {
            ImmutablePair<Integer, TObjectFloatHashMap> p = it.next();
            int thisItemId = p.getKey();
            TObjectFloatHashMap theseFeatures = p.getValue();
            KNNInfo infoForThisItem = new KNNInfo();

            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                ItemWithFeatures otherItem = e.getKey();
                KNNInfo infoForOtherItem = e.getValue();

                TObjectFloatHashMap thoseFeatures = (TObjectFloatHashMap) otherItem.features;
                float distance = distanceCalc.execOnStringFloatMaps(theseFeatures, thoseFeatures);

                infoForThisItem.suggestItem(otherItem.id, distance);
                infoForOtherItem.suggestItem(thisItemId, distance);
            }

            knn.put(new ItemWithFeatures(thisItemId, theseFeatures), infoForThisItem);
        };
    }

    public void accumulateIntSets(Iterator<ImmutablePair<Integer, TIntHashSet>> it) throws ExecException {
        if (knn == null) { knn = new HashMap<ItemWithFeatures, KNNInfo>(); }
        while (it.hasNext()) {
            ImmutablePair<Integer, TIntHashSet> p = it.next();
            int thisItemId = p.getKey();
            TIntHashSet theseFeatures = p.getValue();
            KNNInfo infoForThisItem = new KNNInfo();

            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                ItemWithFeatures otherItem = e.getKey();
                KNNInfo infoForOtherItem = e.getValue();

                TIntHashSet thoseFeatures = (TIntHashSet) otherItem.features;
                float distance = distanceCalc.execOnIntSets(theseFeatures, thoseFeatures);

                infoForThisItem.suggestItem(otherItem.id, distance);
                infoForOtherItem.suggestItem(thisItemId, distance);
            }

            knn.put(new ItemWithFeatures(thisItemId, theseFeatures), infoForThisItem);
        }
    }

    public void accumulateStringSets(Iterator<ImmutablePair<Integer, Set<String>>> it) throws ExecException {
        if (knn == null) { knn = new HashMap<ItemWithFeatures, KNNInfo>(); }
        while (it.hasNext()) {
            ImmutablePair<Integer, Set<String>> p = it.next();
            int thisItemId = p.getKey();
            Set<String> theseFeatures = p.getValue();
            KNNInfo infoForThisItem = new KNNInfo();

            for (Map.Entry<ItemWithFeatures, KNNInfo> e : knn.entrySet()) {
                ItemWithFeatures otherItem = e.getKey();
                KNNInfo infoForOtherItem = e.getValue();

                Set<String> thoseFeatures = (Set<String>) otherItem.features;
                float distance = distanceCalc.execOnStringSets(theseFeatures, thoseFeatures);

                infoForThisItem.suggestItem(otherItem.id, distance);
                infoForOtherItem.suggestItem(thisItemId, distance);
            }

            knn.put(new ItemWithFeatures(thisItemId, theseFeatures), infoForThisItem);
        }
    }
}