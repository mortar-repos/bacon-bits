package com.mortardata.pig.recsys;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.pig.Algebraic;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.ImmutableList;
import gnu.trove.map.hash.TIntObjectHashMap;

public class SelectShortestPaths extends EvalFunc<DataBag>
                                 implements Accumulator<DataBag>, Algebraic {
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>(3);
            tupleFields.add(new Schema.FieldSchema("dest", DataType.INTEGER));
            tupleFields.add(new Schema.FieldSchema("dist", DataType.FLOAT));
            tupleFields.add(new Schema.FieldSchema("steps", DataType.INTEGER));

            return new Schema(
                new Schema.FieldSchema("shortest_paths",
                    new Schema(
                        new Schema.FieldSchema(null,
                            new Schema(tupleFields),
                        DataType.TUPLE)),
                DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Path implements Comparable<Path> {
        private int dest;
        private float dist;
        private byte steps;

        private Path(int dest, float dist, byte steps) {
            this.dest = dest;
            this.dist = dist;
            this.steps = steps;
        }

        public void noteAlternatePath(Path other) {
            if (other.steps < steps) {
                steps = other.steps;
            }
        }

        public int compareTo(Path other) {
            // intentionally reversed,
            // we want the farthest paths to be at the head of the PriorityQueue
            if (dist < other.dist) {
                return 1;
            } else if (dist > other.dist) {
                return -1;
            } else {
                return 0;
            }
        }

        public int hashCode() {
            return dest;
        }
    }

    private int nhoodSize = 20;
    private PriorityQueue<Path> pq;
    private TIntObjectHashMap<Path> index;

    public SelectShortestPaths() {
        cleanup();
    }

    public DataBag getValue() {
        DataBag outputBag = bf.newDefaultBag();
        Path p;
        while ((p = pq.poll()) != null) {
            ArrayList<Object> fields = new ArrayList<Object>(3);
            fields.add(new Integer(p.dest));
            fields.add(new Float(p.dist));
            fields.add(new Integer(p.steps));
            outputBag.add(tf.newTupleNoCopy(fields));
        }
        return outputBag;
    }

    public void cleanup() {
        pq = new PriorityQueue<Path>(nhoodSize);
        index = new TIntObjectHashMap<Path>(nhoodSize);
    }

    public DataBag exec(Tuple input) throws IOException {
        accumulate(input);
        DataBag out = getValue();
        cleanup();
        return out; 
    }

    public void accumulate(Tuple input) throws IOException {
        DataBag inputBag = (DataBag) input.get(0);
        accumulate(inputBag, 0, nhoodSize, pq, index);
    }

    private static void accumulate(DataBag inputBag,
                                   int dataIdx,
                                   int nhoodSize,
                                   PriorityQueue<Path> pq,
                                   TIntObjectHashMap<Path> index)
                                   throws IOException {
        for (Tuple t : inputBag) {
            DataByteArray paths = (DataByteArray) t.get(dataIdx);

            ByteArrayInputStream bais = new ByteArrayInputStream(paths.get());
            DataInput in = new DataInputStream(bais);
            short sz = in.readShort();

            for (short i = 0; i < sz; i++) {
                int dest = in.readInt();
                float dist = in.readFloat();
                byte steps = in.readByte();
                Path curPath = new Path(dest, dist, steps);
                Path indexedPath = index.get(dest);

                if (indexedPath == null) {
                    updateWithNonPresentPath(curPath, nhoodSize, pq, index);
                } else {
                    updateWithPresentPath(curPath, indexedPath, pq, index);
                }
            }
        }
    }

    private static void updateWithNonPresentPath(Path curPath,
                                                 int nhoodSize,
                                                 PriorityQueue<Path> pq,
                                                 TIntObjectHashMap<Path> index)
                                                 throws IOException {
        if (pq.size() < nhoodSize) {
            pq.offer(curPath);
            index.put(curPath.dest, curPath);
        } else {
            Path farthestPath = pq.poll();
            if (curPath.dist < farthestPath.dist) {
                pq.offer(curPath);
                index.remove(farthestPath.dest);
                index.put(curPath.dest, curPath);
            } else {
                pq.offer(farthestPath);
            }
        }
    }

    private static void updateWithPresentPath(Path curPath,
                                              Path indexedPath,
                                              PriorityQueue<Path> pq,
                                              TIntObjectHashMap<Path> index)
                                              throws IOException {
        if (curPath.dist < indexedPath.dist) {
            pq.remove(indexedPath); // linear time :(
            curPath.noteAlternatePath(indexedPath);
            pq.offer(curPath);

            index.remove(indexedPath.dest);
            index.put(curPath.dest, curPath);
        } else {
            indexedPath.noteAlternatePath(curPath);
        }
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        // input schema: single tuple bag {nhood_size: int, paths: bytearray}
        // output schema: (nhood_size: int, paths: bytearray)
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() != 2) {
                return null;
            } else {
                ArrayList<Object> fields = new ArrayList<Object>(2);
                fields.add(input.get(0));
                Iterator<Tuple> it = ((DataBag) input.get(1)).iterator();
                if (!it.hasNext()) {
                    return null;
                }
                fields.add(it.next().get(0));
                return tf.newTupleNoCopy(fields);
            }
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {
        // input schema: {nhood_size: int, paths: bytearray}
        // output schema: {nhood_size: int, paths: bytearray}
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0) {
                return null;
            }
            
            DataBag inputBag = (DataBag) input.get(0);
            Iterator<Tuple> it = inputBag.iterator();
            if (!it.hasNext()) {
                return null;
            }

            Tuple peek = it.next();
            int nhoodSize = (Integer) peek.get(0);
            PriorityQueue<Path> pq = new PriorityQueue<Path>(nhoodSize);
            TIntObjectHashMap<Path> index = new TIntObjectHashMap<Path>(nhoodSize);

            accumulate(inputBag, 1, nhoodSize, pq, index);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput local_paths = new DataOutputStream(baos);
            long numPaths = pq.size();

            if (numPaths > Short.MAX_VALUE) {
                throw new RuntimeException(
                    "SelectShortestPaths does not support following > " +
                    Short.MAX_VALUE + " paths for any one item"
                );
            }

            local_paths.writeShort((short) numPaths);

            Path p;
            while ((p = pq.poll()) != null) {
                local_paths.writeInt(p.dest);
                local_paths.writeFloat(p.dist);
                local_paths.writeByte(p.steps);
            }

            DataByteArray local_paths_dba = new DataByteArray(baos.toByteArray());
            ArrayList<Object> fields = new ArrayList<Object>(2);
            fields.add(new Integer(nhoodSize));
            fields.add(local_paths_dba);
            return tf.newTupleNoCopy(fields);
        }
    }
    
    static public class Final extends EvalFunc<DataBag> {
        // input schema: {nhood_size: int, paths: bytearray}
        // output schema: {item_B: int, dist: float, steps: int}
        public DataBag exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0) {
                return null;
            }
            
            DataBag inputBag = (DataBag) input.get(0);
            Iterator<Tuple> it = inputBag.iterator();
            if (!it.hasNext()) {
                return null;
            }

            Tuple peek = it.next();
            int nhoodSize = (Integer) peek.get(0);
            PriorityQueue<Path> pq = new PriorityQueue<Path>(nhoodSize);
            TIntObjectHashMap<Path> index = new TIntObjectHashMap<Path>(nhoodSize);

            accumulate(inputBag, 1, nhoodSize, pq, index);
            DataBag outputBag = bf.newDefaultBag();
            Path p;
            while ((p = pq.poll()) != null) {
                ArrayList<Object> fields = new ArrayList<Object>(3);
                fields.add(new Integer(p.dest));
                fields.add(new Float(p.dist));
                fields.add(new Integer(p.steps));
                outputBag.add(tf.newTupleNoCopy(fields));
            }
            return outputBag;
        }
    }
}
