package com.mortardata.pig.partitioners;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

public class LoadBalancingReducerAllocation extends EvalFunc<DataBag> implements Accumulator<DataBag> {
    private static TupleFactory tf = TupleFactory.getInstance();
    private static BagFactory bf = BagFactory.getInstance();
    private static final Log log = LogFactory.getLog(LoadBalancingReducerAllocation.class);

    public Schema outputSchema(Schema input) {
        try {
            Schema.FieldSchema keyField = input.getField(0).schema.getField(0).schema.getField(0);

            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>(2);
            tupleFields.add(keyField);
            tupleFields.add(new Schema.FieldSchema("reducer", DataType.INTEGER));

            return new Schema(
                new Schema.FieldSchema("reducer_allocations",
                    new Schema(
                        new Schema.FieldSchema("t",
                            new Schema(tupleFields),
                        DataType.TUPLE)
                    ),
                DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public class ReducerAllocation implements Comparable<ReducerAllocation> {
        public int reducerNum;
        public Set<Object> keys;
        public long numItems;

        public ReducerAllocation(int reducerNum, Object initKey, long initNumItems) {
            this.reducerNum = reducerNum;
            this.keys = new HashSet<Object>();
            this.keys.add(initKey);
            this.numItems = initNumItems;
        }

        public int compareTo(ReducerAllocation other) {
            if      (numItems < other.numItems) { return -1; }
            else if (numItems > other.numItems) { return +1; }
                                          else  { return  0; }
        }
    }

    private DataBag outputBag;
    private Configuration jobConf;
    private int maxNumReducers;
    private int numAllocatedReducers;
    private PriorityQueue<ReducerAllocation> allocations;
    
    public LoadBalancingReducerAllocation() {
        cleanup();
    }

    public void cleanup() {
        outputBag = bf.newDefaultBag();
        jobConf = null;
        allocations = new PriorityQueue<ReducerAllocation>();
    }

    public DataBag getValue() {
        ReducerAllocation[] finalAllocations = allocations.toArray(new ReducerAllocation[0]);
        Arrays.sort(finalAllocations);

        log.info("Reducer with maximum load has " + 
                 finalAllocations[finalAllocations.length-1].numItems + " items");
        log.info("Reducer with minimum load has " +
                 finalAllocations[0].numItems + " items");

        return outputBag;
    }

    public DataBag exec(Tuple input) {
        accumulate(input);
        DataBag out = getValue();
        cleanup();
        return out;
    }

    public void accumulate(Tuple input) {
        try {
            DataBag inputBag = (DataBag) input.get(0);

            if (jobConf == null) {
                jobConf = UDFContext.getUDFContext().getJobConf();
                maxNumReducers = jobConf.getInt("pig.exec.reducers.max", 999);
                if (maxNumReducers == 2) {
                    maxNumReducers = 1; // reverse a hack we did for mortar local dev mode
                }
                numAllocatedReducers = 0;
            }

            for (Tuple t : inputBag) {
                Tuple t2 = tf.newTuple(2);
                t2.set(0, t.get(0));

                if (numAllocatedReducers < maxNumReducers) {
                    allocations.offer(new ReducerAllocation(numAllocatedReducers, t.get(0), (Long) t.get(1)));
                    t2.set(1, new Integer(numAllocatedReducers));
                    numAllocatedReducers++;
                } else {
                    ReducerAllocation lightestLoad = allocations.poll();
                    lightestLoad.keys.add(t.get(0));
                    lightestLoad.numItems += (Long) t.get(1);
                    allocations.offer(lightestLoad);
                    t2.set(1, new Integer(lightestLoad.reducerNum));
                }

                outputBag.add(t2);
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }
}