package com.mortardata.pig.partitioners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.PigNullableWritable;

public class PrecalculatedPartitioner extends Partitioner<PigNullableWritable, Writable> {

    public int getPartition(PigNullableWritable key, Writable value, int numPartitions) {
        try {
            return (Integer) ((Tuple) key.getValueAsPigType()).get(1);
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }
}
