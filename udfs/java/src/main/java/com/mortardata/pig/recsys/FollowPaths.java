package com.mortardata.pig.recsys;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

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

// input:  { graph: {(item_A: int, item_B: int, weight: float, steps: int)},
//           paths: {(item_A: int, item_B: int, weight: float)} }
//           where graph.item_B == paths.item_A
// output: { item_A: int, local_paths: bytearray }
//           local_paths is a serialized form of
//           {(item_B: int, dist: float, steps: byte)}
public class FollowPaths extends EvalFunc<DataBag> {
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>(2);
            tupleFields.add(new Schema.FieldSchema("item_A", DataType.INTEGER));
            tupleFields.add(new Schema.FieldSchema("local_paths", DataType.BYTEARRAY));

            return new Schema(
                new Schema.FieldSchema("followed_paths",
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

    public DataBag exec(Tuple input) throws IOException {
        DataBag graph = (DataBag) input.get(0);
        DataBag paths = (DataBag) input.get(1);
        DataBag outputBag = bf.newDefaultBag();

        for (Tuple graph_edge : graph) {
            int graph_dest = (Integer) graph_edge.get(1);
            float graph_dist = (Float) graph_edge.get(2);
            int graph_steps = (Integer) graph_edge.get(3);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput local_paths = new DataOutputStream(baos);
            long numPaths = paths.size();

            if (numPaths > Short.MAX_VALUE) {
                throw new RuntimeException(
                    "FollowPaths does not support following > " +
                    Short.MAX_VALUE + " paths for any one item"
                );
            }

            local_paths.writeShort((short) numPaths);

            for (Tuple path : paths) {
                int path_dest = (Integer) path.get(1);
                float path_dist = (Float) path.get(2);

                local_paths.writeInt(path_dest);
                local_paths.writeFloat(graph_dist + path_dist);
                local_paths.writeByte((byte) (graph_dest == path_dest ?
                                              graph_steps : graph_steps + 1));
            }

            DataByteArray local_paths_dba = new DataByteArray(baos.toByteArray());

            outputBag.add(
                tf.newTupleNoCopy(
                    ImmutableList.of(graph_edge.get(0), local_paths_dba)));
        }

        return outputBag;
    }
}
