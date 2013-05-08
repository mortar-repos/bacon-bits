package com.mortardata.pig;


import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class EuclideanDistance extends EvalFunc<DataBag> {

    public int limit = 10;

    public EuclideanDistance(String limit) {
        this.limit = Integer.parseInt(limit);
    }


    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1) {
            throw new IllegalArgumentException("EuclideanDistance: requires one databag as input.");
        }

        HashMap<String, DistanceSet> distances = new HashMap<String, DistanceSet>();
        int i = 0;
        for (Tuple t1 : (DataBag) input.get(0)) {

            String id1 = DataType.toString(t1.get(0));
            Tuple tuple1 = DataType.toTuple(t1.get(1));
            List weights = DataType.toTuple(t1.get(2)).getAll();

            int j = 0;
            for (Tuple t2 : (DataBag) input.get(0)) {
                if (j > i) {
                    String id2 = DataType.toString(t2.get(0));
                    Tuple tuple2 = DataType.toTuple(t2.get(1));

                    double dist = computeDistance(tuple1, tuple2, weights);
                    DistanceSet dSet1 = addObject(id2, dist, distances.get(id1));
                    distances.put(id1, dSet1);

                    DistanceSet dSet2 = addObject(id1, dist, distances.get(id2));
                    distances.put(id2, dSet2);

                }
                j++;
            }
            i++;
        }

        DataBag output = bagFactory.newDefaultBag();
        for (String id1 : distances.keySet()) {
            for (DistanceObject obj : distances.get(id1).getObjects()) {
                List l = new ArrayList<Object>();
                l.add(id1);
                l.add(obj.getId());
                l.add(obj.getDistance());
                output.add(tupleFactory.newTuple(l));
            }
        }
        return output;
    }

    private DistanceSet addObject(String id2, double distance, DistanceSet dSet) {
        DistanceObject obj = new DistanceObject(id2, distance);

        if (dSet == null) {
            dSet = new DistanceSet();
        }
        dSet.addObject(obj);
        return dSet;
    }


    private double computeDistance(Tuple tuple1, Tuple tuple2, List weights) throws ExecException {
        double total = 0;
        for (int i = 0; i < tuple1.size(); i++) {
            double val1 = DataType.toDouble(tuple1.get(i));
            double val2 = DataType.toDouble(tuple2.get(i));
            double diff = Math.pow(val1 - val2, 2) ;
            if (weights != null) {
                    diff = diff * ((Number) weights.get(i)).doubleValue();
            }
            total += diff;
        }
        return Math.sqrt(total);

    }

    public Schema outputSchema(Schema input) {
        try{
            Schema bagSchema = new Schema();
            bagSchema.add(new Schema.FieldSchema("user_id1", DataType.CHARARRAY));
            bagSchema.add(new Schema.FieldSchema("user_id2", DataType.CHARARRAY));
            bagSchema.add(new Schema.FieldSchema("distance", DataType.DOUBLE));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                    bagSchema, DataType.BAG));
        }catch (Exception e){
            return null;
        }
    }

    private class DistanceSet {

        private double maxDistance = Double.MIN_VALUE;



        private List<DistanceObject> objects = new ArrayList<DistanceObject>();

        public DistanceSet() {

        }

        public void addObject(DistanceObject obj) {
            if (objects.size() < limit) {
                objects.add(obj);
                maxDistance = Math.max(maxDistance, obj.getDistance());
            }
            else if (obj.getDistance() < maxDistance) {
                Collections.sort(objects);
                objects.remove(limit - 1);
                objects.add(obj);
                maxDistance = Math.max(obj.getDistance(), objects.get(limit - 2).getDistance());
            }
        }

        private List<DistanceObject> getObjects() {
            return objects;
        }
    }


    private class DistanceObject implements Comparable<DistanceObject> {
        private final String id;
        private final double distance;

        public DistanceObject(String id, double distance) {
            this.id = id;
            this.distance = distance;
        }

        private String getId() {
            return id;
        }

        private double getDistance() {
            return distance;
        }


        public int compareTo(DistanceObject compareObject)
        {
            if (getDistance() < compareObject.getDistance())
                return -1;
            else if (getDistance() == compareObject.getDistance())
                return 0;
            else
                return 1;
        }
    }
}
