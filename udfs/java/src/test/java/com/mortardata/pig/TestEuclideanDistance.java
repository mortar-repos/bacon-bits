package com.mortardata.pig;


import junit.framework.Assert;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class TestEuclideanDistance {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();



    @Test
    public void testDistance() throws IOException {

        DataBag dataBag = bagFactory.newDefaultBag();

        EuclideanDistance func = new EuclideanDistance("1");

        Tuple tuple1 = tupleFactory.newTuple(Arrays.asList(new Double[]{3.0, 6.0}));
        Tuple tuple2 = tupleFactory.newTuple(Arrays.asList(new Double[]{5.0, 5.0}));
        Tuple tuple3 = tupleFactory.newTuple(Arrays.asList(new Double[]{6.0, 10.0}));
        Tuple weights = tupleFactory.newTuple(Arrays.asList(new Double[]{2.0, 8.0}));

        Tuple input = tupleFactory.newTuple(Arrays.asList(new Object[]{"id1", tuple1, weights}));
        Tuple input2 = tupleFactory.newTuple(Arrays.asList(new Object[]{"id2", tuple2, weights}));
        Tuple input3 = tupleFactory.newTuple(Arrays.asList(new Object[]{"id3", tuple3, weights}));

        dataBag.add(input);
        dataBag.add(input2);
        dataBag.add(input3);

        DataBag expected = bagFactory.newDefaultBag();
        expected.add(tupleFactory.newTuple(Arrays.asList(new Object[]{"id1", "id2", 4.0})));
        expected.add(tupleFactory.newTuple(Arrays.asList(new Object[]{"id2", "id1", 4.0})));
        expected.add(tupleFactory.newTuple(Arrays.asList(new Object[]{"id3", "id1", 12.083045973594572})));

        DataBag output = func.exec(tupleFactory.newTuple(Collections.singletonList(dataBag)));

        Assert.assertEquals(expected, output);

    }

}
