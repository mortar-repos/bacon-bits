package com.mortardata.pig.util;

import junit.framework.Assert;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class TestTupleToBag {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();

    @Test
    public void testFlattenTuple() throws IOException {

        Tuple input =  tupleFactory.newTuple(Collections.singletonList(tupleFactory.newTuple(Arrays.asList("blog", "robot", "robotics"))));

        TupleToBag func = new TupleToBag();
        DataBag output = func.exec(input);

        DataBag expected = bagFactory.newDefaultBag();
        expected.add(tupleFactory.newTuple(Arrays.asList("blog")));
        expected.add(tupleFactory.newTuple(Arrays.asList("robot")));
        expected.add(tupleFactory.newTuple(Arrays.asList("robotics")));

        Assert.assertEquals(expected, output);
    }
}
