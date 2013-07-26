package com.mortardata.pig.util;

import junit.framework.Assert;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class TestIN {

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testIN() throws IOException {
        IN funcTwoItem = new IN("123,567");
        Assert.assertTrue(funcTwoItem.exec(tupleFactory.newTuple(Arrays.asList("123"))));
        Assert.assertTrue(funcTwoItem.exec(tupleFactory.newTuple(Arrays.asList("567"))));
        Assert.assertFalse(funcTwoItem.exec(tupleFactory.newTuple(Arrays.asList("20"))));

        IN funcOneItem = new IN("123");
        Assert.assertTrue(funcOneItem.exec(tupleFactory.newTuple(Arrays.asList("123"))));
        Assert.assertFalse(funcOneItem.exec(tupleFactory.newTuple(Arrays.asList("20"))));
    }

}
