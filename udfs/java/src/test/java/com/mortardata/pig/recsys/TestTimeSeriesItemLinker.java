package com.mortardata.pig.recsys;

import junit.framework.Assert;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

public class TestTimeSeriesItemLinker {

    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testTimeSeriesItemLinker() throws IOException {
        DataBag input = bagFactory.newDefaultBag();
        input.add(tupleFactory.newTuple(Arrays.asList("a",1)));
        input.add(tupleFactory.newTuple(Arrays.asList("b",2)));
        input.add(tupleFactory.newTuple(Arrays.asList("c",3)));
        input.add(tupleFactory.newTuple(Arrays.asList("d",4)));
        input.add(tupleFactory.newTuple(Arrays.asList("e",5)));
        input.add(tupleFactory.newTuple(Arrays.asList("f",6)));

        Schema inputSchema = Utils.getSchemaFromString("items:{(item:chararray, thing:int)}");
        TimeSeriesItemLinker linker = new TimeSeriesItemLinker("3");
        
        DataBag result = linker.exec(tupleFactory.newTuple(input));

        HashMap<String, ArrayList<String>> map = new HashMap<String,ArrayList<String>>();
        map.put("a", new ArrayList(Arrays.asList("b","c","d")));
        map.put("b", new ArrayList(Arrays.asList("a","c","d","e")));
        map.put("c", new ArrayList(Arrays.asList("a","b","d","e","f")));
        map.put("d", new ArrayList(Arrays.asList("a","b","c","e","f")));
        map.put("e", new ArrayList(Arrays.asList("b","c","d","f")));
        map.put("f", new ArrayList(Arrays.asList("c","d","e")));

        HashMap<String, Object> metadata = new HashMap<String,Object>();
        metadata.put("a", 1);
        metadata.put("b", 2);
        metadata.put("c", 3);
        metadata.put("d", 4);
        metadata.put("e", 5);
        metadata.put("f", 6);
        
        for (Tuple linked : result) {
            String nodeA = (String)linked.get(0);
            String nodeB = (String)linked.get(2);
            
            List<String> allowedLinks = map.get(nodeA);
            Assert.assertTrue(allowedLinks.contains(nodeB));

            Object nodeAmeta = linked.get(1);
            Object nodeAmetaExpected = metadata.get(nodeA);
            Assert.assertEquals(nodeAmeta, nodeAmetaExpected);

            Object nodeBmeta = linked.get(3);
            Object nodeBmetaExpected = metadata.get(nodeB);
            Assert.assertEquals(nodeBmeta, nodeBmetaExpected);
        }
    }
}
