package com.mortardata.pig.characterize;

import org.junit.Test;
import junit.framework.Assert;
import java.lang.System;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Collections;

import java.util.Properties;

public class TestExtractFields {
    
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();

    @Test
    public void testExtractFieldInfo() throws IOException {
        try {
            Integer f1 = 23;
            String  f2 = "A path may be opened from inside Kant's";
            HashMap f3 = new HashMap<String,Object>();
            f3.put("foo", tupleFactory.newTuple(Arrays.asList(1,2,3)));
            f3.put("bar", 5675);
            f3.put("baz", "qux");
            DataBag f4 = bagFactory.newDefaultBag();
            f4.add(tupleFactory.newTuple(Arrays.asList(1)));
            f4.add(tupleFactory.newTuple(Arrays.asList(2)));
            f4.add(tupleFactory.newTuple(Arrays.asList(3)));
            Tuple   f5 = tupleFactory.newTuple(Arrays.asList("correlational", "circle."));

            Tuple input = tupleFactory.newTuple(Arrays.asList(f1, f2, f3, f4, f5));
        
            ExtractFields extractor = new ExtractFields();

            Properties props = new Properties();
            props.setProperty("$0", "f1");
            props.setProperty("$1", "f2");
            props.setProperty("$2", "f3");
            props.setProperty("$3", "f4");
            props.setProperty("$4", "f5");
            DataBag output = extractor.execProps(input, props);

            DataBag expected = bagFactory.newDefaultBag();
            expected.add(tupleFactory.newTuple(Arrays.asList("f1", "int", new Double(23), "23")));
            expected.add(tupleFactory.newTuple(Arrays.asList("f2", "chararray", new Double(f2.length()), f2)));
            expected.add(tupleFactory.newTuple(Arrays.asList("f3.baz", "chararray", new Double(3), f3.get("baz"))));
            expected.add(tupleFactory.newTuple(Arrays.asList("f3.foo", "tuple", new Double(3), f3.get("foo").toString())));
            expected.add(tupleFactory.newTuple(Arrays.asList("f3.bar", "int", new Double(5675), "5675")));
            expected.add(tupleFactory.newTuple(Arrays.asList("f4", "bag", new Double(3), f4.toString())));
            expected.add(tupleFactory.newTuple(Arrays.asList("f5", "tuple", new Double(2), f5.toString())));

            Assert.assertEquals(output.toString(), expected.toString());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Test
    public void testBuildSchema() throws IOException {
        try {
            Schema input = new Schema();
            input.add(new Schema.FieldSchema("f1", DataType.INTEGER));
            input.add(new Schema.FieldSchema("f2", DataType.CHARARRAY));
            input.add(new Schema.FieldSchema("f3", DataType.MAP));
            input.add(new Schema.FieldSchema("f4", DataType.BAG));
            input.add(new Schema.FieldSchema("f5", DataType.TUPLE));
            
            ExtractFields extractor = new ExtractFields();
            Schema output = extractor.outputSchema(input);
            Properties output_props = UDFContext.getUDFContext().getUDFProperties(extractor.getClass());
            
            Schema expected = null;
            Schema bagSchema = new Schema();
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("keyname", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("type", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("val", DataType.DOUBLE));
            tupleSchema.add(new Schema.FieldSchema("orig", DataType.CHARARRAY));

            bagSchema.add(new Schema.FieldSchema("field", tupleSchema, DataType.TUPLE));
            expected = new Schema(new Schema.FieldSchema("fields", bagSchema, DataType.BAG));

            Properties expected_props = new Properties();
            expected_props.setProperty("$0", "f1");
            expected_props.setProperty("$1", "f2");
            expected_props.setProperty("$2", "f3");
            expected_props.setProperty("$3", "f4");
            expected_props.setProperty("$4", "f5");
            
            Assert.assertEquals(expected, output);
            Assert.assertEquals(expected_props, output_props);

        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    
}
