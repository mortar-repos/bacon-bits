package com.mortardata.pig.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * SQL IN for Pig.
 *
 * Example Filter function: returns Boolean, can only be used in a FILTER statement.
 *
 * Ex:
 *
 * DEFINE IN com.mortardata.pig.util.IN('3,5,7');
 *
 * FILTER my_relation by IN(first_field);
 */
public class IN extends FilterFunc {

    private static final String DELIMITER = ",";

    private Set<String> values;

    public IN(String valuesStr) {
        String[] splitValues = valuesStr.split(DELIMITER);
        values = new HashSet<String>(Arrays.asList(splitValues));
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        // expect one string
        if (input == null || input.size() != 1) {
            throw new IllegalArgumentException("IN: requires one input parameter.");
        }
        try {
            String value = (String) input.get(0);
            return values.contains(value);
        }
        catch (ExecException e) {
            throw new IOException("IN: caught exception processing input.", e);
        }

    }
}
