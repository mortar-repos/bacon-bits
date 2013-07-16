package com.mortardata.pig.collections;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

/*
 * If given several fields, will encode them as a array-type PigCollection.
 * If given a single tuple field, will encode it as a array-type PigCollection.
 * If given a single bag field where each tuple has two fields, will encode it as a map-type PigCollection.
 * If given a single bag field where each tuple has one field, will encode it as a map-type PigCollection.
 */
public class ToPigCollection extends EvalFunc<DataByteArray> {

    public static final byte   INPUT_SEVERAL_FIELDS = 1;
    public static final byte   INPUT_TUPLE_FIELD    = 2;
    public static final byte   INPUT_BAG_FIELD      = 3;

    public static final String INPUT_TYPE_SIGNATURE      = "input_type";
    public static final String COLLECTION_TYPE_SIGNATURE = "collection_type";

    public Schema outputSchema(Schema input) {
        try {
            Properties prop = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            String outputAlias = null;

            if (input.size() == 1) {
                Schema.FieldSchema onlyField = input.getField(0);
                outputAlias = onlyField.alias;
                if (onlyField.type == DataType.TUPLE) {
                    prop.setProperty(INPUT_TYPE_SIGNATURE, new Byte(INPUT_TUPLE_FIELD).toString());
                    determineArrayCollectionType(onlyField.schema, prop);
                } else if (onlyField.type == DataType.BAG) {
                    prop.setProperty(INPUT_TYPE_SIGNATURE, new Byte(INPUT_BAG_FIELD).toString());

                    Schema tupleSchema = onlyField.schema.getField(0).schema;
                    if (tupleSchema.size() == 1) {
                        determineSetCollectionType(tupleSchema, prop);
                    } else if (tupleSchema.size() == 2) {
                        determineMapCollectionType(tupleSchema, prop);
                    } else {
                        throw new RuntimeException(
                            "Bag must have either single-element tuples (set) " +
                            "or two-element tuples (key, value) to be encoded as a PigArray."
                        );
                    }
                }
            } else {
                prop.setProperty(INPUT_TYPE_SIGNATURE, new Byte(INPUT_SEVERAL_FIELDS).toString());
                determineArrayCollectionType(input, prop);
            }

            return new Schema(new Schema.FieldSchema(
                outputAlias == null ? "pig_collection" : outputAlias,
                DataType.BYTEARRAY
            ));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public void determineArrayCollectionType(Schema input, Properties prop) throws FrontendException {
        byte type = input.getField(0).type;
                
        for (int i = 1; i < input.size(); i++) {
            if (type != input.getField(i).type) {
                throw new RuntimeException("All inputs must have the same type");
            }
        }

        if (type == DataType.INTEGER) {
            setArrayTypeProperty(prop, PigCollection.INT_ARRAY);
        } else if (type == DataType.FLOAT) {
            setArrayTypeProperty(prop, PigCollection.FLOAT_ARRAY);
        } else {
            throw new RuntimeException(
                "Recieved vector of unsupported schema. Should be ints or floats"
            );
        }
    }

    public void determineMapCollectionType(Schema input, Properties prop) throws FrontendException {
        byte keyType = input.getField(0).type;
        byte valueType = input.getField(1).type;

        if (keyType == DataType.INTEGER) {
            if (valueType == DataType.INTEGER) {
                setArrayTypeProperty(prop, PigCollection.INT_INT_MAP);
            } else if (valueType == DataType.FLOAT) {
                setArrayTypeProperty(prop, PigCollection.INT_FLOAT_MAP);
            } else {
                throw new RuntimeException("Value type for map-type PigCollection should be int or float");
            }
        } else if (keyType == DataType.CHARARRAY) {
            if (valueType == DataType.INTEGER) {
                setArrayTypeProperty(prop, PigCollection.STRING_INT_MAP);
            } else if (valueType == DataType.FLOAT) {
                setArrayTypeProperty(prop, PigCollection.STRING_FLOAT_MAP);
            } else {
                throw new RuntimeException("Value type for map-type PigCollection should be int or float");
            }
        } else {
            throw new RuntimeException(
                "Recieved bag of unsupported schema. " + 
                "Bags with two-element tuples must have schema {(int/chararray, int/float/float)}"
            );
        }
    }

    public void determineSetCollectionType(Schema input, Properties prop) throws FrontendException {
        byte type = input.getField(0).type;

        if (type == DataType.INTEGER) {
            setArrayTypeProperty(prop, PigCollection.INT_SET);
        } else if (type == DataType.CHARARRAY) {
            setArrayTypeProperty(prop, PigCollection.STRING_SET);
        } else {
            throw new RuntimeException(
                "Recieved bag of unsupported schema. " + 
                "Bags with two-element tuples must have schema {(int/chararray)}"
            );
        }
    }

    private void setArrayTypeProperty(Properties prop, byte type) {
        prop.setProperty(COLLECTION_TYPE_SIGNATURE, new Byte(type).toString());
    }

    public DataByteArray exec(Tuple input) throws IOException {
        try {
            Properties prop = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            byte inputType = Byte.parseByte(prop.getProperty(INPUT_TYPE_SIGNATURE));
            byte arrayType = Byte.parseByte(prop.getProperty(COLLECTION_TYPE_SIGNATURE));

            if (arrayType == PigCollection.INT_ARRAY) {
                Tuple t = getTupleToEncode(input, inputType);
                int arr[] = new int[t.size()];
                for (int i = 0; i < t.size(); i++) {
                    arr[i] = (Integer) t.get(i);
                }
                return PigCollection.serialize(arr);
            } else if (arrayType == PigCollection.FLOAT_ARRAY) {
                Tuple t = getTupleToEncode(input, inputType);
                float arr[] = new float[t.size()];
                for (int i = 0; i < t.size(); i++) {
                    arr[i] = (Float) t.get(i);
                }
                return PigCollection.serialize(arr);
            } else if (arrayType == PigCollection.INT_INT_MAP) {
                DataBag bag = (DataBag) input.get(0);
                TIntIntHashMap map = new TIntIntHashMap((int) bag.size());
                for (Tuple t : bag) {
                    map.put((Integer) t.get(0), (Integer) t.get(1));
                }
                return PigCollection.serialize(map);
            } else if (arrayType == PigCollection.INT_FLOAT_MAP) {
                DataBag bag = (DataBag) input.get(0);
                TIntFloatHashMap map = new TIntFloatHashMap((int) bag.size());
                for (Tuple t : bag) {
                    map.put((Integer) t.get(0), (Float) t.get(1));
                }
                return PigCollection.serialize(map);
            } else if (arrayType == PigCollection.STRING_INT_MAP) {
                DataBag bag = (DataBag) input.get(0);
                TObjectIntHashMap map = new TObjectIntHashMap((int) bag.size());
                for (Tuple t : bag) {
                    map.put((String) t.get(0), (Integer) t.get(1));
                }
                return PigCollection.serialize(map);
            } else if (arrayType == PigCollection.STRING_FLOAT_MAP) {
                DataBag bag = (DataBag) input.get(0);
                TObjectFloatHashMap map = new TObjectFloatHashMap((int) bag.size());
                for (Tuple t : bag) {
                    map.put((String) t.get(0), (Float) t.get(1));
                }
                return PigCollection.serialize(map);
            } else if (arrayType == PigCollection.INT_SET) {
                DataBag bag = (DataBag) input.get(0);
                TIntHashSet set = new TIntHashSet((int) bag.size());
                for (Tuple t : bag) {
                    set.add((Integer) t.get(0));
                }
                return PigCollection.serialize(set);
            } else if (arrayType == PigCollection.STRING_SET) {
                DataBag bag = (DataBag) input.get(0);
                Set<String> set = new HashSet<String>();
                for (Tuple t : bag) {
                    set.add((String) t.get(0));
                }
                return PigCollection.serialize(set);
            } else {
                throw new RuntimeException("Invalid PigCollection type requested");
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    public Tuple getTupleToEncode(Tuple input, byte inputType) throws ExecException {
        if (inputType == INPUT_TUPLE_FIELD) {
            return (Tuple) input.get(0);
        } else if (inputType == INPUT_SEVERAL_FIELDS) {
            return input;
        } else {
            throw new RuntimeException(
                "Input must be several fields or a single tuple field to be encoded as a dense PigArray")
            ;
        }
    }
}
