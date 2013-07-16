package com.mortardata.pig.nlp;

import java.io.IOException;

import gnu.trove.map.hash.TObjectIntHashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.mortardata.pig.collections.PigCollection;

/**
 * Given a string of text, splits on whitespace, counts words, and encodes as a map-type PigCollection.
 */
public class TextToWordFrequencies extends EvalFunc<DataByteArray> {
    private int minWordLength;

    public TextToWordFrequencies(String minWordLength) {
        this.minWordLength = Integer.parseInt(minWordLength);
    }

    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("word_frequencies", DataType.BYTEARRAY));
    }

    public DataByteArray exec(Tuple input) throws IOException {
        String text = (String) input.get(0);
        String[] words = text.split("\\s+");
        TObjectIntHashMap counts = new TObjectIntHashMap();
        for (String s : words) {
            if (s.length() >= minWordLength) {
                counts.adjustOrPutValue(s, 1, 1);
            }
        }
        return PigCollection.serialize(counts);
    }
}
