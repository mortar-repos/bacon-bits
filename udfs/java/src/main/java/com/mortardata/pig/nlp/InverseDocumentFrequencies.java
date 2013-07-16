package com.mortardata.pig.nlp;

import java.io.IOException;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.mortardata.pig.collections.PigCollection;

/**
 * Given a bag of tuples with a single bytearray field,
 * which is the result of the com.mortardata.pig.nlp.TextToWordFrequencies UDF,
 * calculate inverse document frequencies for the corpus
 * and returns them as a string-float map-type PigCollection.
 */
public class InverseDocumentFrequencies extends EvalFunc<DataByteArray>
                                        implements Algebraic, Accumulator<DataByteArray> {

    private static final Log log = LogFactory.getLog(InverseDocumentFrequencies.class);
    private static TupleFactory tf = TupleFactory.getInstance();
    private static BagFactory bf = BagFactory.getInstance();

    private long numDocuments;
    private TObjectFloatHashMap idfs;

    public InverseDocumentFrequencies() {
        cleanup();
    }

    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("idfs", DataType.BYTEARRAY));
    }

    public DataByteArray exec(Tuple input) throws IOException {
        accumulate(input);
        DataByteArray output = getValue();
        cleanup();
        return output;
    }

    public void accumulate(Tuple input) throws IOException {
        DataBag bag = (DataBag) input.get(0);
        for (Tuple t : bag) {
            numDocuments++;
            TObjectIntHashMap tfs = PigCollection.deserializeStringIntMap((DataByteArray) input.get(0));
            TObjectIntIterator it = tfs.iterator();
            for (int i = 0; i < tfs.size(); i++) {
                it.advance();
                // count # documents the term appears in
                // we will adjust this map in-place at the end to turn these counts into idfs
                idfs.adjustOrPutValue(it.key(), 1.0f, 1.0f);
            }
        }
    }

    public DataByteArray getValue() {
        TObjectFloatIterator it = idfs.iterator();
        for (int i = 0; i < idfs.size(); i++) {
            it.advance();
            it.setValue((float) Math.log1p(numDocuments / it.value()));
        }
        return PigCollection.serialize(idfs);
    }

    public void cleanup() {
        numDocuments = 0;
        idfs = new TObjectFloatHashMap();
    }

    public String getInitial()  { return Initial.class.getName();      }
    public String getIntermed() { return Intermediate.class.getName(); }
    public String getFinal()    { return Final.class.getName();        }

    static public class Initial extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            long numDocuments = 0;
            TObjectIntHashMap numDocumentsWithTerm = new TObjectIntHashMap();

            DataBag bag = (DataBag) input.get(0);
            for (Tuple t : bag) {
                numDocuments++;
                TObjectIntHashMap tfs = PigCollection.deserializeStringIntMap((DataByteArray) t.get(0));
                TObjectIntIterator it = tfs.iterator();
                for (int i = 0; i < tfs.size(); i++) {
                    it.advance();
                    numDocumentsWithTerm.adjustOrPutValue(it.key(), 1, 1);
                }
            }

            Tuple t = tf.newTuple(2);
            t.set(0, new Long(numDocuments));
            t.set(1, PigCollection.serialize(numDocumentsWithTerm));
            return t;
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            long numDocuments = 0;
            TObjectIntHashMap numDocumentsWithTerm = new TObjectIntHashMap();

            DataBag bag = (DataBag) input.get(0);
            for (Tuple t : bag) {
                numDocuments += (Long) t.get(0);
                TObjectIntHashMap numDocsPartial = PigCollection.deserializeStringIntMap((DataByteArray) t.get(1));
                TObjectIntIterator it = numDocsPartial.iterator();
                for (int i = 0; i < numDocsPartial.size(); i++) {
                    it.advance();
                    numDocumentsWithTerm.adjustOrPutValue(it.key(), it.value(), it.value());
                }
            }

            Tuple t = tf.newTuple(2);
            t.set(0, new Long(numDocuments));
            t.set(1, PigCollection.serialize(numDocumentsWithTerm));
            return t;
        }
    }

    static public class Final extends EvalFunc<DataByteArray> {
        public DataByteArray exec(Tuple input) throws IOException {
            long numDocuments = 0;
            TObjectFloatHashMap idfs = new TObjectFloatHashMap();

            DataBag bag = (DataBag) input.get(0);
            for (Tuple t : bag) {
                numDocuments += (Long) t.get(0);
                TObjectIntHashMap numDocsPartial = PigCollection.deserializeStringIntMap((DataByteArray) t.get(1));
                TObjectIntIterator it = numDocsPartial.iterator();
                for (int i = 0; i < numDocsPartial.size(); i++) {
                    it.advance();
                    idfs.adjustOrPutValue(it.key(), it.value(), it.value());
                }
            }

            TObjectFloatIterator it = idfs.iterator();
            for (int i = 0; i < idfs.size(); i++) {
                it.advance();
                it.setValue((float) Math.log1p(numDocuments / it.value()));
            }
            return PigCollection.serialize(idfs);
        }
    }
}
