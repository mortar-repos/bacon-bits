package com.mortardata.pig.nlp;

import junit.framework.Assert;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

public class TestNGramTokenize {

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testNGramTokenizeUnigrams() throws IOException {
        String toTokenize = "The quick brown fox jumps over the lazy dog.";
        Set<String> unigrams = new HashSet<String>() {
            {
                add("quick");
                add("brown");
                add("fox");
                add("jumps");
                add("over");
                add("lazy");
                add("dog");
            }
        };

        NGramTokenize unigramTokenizer = new NGramTokenize("1","1");
        DataBag unigramResult = unigramTokenizer.exec(tupleFactory.newTuple(toTokenize));
        Assert.assertEquals(unigramResult.size(), 7l);
        for (Tuple t : unigramResult) {
            Assert.assertTrue(unigrams.contains(t.get(0).toString()));
        }
    }
    
    @Test
    public void testNGramTokenize() throws IOException {
        String toTokenize = "The quick brown fox jumps over the lazy dog.";
        Set<String> ngrams = new HashSet<String>() {
            {
                add("quick brown");
                add("brown fox");
                add("fox jumps");
                add("jumps over");
                add("lazy dog");
                add("brown fox jumps");
                add("fox jumps over");
                add("quick brown fox");
            }
        };        
        
        NGramTokenize tokenizer = new NGramTokenize("2","3");
        DataBag result = tokenizer.exec(tupleFactory.newTuple(toTokenize));
        Assert.assertEquals(result.size(), 8l);
        for (Tuple t : result) {
            Assert.assertTrue(ngrams.contains(t.get(0).toString()));
        }        
    }

}
