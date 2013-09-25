package com.mortardata.pig.nlp;

import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
   Given a piece of text, returns a DataBag containing N-grams from the
   text.
 */
public class NGramTokenize extends EvalFunc<DataBag>{

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static String NOFIELD = "";
    private static Pattern SHINGLE_FILLER = Pattern.compile(".* _ .*|_ .*|.* _| _");
    private StandardAnalyzer analyzer;
    private Integer minWordSize;
    private Integer minGramSize;
    private Integer maxGramSize;
    
    public NGramTokenize(String minGramSize, String maxGramSize) {
        this(minGramSize, maxGramSize, "3");
    }
    
    public NGramTokenize(String minGramSize, String maxGramSize, String minWordSize) {
        this.minWordSize = Integer.parseInt(minWordSize);
        this.minGramSize = Integer.parseInt(minGramSize);
        this.maxGramSize = Integer.parseInt(maxGramSize);
        this.analyzer = new StandardAnalyzer(Version.LUCENE_44);
    }
    
    /**
       Uses Lucene's StandardAnalyzer and tuns the tokens through several lucene filters
       - LengthFilter: Filter individual words to be of length > minWordSize
       - ShingleFilter: Converts word stream into n-gram stream
       - PatternReplaceFilter: Removes the 'filler' character that ShingleFilter puts in to
         replace stopwords
     */
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        StringReader textInput = new StringReader(input.get(0).toString());
        TokenStream stream = analyzer.tokenStream(NOFIELD, textInput);
        LengthFilter filtered = new LengthFilter(Version.LUCENE_44, stream, minWordSize, Integer.MAX_VALUE); // Let words be long
                
        if (minGramSize == 1 && maxGramSize == 1) {
            return fillBag(filtered);
        } else {
            Boolean outputUnigrams = false;
            if (minGramSize == 1) {
                minGramSize = 2;
                outputUnigrams = true;
            }
            ShingleFilter nGramStream = new ShingleFilter(filtered, minGramSize, maxGramSize);        
            nGramStream.setOutputUnigrams(outputUnigrams);                
            PatternReplaceFilter replacer = new PatternReplaceFilter(nGramStream, SHINGLE_FILLER, NOFIELD, true);
            return fillBag(replacer);
        } 
    }

    /**
       Fills a DataBag with tokens from a TokenStream
     */
    public DataBag fillBag(TokenStream stream) throws IOException {
        DataBag result = bagFactory.newDefaultBag();
        CharTermAttribute termAttribute = stream.getAttribute(CharTermAttribute.class);
        stream.reset();

        while (stream.incrementToken()) {
            if (termAttribute.length() > 0) {
                Tuple termText = tupleFactory.newTuple(termAttribute.toString());
                result.add(termText);
            }
            termAttribute.setEmpty();
        }
        return result;
    }
}
