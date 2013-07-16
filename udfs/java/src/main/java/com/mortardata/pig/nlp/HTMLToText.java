package com.mortardata.pig.nlp;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import gnu.trove.map.hash.TObjectIntHashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Given a string of html, extracts from paragraph tags.
 * Cleanses internal tags
 * Lowercases everything, removes non-alphabetic characters
 */
public class HTMLToText extends EvalFunc<String> {
    private Pattern cleanseWithSpace;
    private Pattern cleanseWithNothing;
    private Pattern paragraphPattern;
    private Pattern tagPattern;

    public HTMLToText() {
        cleanseWithSpace   = Pattern.compile("\r|\n|&.*?;");
        cleanseWithNothing = Pattern.compile("['’]s|[^a-z\\s]");
        paragraphPattern   = Pattern.compile("<p.*?>(.*?)</p>");
        tagPattern         = Pattern.compile("<.*?>");
    }

    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("text", DataType.CHARARRAY));
    }

    public String exec(Tuple input) throws IOException {
        String text = cleanseWithSpace.matcher(((String) input.get(0))).replaceAll(" ").toLowerCase();
        Matcher paragraphMatcher = paragraphPattern.matcher(text);
        
        StringBuilder sb = new StringBuilder();
        while (paragraphMatcher.find()) {
            String pText = paragraphMatcher.group();
            pText = tagPattern.matcher(pText).replaceAll("");
            pText = cleanseWithNothing.matcher(pText).replaceAll("");
            sb.append(pText.trim());
            sb.append("\n");
        }

        return sb.toString();
    }
}
