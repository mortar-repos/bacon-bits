import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import junit.framework.Assert;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;

public class TestTFIDF {
    
    @Test
    public void testTFIDF() throws IOException, ParseException {
        // Read the macro file
        String nlpMacros = "nlp.pig";
        List<String> scriptLines = PigMacroTester.getScriptLines(nlpMacros);

        // Append the script to the macro file
        scriptLines.add("documents = load 'notused' as (id:chararray, text:chararray);");
        scriptLines.add("vectors = NLP__TFIDF(documents, 3, 1, 1, 5);");

        String[] script = scriptLines.toArray(new String[scriptLines.size()]);
        
        String[] documents = {
            "gir\tLet's go to my room, pig! I am government man, come from the government. The government has sent me.",
            "zim\tCome, GIR. Let us rain some doom down upon the heads of our doomed enemies. Curse you snacks! Curse yooooooou!",
            "dib\tMy head's not big! Why does everyone say that? Who takes three hours to go to the bathroom *before* lunch, Zim?",
            "mrs_bitters\tChildren, your performance was miserable. Your parents will all receive phone calls instructing them to love you less now.",
            "gaz\tThe pig... COMMANDS ME!"
        };
        
        String[] vectors = {
            "(dib,{(before,1.6094379124341003),(does,1.6094379124341003),(three,1.6094379124341003),(hours,1.6094379124341003),(lunch,1.6094379124341003)})",
            "(gaz,{(commands,1.6094379124341003),(pig,0.9162907318741551)})",
            "(gir,{(government,1.6094379124341003),(sent,1.0729586402660585),(let's,1.0729586402660585),(man,1.0729586402660585),(from,1.0729586402660585)})",
            "(mrs_bitters,{(your,1.6094379124341003),(all,1.2070784343255752),(now,1.2070784343255752),(less,1.2070784343255752),(love,1.2070784343255752)})",
            "(zim,{(curse,1.6094379124341003),(doom,1.2070784343255752),(doomed,1.2070784343255752),(snacks,1.2070784343255752),(enemies,1.2070784343255752)})"
        };
        
        PigMacroTester tst = new PigMacroTester(script);
        tst.assertOutput("documents", documents, "vectors", vectors);        
    }
}
