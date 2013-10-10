import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import junit.framework.Assert;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;

public class TestCosineSimilarity {
    
    @Test
    public void testCosineSimilarity() throws IOException, ParseException {
        // Read the macro file
        String nlpMacros = "nlp.pig";
        List<String> scriptLines = PigMacroTester.getScriptLines(nlpMacros);

        // Append the script to the macro file
        scriptLines.add("vectors = load 'notused' as (id:chararray, features:bag{t:tuple(token:chararray, weight:float)});");
        scriptLines.add("graph = NLP__CosineSimilarity(vectors);");

        String[] script = scriptLines.toArray(new String[scriptLines.size()]);               
        
        String[] vectors = {
            "dib\t{(before,1.6094379124341003),(does,1.6094379124341003),(three,1.6094379124341003),(hours,1.6094379124341003),(lunch,1.6094379124341003)}",
            "gaz\t{(commands,1.6094379124341003),(pig,0.9162907318741551)}",
            "gir\t{(pig,1.8162214318741261),(government,1.6094379124341003),(sent,1.0729586402660585),(let's,1.0729586402660585),(man,1.0729586402660585)}",
            "mrs_bitters\t{(your,1.6094379124341003),(all,1.2070784343255752),(now,1.2070784343255752),(less,1.2070784343255752),(love,1.2070784343255752)}",
            "zim\t{(curse,1.6094379124341003),(doom,1.2070784343255752),(doomed,1.2070784343255752),(snacks,1.2070784343255752),(enemies,1.2070784343255752)}"
        };

        String[] graph = {
            "(gaz,gir,0.29398634421497577)",
            "(gir,gaz,0.29398634421497577)"
        };
        
        PigMacroTester tst = new PigMacroTester(script);
        tst.assertOutput("vectors", vectors, "graph", graph, "(id:chararray, features:bag{t:tuple(token:chararray, weight:float)})");        
    }
}
