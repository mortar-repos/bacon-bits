import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;
import java.lang.reflect.Field;

import org.apache.pig.ExecType;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.impl.io.FileLocalizer;
    
/**
   Mostly necessary as a means of getting around https://issues.apache.org/jira/browse/PIG-3114
 */
public class PigMacroTester extends PigTest {

    public PigMacroTester(String[] script) {
        super(script);
    }

    // Don't kill me.
    public PigServer getPig() {
        try {
            Field staticPigServerField = null;
            for (Field field : PigTest.class.getDeclaredFields()) {
                if (PigServer.class.isAssignableFrom(field.getType())) {
                    staticPigServerField = field;
                    break;
                }
            }
            
            if (staticPigServerField!=null) {
                staticPigServerField.setAccessible(true);
                return (PigServer)staticPigServerField.get(null);
            } else {
                System.out.println("No matching static PigServer field found to patch.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    // Don't kill me.
    public void resetPig() {
        try {
            Field staticPigServerField = null;
            for (Field field : PigTest.class.getDeclaredFields()) {
                if (PigServer.class.isAssignableFrom(field.getType())) {
                    staticPigServerField = field;
                    break;
                }
            }
            
            if (staticPigServerField!=null) {
                staticPigServerField.setAccessible(true);
                staticPigServerField.set(null, new PigServer(ExecType.LOCAL));
            } else {
                System.out.println("No matching static PigServer field found to patch.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void registerScript() throws IOException, ParseException {
        resetPig();
        super.registerScript();
    }

    /**
       Helper method for reading the lines of a pig script into an arraylist
     */
    public static List<String> getScriptLines(String scriptFile) throws IOException, FileNotFoundException {
        FileReader fileReader = new FileReader(scriptFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        List<String> lines = new ArrayList<String>();
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            lines.add(line);
        }
        bufferedReader.close();
        return lines;
    }

    /**
       Bag schemas aren't serialized properly, workaround
     */
    public void assertOutput(String aliasInput, String[] input, String alias, String[] expected, String inputSchema)
        throws IOException, ParseException {
        registerScript();

        final String destination = FileLocalizer.getTemporaryPath(getPig().getPigContext()).toString();
        getCluster().copyFromLocalFile(input, destination, true);
        override(aliasInput,
                 String.format("%s = LOAD '%s' AS %s;", aliasInput, destination, inputSchema));

        assertOutput(alias, expected);
  }
}
