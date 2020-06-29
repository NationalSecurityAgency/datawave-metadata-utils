package datawave.util;

import datawave.query.util.DateFrequencyValue;
import datawave.query.util.OrdinalDayOfYear;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DateFrequencyValueTest {
    DateFrequencyValue dateFrequencyValue = new DateFrequencyValue();
    HashMap<String,Integer> dateFrequencyUncompressed = new HashMap<>();
    
    private static final Logger log = LoggerFactory.getLogger(DateFrequencyValueTest.class);
    
    public static String[] YEARS = new String[] {"2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"};
    public static String[] MONTHS = new String[] {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
    
    @Before
    public void initialize() {
        dateFrequencyUncompressed = new HashMap<>();
        Random random = new Random();
        int frequencyValue;
        String dayString;
        // TODO Build a loop that does an on everydate but the 29, 30, 31st for 10 years
        for (String year : YEARS) {
            for (String month : MONTHS) {
                for (int day = 1; day < 29; day++) {
                    frequencyValue = random.nextInt(Integer.MAX_VALUE);
                    
                    if (day < 10)
                        dayString = "0" + day;
                    else
                        dayString = String.valueOf(day);
                    
                    dateFrequencyUncompressed.put(year + month + dayString, frequencyValue < 0 ? -frequencyValue : frequencyValue);
                }
            }
            
            if (OrdinalDayOfYear.isLeapYear(year))
                dateFrequencyUncompressed.put(year + "0229", Integer.MAX_VALUE);
            
            dateFrequencyUncompressed.put(year + "1231", 1231);
            dateFrequencyUncompressed.put(year + "0131", 31);
            
        }
        
    }
    
    @Test
    public void DateFrequencyValueTest() {
        dateFrequencyValue.serialize(dateFrequencyUncompressed, false);
        dateFrequencyValue.dumpCompressedDateFrequencies();
        byte[] compressedMapBytes = dateFrequencyValue.getCompressedDateFrequencyMapBytes();
        Assert.assertTrue(compressedMapBytes != null);
        Value accumlo_value = new Value(compressedMapBytes);
        HashMap<String,Integer> restored = dateFrequencyValue.deserialize(accumlo_value, false);
        
        for (Map.Entry<String,Integer> entry : restored.entrySet()) {
            log.info("key is: " + entry.getKey() + " value is: " + entry.getValue());
        }
        log.info("The restored size is " + restored.size());
        log.info("The size of the unprocessed frequency map is " + dateFrequencyUncompressed.size());
        Assert.assertTrue(dateFrequencyUncompressed.size() == 3380);
        Assert.assertTrue(restored.size() == 3380);
        
        // Verify accurate restoration
        for (Map.Entry<String,Integer> entry : dateFrequencyUncompressed.entrySet()) {
            if (restored.containsKey(entry.getKey())) {
                if (!(restored.get(entry.getKey()).intValue() == entry.getValue().intValue())) {
                    Assert.fail("The key: " + entry.getKey() + " was not restored with the original value ");
                }
                
            } else {
                Assert.fail("The key: " + entry.getKey() + " with value: " + entry.getValue() + " was not restored");
            }
        }
        
        log.info("All entries were inserted, tranformed, compressed and deserialized properly");
        
    }
    
    @Test
    public void GenerateAccumuloShellScript() {
        
        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter("/var/tmp/dw_716_accumulo_script.txt"));
            
            for (Map.Entry<String,Integer> entry : dateFrequencyUncompressed.entrySet()) {
                String command = "insert BAR_FIELD f csv\\x00" + entry.getKey() + "  " + entry.getValue();
                printWriter.println(command);
                
            }
            printWriter.close();
        } catch (IOException ioException) {
            Assert.fail("There was an error creating test script");
            log.info("Here is the problem:", ioException);
        }
    }
    
}
