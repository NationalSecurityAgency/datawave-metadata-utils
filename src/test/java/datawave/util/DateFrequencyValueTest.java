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
        int monthIndex = 0;
        for (String year : YEARS) {
            for (String month : MONTHS) {
                for (int day = 1; day <= OrdinalDayOfYear.DAYSINMONTH.values()[monthIndex].numdays; day++) {
                    frequencyValue = random.nextInt(Integer.MAX_VALUE);
                    dayString = makeDayString(day);
                    dateFrequencyUncompressed.put(year + month + dayString, frequencyValue < 0 ? -frequencyValue : frequencyValue);
                }
                monthIndex++;
            }
            
            if (!OrdinalDayOfYear.isLeapYear(year))
                dateFrequencyUncompressed.remove(year + "0229");
            
            monthIndex = 0;
            
            /*
             * dateFrequencyUncompressed.put(year + "0129", 129); dateFrequencyUncompressed.put(year + "0130", 130); dateFrequencyUncompressed.put(year +
             * "0131", 131); dateFrequencyUncompressed.put(year + "0329", 329); dateFrequencyUncompressed.put(year + "0330", 330);
             * dateFrequencyUncompressed.put(year + "0331", 331); dateFrequencyUncompressed.put(year + "0429", 429); dateFrequencyUncompressed.put(year +
             * "0430", 430); dateFrequencyUncompressed.put(year + "0529", 529); dateFrequencyUncompressed.put(year + "0530", 530);
             * dateFrequencyUncompressed.put(year + "0531", 531); dateFrequencyUncompressed.put(year + "0629", 629); dateFrequencyUncompressed.put(year +
             * "0630", 630); dateFrequencyUncompressed.put(year + "0729", 729); dateFrequencyUncompressed.put(year + "0730", 730);
             * dateFrequencyUncompressed.put(year + "0731", 731); dateFrequencyUncompressed.put(year + "0829", 829); dateFrequencyUncompressed.put(year +
             * "0830", 830); dateFrequencyUncompressed.put(year + "0831", 831); dateFrequencyUncompressed.put(year + "0929", 929);
             * dateFrequencyUncompressed.put(year + "0930", 930); dateFrequencyUncompressed.put(year + "1029", 1029); dateFrequencyUncompressed.put(year +
             * "1030", 1030); dateFrequencyUncompressed.put(year + "1031", 1031); dateFrequencyUncompressed.put(year + "1129", 1129);
             * dateFrequencyUncompressed.put(year + "1130", 1130); dateFrequencyUncompressed.put(year + "1229", 1229); dateFrequencyUncompressed.put(year +
             * "1230", 1230); dateFrequencyUncompressed.put(year + "1231", 1231);
             */
            
        }
        
    }
    
    private String makeDayString(int day) {
        String dayString;
        if (day < 10)
            dayString = "0" + day;
        else
            dayString = String.valueOf(day);
        return dayString;
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
        Assert.assertTrue(dateFrequencyUncompressed.size() == 3652);
        Assert.assertTrue(restored.size() == 3652);
        
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
