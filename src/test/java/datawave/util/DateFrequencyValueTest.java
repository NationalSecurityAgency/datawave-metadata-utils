package datawave.util;

import datawave.query.util.DateFrequencyValue;
import datawave.query.util.Frequency;
import datawave.query.util.OrdinalDayOfYear;
import datawave.query.util.YearMonthDay;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class DateFrequencyValueTest {
    DateFrequencyValue dateFrequencyValue = new DateFrequencyValue();
    TreeMap<YearMonthDay,Frequency> dateFrequencyUncompressed = new TreeMap<>();
    
    private static final Logger log = LoggerFactory.getLogger(DateFrequencyValueTest.class);
    
    public static String[] YEARS = new String[] {"2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"};
    public static String[] MONTHS = new String[] {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
    public static final int[] MONTH_LENGTH = new int[] {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    public static final int[] LEAP_MONTH_LENGTH = new int[] {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    
    private void initialize(String monthToEliminate) {
        Random random = new Random();
        int frequencyValue;
        String dayString;
        int monthIndex = 0;
        int nYear;
        boolean isLeapYear;
        for (String year : YEARS) {
            GregorianCalendar gc = new GregorianCalendar(Integer.parseInt(year), 1, 1);
            nYear = Integer.parseInt(year);
            isLeapYear = gc.isLeapYear(nYear);
            int[] monthLengths;
            
            if (isLeapYear)
                monthLengths = LEAP_MONTH_LENGTH;
            else
                monthLengths = MONTH_LENGTH;
            long thelong = Long.MAX_VALUE;
            for (String month : MONTHS) {
                for (int day = 1; day <= monthLengths[monthIndex]; day++) {
                    frequencyValue = random.nextInt(Integer.MAX_VALUE);
                    // frequencyValue = 255;
                    dayString = makeDayString(day);
                    if (monthToEliminate != null) {
                        if (!month.equals(monthToEliminate))
                            dateFrequencyUncompressed.put(new YearMonthDay(year + month + dayString),
                                            new Frequency(frequencyValue < 0 ? -frequencyValue : frequencyValue));
                    } else {
                        dateFrequencyUncompressed.put(new YearMonthDay(year + month + dayString),
                                        new Frequency(frequencyValue < 0 ? -frequencyValue : frequencyValue));
                    }
                }
                monthIndex++;
            }
            
            monthIndex = 0;
            
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
    public void DateFrequencyValueTestNoDecemberFrequencies() {
        initialize(MONTHS[11]);
        Value accumuloValue = dateFrequencyValue.serialize(dateFrequencyUncompressed);
        byte[] compressedMapBytes = accumuloValue.get();
        Assert.assertTrue(compressedMapBytes != null);
        TreeMap<YearMonthDay,Frequency> restored = dateFrequencyValue.deserialize(accumuloValue);
        
        for (Map.Entry<YearMonthDay,Frequency> entry : restored.entrySet()) {
            log.info("key is: " + entry.getKey() + " value is: " + entry.getValue());
        }
        log.info("The restored size is " + restored.size());
        log.info("The size of the unprocessed frequency map is " + dateFrequencyUncompressed.size());
        Assert.assertTrue(dateFrequencyUncompressed.size() == 3342);
        Assert.assertTrue(restored.size() == 3342);
        verifySerializationAndDeserialization(restored);
        
        log.info("All entries were inserted, tranformed, compressed and deserialized properly");
        
    }
    
    private void verifySerializationAndDeserialization(TreeMap<YearMonthDay,Frequency> restored) {
        // Verify accurate restoration
        Frequency restoredFreq, originalFreq;
        
        for (Map.Entry<YearMonthDay,Frequency> entry : dateFrequencyUncompressed.entrySet()) {
            if (restored.containsKey(entry.getKey())) {
                restoredFreq = restored.get(entry.getKey());
                originalFreq = entry.getValue();
                if (!(restoredFreq.equals(originalFreq))) {
                    Assert.fail("The key: " + entry.getKey() + " was not restored with the original value ");
                }
                
            } else {
                Assert.fail("The key: " + entry.getKey() + " with value: " + entry.getValue() + " was not restored");
            }
        }
    }
    
    @Test
    public void DateFrequencyValueTestNoJanuaryFrequencies() {
        initialize(MONTHS[0]);
        Value accumuloValue = dateFrequencyValue.serialize(dateFrequencyUncompressed);
        byte[] compressedMapBytes = accumuloValue.get();
        Assert.assertTrue(compressedMapBytes != null);
        TreeMap<YearMonthDay,Frequency> restored = dateFrequencyValue.deserialize(accumuloValue);
        
        for (Map.Entry<YearMonthDay,Frequency> entry : restored.entrySet()) {
            log.info("key is: " + entry.getKey() + " value is: " + entry.getValue());
        }
        log.info("The restored size is " + restored.size());
        log.info("The size of the unprocessed frequency map is " + dateFrequencyUncompressed.size());
        Assert.assertTrue(dateFrequencyUncompressed.size() == 3342);
        Assert.assertTrue(restored.size() == 3342);
        
        // Verify accurate restoration
        verifySerializationAndDeserialization(restored);
        
        log.info("All entries were inserted, tranformed, compressed and deserialized properly");
        
    }
    
    @Test
    public void DateFrequencyValueTestNoFebruaryFrequencies() {
        initialize(MONTHS[1]);
        Value accumuloValue = dateFrequencyValue.serialize(dateFrequencyUncompressed);
        byte[] compressedMapBytes = accumuloValue.get();
        Assert.assertTrue(compressedMapBytes != null);
        TreeMap<YearMonthDay,Frequency> restored = dateFrequencyValue.deserialize(accumuloValue);
        
        for (Map.Entry<YearMonthDay,Frequency> entry : restored.entrySet()) {
            log.info("key is: " + entry.getKey() + " value is: " + entry.getValue());
        }
        
        log.info("The restored size is " + restored.size());
        log.info("The size of the unprocessed frequency map is " + dateFrequencyUncompressed.size());
        Assert.assertTrue(dateFrequencyUncompressed.size() == 3370);
        Assert.assertTrue(restored.size() == 3370);
        
        // Verify accurate restoration
        verifySerializationAndDeserialization(restored);
        
        log.info("All entries were inserted, tranformed, compressed and deserialized properly");
        
    }
    
    @Test
    public void GenerateAccumuloShellScript() {
        initialize(null);
        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter("/var/tmp/dw_716_accumulo_script.txt"));
            
            for (Map.Entry<YearMonthDay,Frequency> entry : dateFrequencyUncompressed.entrySet()) {
                String command = "insert BAR_FIELD f csv\\x00" + entry.getKey().getYyyymmdd() + "  " + entry.getValue().getValue();
                printWriter.println(command);
                
            }
            printWriter.close();
        } catch (IOException ioException) {
            Assert.fail("There was an error creating test script");
            log.info("Here is the problem:", ioException);
        }
    }
    
}
