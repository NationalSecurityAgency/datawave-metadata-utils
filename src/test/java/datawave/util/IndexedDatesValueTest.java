package datawave.util;

import datawave.query.util.IndexedDatesValue;
import datawave.query.util.YearMonthDay;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class IndexedDatesValueTest {
    // The date used in the constructor will be overwritten
    IndexedDatesValue indexedDatesValue = new IndexedDatesValue(null);
    TreeSet<YearMonthDay> indexedDatesUncompressed = new TreeSet<>();
    
    private static final Logger log = LoggerFactory.getLogger(IndexedDatesValueTest.class);
    
    public static String[] YEARS = new String[] {"2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"};
    public static String[] MONTHS = new String[] {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
    public static final int[] MONTH_LENGTH = new int[] {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    public static final int[] LEAP_MONTH_LENGTH = new int[] {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    
    private void initialize(String monthToEliminate) {
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
            
            for (String month : MONTHS) {
                for (int day = 1; day <= monthLengths[monthIndex]; day++) {
                    dayString = makeDayString(day);
                    if (monthToEliminate != null) {
                        if (!month.equals(monthToEliminate))
                            indexedDatesUncompressed.add(new YearMonthDay(year + month + dayString));
                    } else {
                        indexedDatesUncompressed.add(new YearMonthDay(year + month + dayString));
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
    /*
     * @Test public void DateFrequencyValueTestNoDecemberFrequencies() { initialize(MONTHS[11]); Value accumuloValue = initAndSerializeIndexDatesValue(); byte[]
     * compressedMapBytes = accumuloValue.get(); Assert.assertTrue(compressedMapBytes != null); BitSet restored =
     * IndexedDatesValue.deserialize(accumuloValue).getIndexedDatesBitSet();
     * 
     * testRestoredEntries(restored); log.info("The restored size is " + restored.size()); log.info("The size of the unprocessed frequency map is " +
     * indexedDatesUncompressed.size()); Assert.assertTrue(indexedDatesUncompressed.size() == 3342); Assert.assertTrue(restored.size() == 3342);
     * verifySerializationAndDeserialization(restored);
     * 
     * log.info("All entries were inserted, tranformed, compressed and deserialized properly");
     * 
     * }
     * 
     * private void testRestoredEntries(YearMonthDay startDay,BitSet restored) { for (YearMonthDay entry : restored) { if
     * (!indexedDatesUncompressed.contains(entry)) { log.info("Restored entry is not in the original set " + entry); } } }
     * 
     * private void verifySerializationAndDeserialization(TreeSet<YearMonthDay> restored) { // Verify accurate restoration boolean passTest = true;
     * 
     * for (YearMonthDay entry : indexedDatesUncompressed) { if (!restored.contains(entry)) { passTest = false; log.info("The date: " + entry +
     * " was not restored"); } }
     * 
     * Assert.assertTrue(passTest); }
     * 
     * @Test public void DateFrequencyValueTestNoJanuaryFrequencies() { initialize(MONTHS[0]); Value accumuloValue = initAndSerializeIndexDatesValue(); byte[]
     * compressedMapBytes = accumuloValue.get(); Assert.assertTrue(compressedMapBytes != null); BitSet restored =
     * IndexedDatesValue.deserialize(accumuloValue).getIndexedDatesBitSet();
     * 
     * testRestoredEntries(restored);
     * 
     * log.info("The restored size is " + restored.size()); log.info("The size of the unprocessed frequency map is " + indexedDatesUncompressed.size());
     * Assert.assertTrue(indexedDatesUncompressed.size() == 3342); Assert.assertTrue(restored.size() == 3342);
     * 
     * // Verify accurate restoration verifySerializationAndDeserialization(restored);
     * 
     * log.info("All entries were inserted, tranformed, compressed and deserialized properly");
     * 
     * }
     * 
     * private Value initAndSerializeIndexDatesValue() {
     * 
     * indexedDatesValue.setStartDay(indexedDatesUncompressed.first()); indexedDatesValue.setIndexedDatesSet(indexedDatesUncompressed); return
     * indexedDatesValue.serialize(); }
     * 
     * @Test public void DateFrequencyValueTestNoFebruaryFrequencies() { initialize(MONTHS[1]); Value accumuloValue = initAndSerializeIndexDatesValue(); byte[]
     * compressedMapBytes = accumuloValue.get(); Assert.assertTrue(compressedMapBytes != null); TreeSet<YearMonthDay> restored =
     * IndexedDatesValue.deserialize(accumuloValue).getIndexedDatesSet();
     * 
     * testRestoredEntries(restored);
     * 
     * log.info("The restored size is " + restored.size()); log.info("The size of the unprocessed frequency map is " + indexedDatesUncompressed.size());
     * Assert.assertTrue(indexedDatesUncompressed.size() == 3370); Assert.assertTrue(restored.size() == 3370);
     * 
     * // Verify accurate restoration verifySerializationAndDeserialization(restored);
     * 
     * log.info("All entries were inserted, tranformed, compressed and deserialized properly");
     * 
     * }
     * 
     * void doSomething (){ TreeSet<YearMonthDay> indexedDatesSet = new TreeSet<>(); // TODO Remove this try catch block and get the TreeSet<YearMonthDay> out
     * of class // The indexedDatesSet is used for testing the class so the test needs to change. try { YearMonthDay firstDay = indexedDatesSet.first();
     * 
     * if (!firstDay.equals(startDay)) { log.warn("First day in treeset should be the start day");
     * log.warn("Start day will now be initialized to firstDate in the sorted treeset"); startDay = firstDay; }
     * 
     * YearMonthDay lastDay = indexedDatesSet.last(); // TODO Do a better job estimating the size using the ordinals // Estimate the span of dates with firstDay
     * and lastDay int bitSetSize; if (firstDay.equals(lastDay)) bitSetSize = 1; else if (lastDay.getYear() == firstDay.getYear()) bitSetSize =
     * lastDay.getJulian() - firstDay.getJulian() + 1; else // Estimate the span of dates with firstDay and lastDay bitSetSize = (lastDay.getYear() -
     * firstDay.getYear() + 1) * 366; indexedDatesBitSet = new BitSet(bitSetSize); int dayIndex = 0; YearMonthDay nextDay = startDay;
     * 
     * for (YearMonthDay ymd : indexedDatesSet) { if (ymd.compareTo(nextDay) == 0) { indexedDatesBitSet.set(dayIndex); dayIndex++; nextDay =
     * YearMonthDay.nextDay(nextDay.getYyyymmdd()); } else { do { dayIndex++; nextDay = YearMonthDay.nextDay(nextDay.getYyyymmdd()); if (ymd.compareTo(nextDay)
     * == 0) { indexedDatesBitSet.set(dayIndex); }
     * 
     * } while (nextDay.compareTo(ymd) < 0); }
     * 
     * } } catch (NoSuchElementException noSuchElementException) { log.warn("The empty contructor was called and bitset and start day never populated."); if
     * (indexedDatesBitSet == null) return new Value(); } }
     */
    
}
