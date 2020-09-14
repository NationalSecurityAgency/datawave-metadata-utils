package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class handles the serialization and deserialization of the Accumulo value in a record of the Datawave Metadata table that has a column family of "f" and
 * a column qualifier that is prefixed with the string "compressed-" like "compressed-csv" for example. This is a class used to help compress the date and
 * frequency values that are aggregated together to by the FrequencyTransformIterator and manipulated in the FrequencyFamilyCounter. The byte array is in
 * regular expression format ((YEAR)(4BYTE-FREQUENCY){366}))* . Explained verbally a four byte representation of Year followed by by up to 366 (Leap year) 4
 * byte holders for frequency values. The month and day of the frequency value is coded by the position in the array. There aren't any delimiters between years
 * and frequencies which adds to the compression. Each Accumulo row for this "aggregated" frequency "map" would be 10 x ( 4 + 4 + (366 * 4) ) bytes long for a
 * maximum length for a 10 year capture of 14720 bytes.
 */

public class DateFrequencyValue {
    
    private static final Logger log = LoggerFactory.getLogger(DateFrequencyValue.class);
    
    public static final int DAYS_IN_LEAP_YEAR = 366;
    private static int NUM_YEAR_BYTES = 4;
    private static int NUM_FREQUENCY_BYTES = DAYS_IN_LEAP_YEAR * 4;
    private static int NUM_BYTES_PER_FREQ_VALUE = 4;
    
    public DateFrequencyValue() {}
    
    /**
     * @param dateToFrequencyValueMap
     *            the keys should be dates in yyyyMMdd format
     * @return size the size of the ByteArrayOutputStream
     */
    private int calculateOutputArraySize(TreeMap<YearMonthDay,Frequency> dateToFrequencyValueMap) {
        int firstYear = dateToFrequencyValueMap.firstKey().year;
        int lastYear = dateToFrequencyValueMap.lastKey().year;
        return (lastYear - firstYear + 1) * (NUM_YEAR_BYTES + NUM_FREQUENCY_BYTES);
    }
    
    /**
     *
     * Serializes a treemap of dates with associated frequencies to an Accumulo value
     *
     * @param dateToFrequencyValueMap
     *            the keys should be dates in yyyyMMdd format
     * @return Value the value to store in accumulo
     */
    public Value serialize(TreeMap<YearMonthDay,Frequency> dateToFrequencyValueMap) {
        
        Value serializedMap;
        int year, presentYear = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(calculateOutputArraySize(dateToFrequencyValueMap));
        int ordinal, nextOrdinal = 1;
        OrdinalDayOfYear ordinalDayOfYear = null;
        
        for (Map.Entry<YearMonthDay,Frequency> dateFrequencyEntry : dateToFrequencyValueMap.entrySet()) {
            year = dateFrequencyEntry.getKey().year;
            ordinal = dateFrequencyEntry.getKey().julian;
            
            if (year != presentYear) {
                
                if (nextOrdinal > 1 && (nextOrdinal < DAYS_IN_LEAP_YEAR)) {
                    // Add zero frequencies for the remaining days of the year that were not in the map
                    // for the last year processed
                    do {
                        Base256Compression.writeToOutputStream(0, baos);
                        nextOrdinal++;
                    } while (nextOrdinal < (DAYS_IN_LEAP_YEAR + 1));
                }
                
                ordinalDayOfYear = new OrdinalDayOfYear(ordinal, year);
                nextOrdinal = 1;
                
                Base256Compression.writeToOutputStream(year, baos);
                presentYear = year;
            }
            
            if (ordinal == nextOrdinal) {
                nextOrdinal++;
                Base256Compression.writeToOutputStream(dateFrequencyEntry.getValue().value, baos);
            } else {
                do {
                    Base256Compression.writeToOutputStream(0, baos);
                    nextOrdinal++;
                } while (nextOrdinal < ordinal);
                
                Base256Compression.writeToOutputStream(dateFrequencyEntry.getValue().value, baos);
                nextOrdinal++;
                
            }
            
            if (nextOrdinal == 366 && ordinalDayOfYear != null && !ordinalDayOfYear.isLeapYear())
                Base256Compression.writeToOutputStream(0, baos);
            
            if (log.isTraceEnabled())
                log.trace(dateFrequencyEntry.getKey().toString());
            
        }
        
        do {
            Base256Compression.writeToOutputStream(0, baos);
            nextOrdinal++;
        } while (nextOrdinal < (DAYS_IN_LEAP_YEAR + 1));
        
        serializedMap = new Value(baos.toByteArray());
        
        return serializedMap;
    }

    /**
     * Deserializes the Accumulo Value object which contains a byte array into a TreeMap of
     * dates to the associated ingest frequencies.
     *
     * @param oldValue
     * @return
     */
    public TreeMap<YearMonthDay,Frequency> deserialize(Value oldValue) {
        
        TreeMap<YearMonthDay,Frequency> dateFrequencyMap = new TreeMap<>();
        if (oldValue == null || oldValue.toString().isEmpty()) {
            log.error("The datefrequency value was empty", new Exception());
            dateFrequencyMap.put(new YearMonthDay("Error"), new Frequency(0));
            return dateFrequencyMap;
        }
        
        byte[] expandedData = oldValue.get();
        
        if (expandedData.length < (NUM_YEAR_BYTES + NUM_FREQUENCY_BYTES)) {
            log.error("The value array is too short ", new Exception());
            dateFrequencyMap.put(new YearMonthDay("Error"), new Frequency(0));
            return dateFrequencyMap;
        }
        
        try {
            for (int i = 0; i < expandedData.length; i += (NUM_YEAR_BYTES + NUM_FREQUENCY_BYTES)) {
                int decodedYear = Base256Compression.bytesToInteger(expandedData[i], expandedData[i + 1], expandedData[i + 2], expandedData[i + 3]);
                log.debug("Deserialize decoded the year " + decodedYear);
                // TODO Extra 4 bytes are being written out in serialize - need to figure this out and remove 2 lines below.
                if (i == expandedData.length - NUM_BYTES_PER_FREQ_VALUE)
                    break;
                /*
                 * Decode the frequencies for each day of the year.
                 */
                for (int j = NUM_YEAR_BYTES; j < DAYS_IN_LEAP_YEAR * NUM_BYTES_PER_FREQ_VALUE + NUM_YEAR_BYTES; j += NUM_BYTES_PER_FREQ_VALUE) {
                    int k = i + j;
                    int decodedFrequencyOnDay = Base256Compression.bytesToInteger(expandedData[k], expandedData[k + 1], expandedData[k + 2],
                                    expandedData[k + 3]);
                    if (decodedFrequencyOnDay != 0) {
                        dateFrequencyMap.put(new YearMonthDay(decodedYear + OrdinalDayOfYear.calculateMMDD(j / NUM_BYTES_PER_FREQ_VALUE, decodedYear)),
                                        new Frequency(decodedFrequencyOnDay));
                        log.debug("put key value pair in SimpleDateFrequency map: " + decodedYear
                                        + OrdinalDayOfYear.calculateMMDD(j / NUM_BYTES_PER_FREQ_VALUE, decodedYear) + "-" + decodedFrequencyOnDay);
                    }
                    
                }
                
            }
        } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
            log.error("Error decoding the compressed array of date values. Expanded array length: " + expandedData.length, indexOutOfBoundsException);
        }
        
        return dateFrequencyMap;
    }
    
    /**
     * This is a helper class that will compress the yyyyMMdd and the frequency date concatenated to it without a delimiter
     */
    public static class Base256Compression {
        
        public static void writeToOutputStream(long num, ByteArrayOutputStream baos) {
            baos.write((byte) (num >>> 24));
            baos.write((byte) (num >>> 16));
            baos.write((byte) (num >>> 8));
            baos.write((byte) (num));
        }
        
        public static int bytesToInteger(byte high, byte nextHigh, byte lowbyte, byte lowest) {
            return ((int) high & 0xff) << 24 | ((int) nextHigh & 0xff) << 16 | ((int) lowbyte & 0xff) << 8 | ((int) lowest & 0xff);
        }
        
    }
    
}
