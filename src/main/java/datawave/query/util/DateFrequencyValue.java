package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
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
     *
     * @param dateToFrequencyValueMap
     *            the keys should be dates in yyyyMMdd format
     * @return Value the value to store in accumulo
     */
    public Value serialize(TreeMap<YearMonthDay,Frequency> dateToFrequencyValueMap) {
        
        Value serializedMap;
        int year, presentYear = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] dayFrequencies = new byte[NUM_FREQUENCY_BYTES];
        
        for (Map.Entry<YearMonthDay,Frequency> dateFrequencyEntry : dateToFrequencyValueMap.entrySet()) {
            year = dateFrequencyEntry.getKey().year;
            if (year != presentYear) {
                
                if (presentYear != 0) {
                    try {
                        baos.write(dayFrequencies);
                    } catch (IOException ioException) {
                        log.error("Did not write out the frequence properly ", ioException);
                    }
                }
                
                try {
                    baos.write(Base256Compression.numToBytes(year));
                    if (presentYear != 0)
                        Arrays.fill(dayFrequencies, (byte) 0);
                } catch (IOException ioException) {
                    log.error("Could not convert the year or the first ordinal (julian) to bytes ", ioException);
                }
                
                presentYear = year;
            }
            
            putFrequencyBytesInByteArray(dateFrequencyEntry, dayFrequencies);
            
            if (log.isTraceEnabled())
                log.trace(dateFrequencyEntry.getKey().toString());
            
        }
        
        if (dayFrequencies != null) {
            try {
                baos.write(dayFrequencies);
            } catch (IOException ioException) {
                log.error("Did not write out the frequence properly ", ioException);
            }
        }
        
        serializedMap = new Value(baos.toByteArray());
        
        return serializedMap;
    }
    
    private void putFrequencyBytesInByteArray(Map.Entry<YearMonthDay,Frequency> entry, byte[] dayFrequencies) {
        // This will always be 4 bytes now - the frequency variable in the function call below get copied into.
        // the dayFrequencies array.
        byte[] frequency = Base256Compression.numToBytes(entry.getValue().value);
        
        int dateOrdinal = entry.getKey().julian;
        
        int index = 0;
        try {
            for (byte frequencyByte : frequency) {
                dayFrequencies[(dateOrdinal - 1) * 4 + index] = frequencyByte;
                index++;
            }
        } catch (ArrayIndexOutOfBoundsException arrayIndexOutOfBoundsException) {
            log.error("The ordinal " + dateOrdinal + " causes an index out of bounds excepton", arrayIndexOutOfBoundsException);
        }
    }
    
    public TreeMap<YearMonthDay,Frequency> deserialize(Value oldValue) {
        
        TreeMap<YearMonthDay,Frequency> dateFrequencyMap = new TreeMap<>();
        if (oldValue == null || oldValue.toString().isEmpty()) {
            log.error("The datefrequency value was empty");
            dateFrequencyMap.put(new YearMonthDay("Error"), new Frequency(0));
            return dateFrequencyMap;
        }
        
        byte[] expandedData = oldValue.get();
        
        try {
            for (int i = 0; i < expandedData.length; i += (NUM_YEAR_BYTES + NUM_FREQUENCY_BYTES)) {
                byte[] encodedYear = new byte[] {expandedData[i], expandedData[i + 1], expandedData[i + 2], expandedData[i + 3]};
                int decodedYear = Base256Compression.bytesToInteger(encodedYear);
                log.debug("Deserialize decoded the year " + decodedYear);
                /*
                 * Decode the frequencies for each day of the year.
                 */
                for (int j = NUM_YEAR_BYTES; j < DAYS_IN_LEAP_YEAR * NUM_BYTES_PER_FREQ_VALUE + NUM_YEAR_BYTES; j += NUM_BYTES_PER_FREQ_VALUE) {
                    int k = i + j;
                    byte[] encodedfrequencyOnDay = new byte[] {expandedData[k], expandedData[k + 1], expandedData[k + 2], expandedData[k + 3]};
                    int decodedFrequencyOnDay = Base256Compression.bytesToInteger(encodedfrequencyOnDay);
                    if (decodedFrequencyOnDay != 0) {
                        dateFrequencyMap.put(new YearMonthDay(decodedYear + OrdinalDayOfYear.calculateMMDD(j / NUM_BYTES_PER_FREQ_VALUE, decodedYear)),
                                        new Frequency(decodedFrequencyOnDay));
                        log.debug("put key value pair in SimpleDateFrequency map: " + decodedYear
                                        + OrdinalDayOfYear.calculateMMDD(j / NUM_BYTES_PER_FREQ_VALUE, decodedYear) + "-" + decodedFrequencyOnDay);
                    }
                    
                }
                
            }
        } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
            log.error("Error decoding the compressed array of date values.", indexOutOfBoundsException);
            
        }
        
        return dateFrequencyMap;
    }
    
    /**
     * This is a helper class that will compress the yyyyMMdd and the frequency date concatenated to it without a delimiter
     */
    public static class Base256Compression {
        
        public static byte[] numToBytes(long num) {
            return new byte[] {(byte) (num >>> 24), (byte) (num >>> 16), (byte) (num >>> 8), (byte) num};
        }
        
        public static int bytesToInteger(byte[] byteArray) {
            return ((int) byteArray[0] & 0xff) << 24 | ((int) byteArray[1] & 0xff) << 16 | ((int) byteArray[2] & 0xff) << 8 | ((int) byteArray[3] & 0xff);
        }
        
    }
    
}
