package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * This class handles the serialization and deserialization of the Accumulo value in a record of the Datawave Metadata table that has a column family of "f" and
 * a column qualifier that is prefixed with the string "compressed-" like "compressed-csv" for example. This is a class used to help compress the date and
 * frequency values that are aggregated together to by the FrequencyTransformIterator and manipulated in the FrequencyFamilyCounter The byte array really only
 * has to be like this in regular expression format (YEAR(4BYTE-FREQUENCY){365,366})(\x00))* . Explained verbally a one byte representation of Year followed by
 * 365 or 366 (Leap year) 4 byte holders for frequency then Null terminated. Each Accumulo row for this "aggregated" frequency "map" would be 10 x ( 1 + (365 x
 * 4) + 1) bytes long for a 10 year capture: 14620 bytes.
 */

public class DateFrequencyValue {
    
    private static final Logger log = LoggerFactory.getLogger(DateFrequencyValue.class);
    
    // An intermediate data structure build up by iterating through uncompressDateFrequencies
    private HashMap<YearKey,byte[]> compressedDateFrequencies = new HashMap<>();
    // The mapping of SimpleDateFormat strings to frequency counts. This is passed in by the
    // FrequencyFamilyCounter object to this classes serialize function
    private HashMap<String,Integer> uncompressedDateFrequencies = null;
    private static int BADORDINAL = 367;
    private static int DAYS_IN_LEAP_YEAR = 366;
    private static int MAX_YEARS = 1;
    private static int NUM_BYTES_YR = 4;
    static int TEST_ORDINAL = 0;
    
    private byte[] compressedDateFrequencyMapBytes = null;
    
    public DateFrequencyValue() {}
    
    /**
     *
     * @param dateToFrequencyValueMap
     *            the keys should be dates in yyyyMMdd format
     * @return Value the value to store in accumulo
     */
    public Value serialize(HashMap<String,Integer> dateToFrequencyValueMap, boolean doGzip) {
        
        Value serializedMap;
        uncompressedDateFrequencies = dateToFrequencyValueMap;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream outputStream;
        int uncompressedLength = 0;
        
        for (Map.Entry<String,Integer> entry : uncompressedDateFrequencies.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isEmpty())
                continue;
            
            KeyParser parser = new KeyParser(entry.getKey());
            byte[] yearBytes = parser.getYearBytes();
            YearKey compressedMapKey = new YearKey(parser.getYear(), yearBytes);
            byte[] dayFrequencies;
            // Build the compressedDateFrequencies object
            if (compressedDateFrequencies.containsKey(compressedMapKey)) {
                dayFrequencies = compressedDateFrequencies.get(compressedMapKey);
                // Get the 366 x 4 byte array vector to hold frequencies
                // corresponding to the ordinal dates of the year.
                putFrequencyBytesInByteArray(parser, entry, dayFrequencies);
                compressedDateFrequencies.put(compressedMapKey, dayFrequencies);
            } else {
                dayFrequencies = new byte[DAYS_IN_LEAP_YEAR * 4];
                putFrequencyBytesInByteArray(parser, entry, dayFrequencies);
                compressedDateFrequencies.put(compressedMapKey, dayFrequencies);
            }
            
        }
        
        try {
            outputStream = new GZIPOutputStream(baos);
        } catch (IOException ioException) {
            log.info("The zipped output stream could not be created", ioException);
            return new Value("Error Compressing Value");
            
        }
        
        // Iterate through the compressed object map and push out to the byte stream
        for (Map.Entry<YearKey,byte[]> entry : compressedDateFrequencies.entrySet()) {
            log.info("Compressing key: " + entry.getKey().key + " value" + entry.getValue() + "Byte array " + entry.getValue());
            try {
                outputStream.write(entry.getKey().compressedContent);
                outputStream.write(entry.getValue());
                uncompressedLength += entry.getKey().compressedContent.length;
                uncompressedLength += entry.getValue().length;
            } catch (IOException ioException) {
                log.error("There was an error writing compressed map to output stream", new Exception());
            }
        }
        
        try {
            outputStream.close();
        } catch (IOException ioException) {
            log.info("Could not close Zip output stream - Error compressing DateFrequencyValue", ioException);
            return new Value("Error closing Zip output stream while compressing DateFrequencyValue");
        }
        
        log.info("Serialized length is " + baos.toByteArray().length + " uncompressed length was " + uncompressedLength);
        
        serializedMap = new Value(baos.toByteArray());
        
        this.compressedDateFrequencyMapBytes = Arrays.copyOf(baos.toByteArray(), baos.size());
        
        log.info("DateFrequencyValue.serialize compressed date value to byte size of " + baos.toByteArray().length);
        
        return serializedMap;
    }
    
    private void putFrequencyBytesInByteArray(KeyParser parser, Map.Entry<String,Integer> entry, byte[] dayFrequencies) {
        // This will always be 4 bytes now even if null it will get compressed later on.
        byte[] frequency = Base256Compression.numToBytes(entry.getValue());
        int dateOrdinal = parser.ordinalDayOfYear.getDateOrdinal();
        
        if (dateOrdinal == BADORDINAL) {
            log.error("There is a bad MMDD value in the date " + parser.ordinalDayOfYear.mmDD, new Exception());
            return;
        }
        int index = 0;
        for (byte frequencyByte : frequency) {
            dayFrequencies[dateOrdinal * 4 + index] = frequencyByte;
            index++;
        }
    }
    
    public HashMap<String,Integer> deserialize(Value oldValue) {
        
        // 10 years of of frequency counts
        byte[] readBuffer = new byte[MAX_YEARS * NUM_BYTES_YR + DAYS_IN_LEAP_YEAR * MAX_YEARS];
        
        HashMap<String,Integer> dateFrequencyMap = new HashMap<>();
        if (oldValue == null || oldValue.toString().isEmpty()) {
            log.error("The datefrequency value was empty");
            dateFrequencyMap.put("Error", 0);
            return dateFrequencyMap;
        }
        
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(oldValue.get());
        GZIPInputStream inputStream;
        try {
            inputStream = new GZIPInputStream(arrayInputStream);
        } catch (IOException ioException) {
            log.error("Error creating input stream during deserialization ", ioException);
            dateFrequencyMap.put("Error", 0);
            return dateFrequencyMap;
        }
        
        int read = 0;
        try {
            read = inputStream.read(readBuffer, 0, readBuffer.length);
        } catch (IOException ioException) {
            log.error("Error creating input stream during deserialization ", ioException);
            dateFrequencyMap.put("Error reading compressed input stream", 0);
            return dateFrequencyMap;
        }
        
        try {
            inputStream.close();
        } catch (IOException ioException) {
            log.info("Error creating input stream during deserialization ", ioException);
            dateFrequencyMap.put("Error closing compressed input stream", 0);
        }
        // Should hold the original (reconstructed) data
        byte[] expandedData = Arrays.copyOf(readBuffer, read);
        
        // Decode the bytes into a String
        try {
            String message = new String(expandedData, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Error creating input stream during deserialization ", e);
            dateFrequencyMap.put("Could not decode value to UTF-8", 0);
        }
        
        try {
            for (int i = 0; i < read; i += (4 + DAYS_IN_LEAP_YEAR * 4)) {
                byte[] encodedYear = new byte[] {expandedData[i], expandedData[i + 1], expandedData[i + 2], expandedData[i + 3]};
                int decodedYear = Base256Compression.bytesToInteger(encodedYear);
                log.info("Deserialize decoded the year " + decodedYear);
                for (int j = 4; j < DAYS_IN_LEAP_YEAR; j += 4) {
                    int k = i + j;
                    byte[] encodedfrequencyOnDay = new byte[] {expandedData[k], expandedData[k + 1], expandedData[k + 2], expandedData[k + 3]};
                    int decodedFrequencyOnDay = Base256Compression.bytesToInteger(encodedfrequencyOnDay);
                    if (decodedFrequencyOnDay != 0) {
                        
                        dateFrequencyMap.put(decodedYear + "-" + j + 1, decodedFrequencyOnDay);
                        log.info("put key value pair in SimpleDateFrequency map: " + decodedYear + "-" + decodedFrequencyOnDay);
                    }
                }
                
            }
        } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
            log.error("Error decoding the compressed array of date values.", indexOutOfBoundsException);
        }
        
        return dateFrequencyMap;
    }
    
    private class KeyParser {
        
        private String keyValue;
        private OrdinalDayOfYear ordinalDayOfYear;
        
        public KeyParser(String key) {
            if (key == null) {
                log.info("The key can't be null", new Exception());
                keyValue = "1313";
                ordinalDayOfYear = new OrdinalDayOfYear(keyValue);
            }
            keyValue = key;
            if (keyValue != null) {
                if (keyValue.length() >= 4)
                    ordinalDayOfYear = new OrdinalDayOfYear(keyValue.substring(3));
            } else {
                ordinalDayOfYear = new OrdinalDayOfYear("1313");
                log.error("Bad ordinal mmDD value");
            }
        }
        
        public byte[] getYearBytes() {
            int year = '\u0000';
            try {
                String yearStr = keyValue.substring(4);
                year = Integer.parseUnsignedInt(yearStr);
                
            } catch (NumberFormatException numberFormatException) {
                log.error("the year could not be extract from ", keyValue, numberFormatException);
            }
            return Base256Compression.numToBytes(year);
        }
        
        public String getYear() {
            return keyValue.substring(4);
        }
        
    }
    
    public class OrdinalDayOfYear {
        private String mmDD;
        
        public OrdinalDayOfYear(String monthDay) {
            mmDD = monthDay;
        }
        
        public int getDateOrdinal() {
            if (mmDD.equals("1313")) // dummy date when mmDD was bad
                return BADORDINAL;
            else
                return calculateOrdinal();
        }
        
        private int calculateOrdinal() {
            // TODO implement
            return TEST_ORDINAL++;
        }
        
    }
    
    /**
     * This is a helper class that will compress the yyyyMMdd and the frequency date concatenated to it without a delimiter
     */
    public static class Base256Compression {
        
        public static byte[] numToBytes(long num) {
            return new byte[] {(byte) (num >>> 24), (byte) (num >>> 16), (byte) (num >>> 8), (byte) num};
        }
        
        public static int bytesToInteger(byte[] byteArray) {
            int result = 0;
            if (byteArray == null)
                return 0;
            if (byteArray.length > 4)
                return 2_147_483_647;
            
            if (byteArray.length == 1) {
                if (byteArray[0] > 0 && byteArray[0] < 128)
                    return byteArray[0];
                else {
                    return byteArray[0] & 0xFF;
                }
                
            }
            
            int numBytes = byteArray.length;
            int bitShifts = numBytes * 8 - 8;
            long addToResult;
            
            for (byte aByte : byteArray) {
                
                if (aByte < 0)
                    aByte = (byte) (256 + aByte);
                
                addToResult = ((long) aByte & 0xFF) << bitShifts;
                
                if (addToResult < 0)
                    addToResult = -addToResult;
                
                result += (addToResult);
                bitShifts -= 8;
                
            }
            
            return result;
        }
    }
    
    private class YearKey {
        private String key;
        private byte[] compressedContent;
        
        public YearKey(String key, byte[] compressedContent) {
            this.key = key;
            this.compressedContent = compressedContent;
        }
        
        public String getKey() {
            return key;
        }
        
        public void setKey(String key) {
            this.key = key;
        }
        
        public byte[] getCompressedContent() {
            return compressedContent;
        }
        
        public void setCompressedContent(byte[] compressedContent) {
            this.compressedContent = compressedContent;
        }
        
        @Override
        public int hashCode() {
            return key.hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            return key.equals(((YearKey) obj).key);
        }
    }
    
    public void dumpCompressedDateFrequencies() {
        for (Map.Entry<YearKey,byte[]> entry : compressedDateFrequencies.entrySet()) {
            System.out.println(entry.getKey().key + ":" + entry.getKey().compressedContent + "Byte array " + entry.getValue());
        }
    }
    
    public byte[] getCompressedDateFrequencyMapBytes() {
        return compressedDateFrequencyMapBytes;
    }
    
}
