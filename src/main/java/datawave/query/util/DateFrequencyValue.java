package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
    
    public static final int DAYS_IN_LEAP_YEAR = 366;
    private static int MAX_YEARS = 10;
    private static int NUM_YEAR_BYTES = 4;
    private static int NUM_FREQUENCY_BYTES = DAYS_IN_LEAP_YEAR * 4;
    private static int NUM_BYTES_PER_FREQ_VALUE = 4;
    
    private byte[] compressedDateFrequencyMapBytes = null;
    
    public DateFrequencyValue() {}
    
    /**
     *
     * @param dateToFrequencyValueMap
     *            the keys should be dates in yyyyMMdd format
     * @return Value the value to store in accumulo
     */
    public Value serialize(HashMap<String,Integer> dateToFrequencyValueMap, boolean compressWithGzip) {
        
        Value serializedMap;
        uncompressedDateFrequencies = dateToFrequencyValueMap;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream theOutputstreamInUse;
        int uncompressedLength = 0;
        
        for (Map.Entry<String,Integer> entry : uncompressedDateFrequencies.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isEmpty())
                continue;
            
            KeyParser parser = new KeyParser(entry.getKey());
            YearKey compressedMapKey = new YearKey(parser.getYear(), parser.getYearBytes());
            byte[] dayFrequencies;
            // Build the compressedDateFrequencies object
            if (compressedDateFrequencies.containsKey(compressedMapKey)) {
                dayFrequencies = compressedDateFrequencies.get(compressedMapKey);
                // Get the 366 x 4 byte array vector to hold frequencies
                // corresponding to the ordinal dates of the year.
                putFrequencyBytesInByteArray(parser, entry, dayFrequencies);
                compressedDateFrequencies.put(compressedMapKey, dayFrequencies);
            } else {
                log.info("Allocating and array of byte frequencies for year " + compressedMapKey.getKey());
                dayFrequencies = new byte[NUM_FREQUENCY_BYTES];
                putFrequencyBytesInByteArray(parser, entry, dayFrequencies);
                compressedDateFrequencies.put(compressedMapKey, dayFrequencies);
            }
            
        }
        
        if (compressWithGzip) {
            try {
                theOutputstreamInUse = new GZIPOutputStream(baos);
            } catch (IOException ioException) {
                log.info("The zipped output stream could not be created", ioException);
                return new Value("Error Compressing Value");
                
            }
        } else {
            theOutputstreamInUse = baos;
        }
        
        // Iterate through the compressed object map and push out to the byte stream
        for (Map.Entry<YearKey,byte[]> entry : compressedDateFrequencies.entrySet()) {
            log.info("Compressing key: " + entry.getKey().key + " value" + entry.getValue() + "Byte array " + entry.getValue());
            try {
                theOutputstreamInUse.write(entry.getKey().compressedContent);
                theOutputstreamInUse.write(entry.getValue());
                uncompressedLength += entry.getKey().compressedContent.length;
                uncompressedLength += entry.getValue().length;
            } catch (IOException ioException) {
                log.error("There was an error writing compressed map to output stream", new Exception());
            }
        }
        
        try {
            theOutputstreamInUse.close();
        } catch (IOException ioException) {
            log.info("Could not close Zip output stream - Error compressing DateFrequencyValue", ioException);
            return new Value("Error closing Zip output stream while compressing DateFrequencyValue");
        }
        
        if (compressWithGzip)
            log.info("Serialized length is " + baos.toByteArray().length + " uncompressed length was " + uncompressedLength);
        
        serializedMap = new Value(baos.toByteArray());
        
        this.compressedDateFrequencyMapBytes = Arrays.copyOf(baos.toByteArray(), baos.size());
        
        log.info("DateFrequencyValue.serialize compressed date value to byte size of " + baos.toByteArray().length);
        
        return serializedMap;
    }
    
    private void putFrequencyBytesInByteArray(KeyParser parser, Map.Entry<String,Integer> entry, byte[] dayFrequencies) {
        // This will always be 4 bytes now even if null it will get compressed later on.
        byte[] frequency = Base256Compression.numToBytes(entry.getValue());
        OrdinalDayOfYear ordinalDayOfYear = new OrdinalDayOfYear(parser.keyValue.substring(4), parser.getYear());
        int dateOrdinal = ordinalDayOfYear.getOrdinalDay();
        
        if (dateOrdinal > DAYS_IN_LEAP_YEAR) {
            log.error("There is a bad MMDD value in the date " + ordinalDayOfYear.getMmDD(), new Exception());
            return;
        }
        int index = 0;
        try {
            for (byte frequencyByte : frequency) {
                dayFrequencies[dateOrdinal * 4 + index] = frequencyByte;
                index++;
            }
        } catch (ArrayIndexOutOfBoundsException arrayIndexOutOfBoundsException) {
            log.error("The ordinal " + dateOrdinal + " causes an index out of bounds excepton", arrayIndexOutOfBoundsException);
        }
    }
    
    public HashMap<String,Integer> deserialize(Value oldValue, boolean usesGzip) {
        
        // 10 years of of frequency counts
        byte[] readBuffer = new byte[MAX_YEARS * (NUM_YEAR_BYTES + NUM_FREQUENCY_BYTES)];
        InputStream theInputStreamUsed;
        
        HashMap<String,Integer> dateFrequencyMap = new HashMap<>();
        if (oldValue == null || oldValue.toString().isEmpty()) {
            log.error("The datefrequency value was empty");
            dateFrequencyMap.put("Error", 0);
            return dateFrequencyMap;
        }
        
        if (usesGzip) {
            try {
                ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(oldValue.get());
                theInputStreamUsed = new GZIPInputStream(arrayInputStream);
            } catch (IOException ioException) {
                log.error("Error creating input stream during deserialization ", ioException);
                dateFrequencyMap.put("Error", 0);
                return dateFrequencyMap;
            }
        } else {
            theInputStreamUsed = new ByteArrayInputStream(oldValue.get());
        }
        
        int read = 0;
        try {
            read = theInputStreamUsed.read(readBuffer, 0, readBuffer.length);
        } catch (IOException ioException) {
            log.error("Error creating input stream during deserialization ", ioException);
            dateFrequencyMap.put("Error reading compressed input stream", 0);
            return dateFrequencyMap;
        }
        
        try {
            theInputStreamUsed.close();
        } catch (IOException ioException) {
            log.info("Error creating input stream during deserialization ", ioException);
            dateFrequencyMap.put("Error closing compressed input stream", 0);
        }
        // Should hold the original (reconstructed) data
        byte[] expandedData = Arrays.copyOf(readBuffer, read);
        
        try {
            for (int i = 0; i < read; i += (NUM_YEAR_BYTES + NUM_FREQUENCY_BYTES)) {
                byte[] encodedYear = new byte[] {expandedData[i], expandedData[i + 1], expandedData[i + 2], expandedData[i + 3]};
                int decodedYear = Base256Compression.bytesToInteger(encodedYear);
                log.info("Deserialize decoded the year " + decodedYear);
                for (int j = NUM_YEAR_BYTES; j < DAYS_IN_LEAP_YEAR * NUM_BYTES_PER_FREQ_VALUE + NUM_YEAR_BYTES; j += NUM_BYTES_PER_FREQ_VALUE) {
                    int k = i + j;
                    byte[] encodedfrequencyOnDay = new byte[] {expandedData[k], expandedData[k + 1], expandedData[k + 2], expandedData[k + 3]};
                    int decodedFrequencyOnDay = Base256Compression.bytesToInteger(encodedfrequencyOnDay);
                    if (decodedFrequencyOnDay != 0) {
                        OrdinalDayOfYear ordinalDayOfYear = new OrdinalDayOfYear(j / NUM_BYTES_PER_FREQ_VALUE - 1, decodedYear);
                        dateFrequencyMap.put(decodedYear + ordinalDayOfYear.getMmDD(), decodedFrequencyOnDay);
                        log.info("put key value pair in SimpleDateFrequency map: " + decodedYear + ordinalDayOfYear.getMmDD() + "-" + decodedFrequencyOnDay);
                    }
                }
                
            }
        } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
            log.error("Error decoding the compressed array of date values.", indexOutOfBoundsException);
            
        }
        
        return dateFrequencyMap;
    }
    
    private class KeyParser {
        // Assuming YYYYMMDD SimpleDateFormat
        private String keyValue;
        
        public KeyParser(String key) {
            if (key == null) {
                log.info("The key can't be null", new Exception());
                keyValue = "1313";
            }
            keyValue = key;
            
        }
        
        public byte[] getYearBytes() {
            int year = '\u0000';
            try {
                String yearStr = keyValue.substring(0, 4);
                year = Integer.parseUnsignedInt(yearStr);
                
            } catch (NumberFormatException numberFormatException) {
                log.error("the year could not be extract from ", keyValue, numberFormatException);
            }
            return Base256Compression.numToBytes(year);
        }
        
        public String getYear() {
            return keyValue.substring(0, 4);
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
            return ((int) byteArray[0] & 0xff) << 24 | ((int) byteArray[1] & 0xff) << 16 | ((int) byteArray[2] & 0xff) << 8 | ((int) byteArray[3] & 0xff);
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
