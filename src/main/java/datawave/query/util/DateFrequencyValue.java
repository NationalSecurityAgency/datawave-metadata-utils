package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

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
    
    private HashMap<CompressedMapKey,byte[]> compressedDateFrequencies = new HashMap<>();
    private HashMap<String,Integer> uncompressedDateFrequencies = null;
    private static int BADORDINAL = 367;
    
    public DateFrequencyValue() {}
    
    /**
     *
     * @param dateToFrequencyValueMap
     *            the keys should be dates in yyyyMMdd format
     * @return Value the value to store in accumulo
     */
    public Value serialize(HashMap<String,Integer> dateToFrequencyValueMap, boolean compress) {
        
        Value serializedMap;
        uncompressedDateFrequencies = dateToFrequencyValueMap;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.reset();
        
        if (compress) {
            for (Map.Entry<String,Integer> entry : uncompressedDateFrequencies.entrySet()) {
                if (entry.getKey() == null || entry.getKey().isEmpty())
                    continue;
                
                KeyValueParser parser = new KeyValueParser(entry.getKey());
                byte[] yearBytes = parser.getYear();
                CompressedMapKey compressedMapKey = new CompressedMapKey(entry.getKey(), yearBytes);
                byte[] dayFrequencies;
                // Build the compressedDateFrequencies object
                if (compressedDateFrequencies.containsKey(compressedMapKey)) {
                    dayFrequencies = compressedDateFrequencies.get(compressedMapKey);
                    // Get the 366 x 4 byte array vector to hold frequencies
                    // corresponding to the ordinal dates of the year.
                    putFrequencyBytesInByteArray(parser, entry, dayFrequencies);
                    compressedDateFrequencies.put(compressedMapKey, dayFrequencies);
                } else {
                    dayFrequencies = new byte[366 * 4];
                    putFrequencyBytesInByteArray(parser, entry, dayFrequencies);
                    compressedDateFrequencies.put(compressedMapKey, dayFrequencies);
                }
                
            }
            /*
             * for (Map.Entry<String,Integer> entry : uncompressedDateFrequencies.entrySet()) { Base256Compression keyContent = new
             * Base256Compression(entry.getKey()); try { baos.write(keyContent.getCompressedYear()); baos.write(keyContent.getCompressedMonth());
             * baos.write(keyContent.getCompressedDay()); } catch (StringIndexOutOfBoundsException exception) { writeUncompressedKey(baos, entry);
             * log.error("We wrote this key uncompressed when compressed was specified " + entry.getKey(), exception); } if (entry.getValue() != null ||
             * entry.getValue().toString().isEmpty()) try { baos.write(String.valueOf(entry.getValue()).getBytes(Charset.defaultCharset())); } catch
             * (IOException ioe) { log.error("could not write out the Long value for frequency"); }
             * 
             * else baos.write('0'); baos.write('\u0000'); // null byte terminator }
             */
        } else {
            for (Map.Entry<String,Integer> entry : dateToFrequencyValueMap.entrySet()) {
                writeUncompressedKey(baos, entry);
                baos.write(Long.valueOf(entry.getValue()).byteValue());
                baos.write('\u0000');
                
            }
        }
        
        serializedMap = new Value(baos.toByteArray());
        
        log.info("DateFrequencyValue.serialize compressed date value to byte size of " + baos.toByteArray().length);
        
        baos.reset();
        
        return serializedMap;
    }
    
    private void putFrequencyBytesInByteArray(KeyValueParser parser, Map.Entry<String,Integer> entry, byte[] dayFrequencies) {
        byte[] frequency = Base256Compression.numToBytes(entry.getValue());
        
        int lenthOfFrequency = 0;
        if (frequency != null) {
            lenthOfFrequency = frequency.length;
            
        }
        // null pad the 4 byte frequency slot if frequency size < 4
        if (lenthOfFrequency < 4) {
            int endPaddingIndex = 4 - lenthOfFrequency;
            for (int i = 0; i < endPaddingIndex; i++) {
                dayFrequencies[parser.ordinalDayOfYear.getDateOrdinal() + i] = '\u0000';
            }
        }
        
        int index = 0;
        for (byte frequencyByte : frequency) {
            dayFrequencies[parser.ordinalDayOfYear.getDateOrdinal() + index] = frequencyByte;
            index++;
        }
    }
    
    private void writeUncompressedKey(ByteArrayOutputStream baos, Map.Entry<String,Integer> entry) {
        try {
            baos.write(entry.getKey().getBytes(Charset.defaultCharset()));
        } catch (IOException ioe) {
            log.error("The key could not be serialized: " + entry.getKey());
        }
    }
    
    public HashMap<String,Integer> deserialize(Value oldValue) {
        
        HashMap<String,Integer> dateFrequencyMap = new HashMap<>();
        if (oldValue == null || oldValue.toString().isEmpty())
            return dateFrequencyMap;
        
        String[] kvps = oldValue.toString().split("\u0000");
        
        for (String kvp : kvps) {
            Integer deserializedFrequency;
            
            try {
                log.info("deserialize is attempting to cast this value to Long " + kvp.substring(3));
                deserializedFrequency = Integer.valueOf(kvp.substring(3));
                
            } catch (NumberFormatException numberFormatException) {
                deserializedFrequency = 0;
                log.info("The frequency for kvp " + kvp + " could not be dertermined by Long.valueof");
                log.error("The value " + kvp.substring(3) + " could not be deserialized properly", numberFormatException);
            }
            
            dateFrequencyMap.put(kvp.substring(0, 2), deserializedFrequency);
            
        }
        
        return dateFrequencyMap;
    }
    
    private class KeyValueParser {
        
        private String keyValue;
        private OrdinalDayOfYear ordinalDayOfYear;
        
        public KeyValueParser(String key) {
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
            }
        }
        
        public byte[] getYear() {
            int year = '\u0000';
            try {
                year = Integer.parseUnsignedInt(keyValue.substring(0, 4));
                
            } catch (NumberFormatException numberFormatException) {
                log.error("the year could not be extract from ", keyValue, numberFormatException);
            }
            return Base256Compression.numToBytes(year);
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
            return 100;
        }
        
    }
    
    /**
     * This is a helper class that will compress the yyyyMMdd and the frequency date concatenated to it without a delimiter
     */
    public static class Base256Compression {
        
        private String content;
        
        public Base256Compression(String content) {
            this.content = content;
        }
        
        public static byte[] numToBytes(long num) {
            if (num == 0) {
                return new byte[] {};
            } else if (num < 256) {
                return new byte[] {(byte) (num)};
            } else if (num < 65536) {
                return new byte[] {(byte) (num >>> 8), (byte) num};
            } else if (num < 16777216) {
                return new byte[] {(byte) (num >>> 16), (byte) (num >>> 8), (byte) num};
            } else { // up to 2,147,483,647
                return new byte[] {(byte) (num >>> 24), (byte) (num >>> 16), (byte) (num >>> 8), (byte) num};
            }
        }
        
        public static int bytesToLong(byte[] byteArray) {
            int result = 0;
            if (byteArray == null)
                return 0;
            if (byteArray.length > 4)
                return 2_147_483_647;
            
            if (byteArray.length == 1) {
                // TODO need to program a tranform from Binary twos complement or this can
                // be interpreted as a negative number if highest bit is one.
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
    
    private class CompressedMapKey {
        private String key;
        
        public CompressedMapKey(String key, byte[] compressedContent) {
            this.key = key;
            this.compressedContent = compressedContent;
        }
        
        private byte[] compressedContent;
        
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
    }
    
}
