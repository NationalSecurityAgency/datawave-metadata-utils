package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.mortbay.util.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.attribute.HashPrintJobAttributeSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * This class handles the serialization and deserialization of the Accumulo value in a record of the Datawave Metadata table that has a column family of "f" and
 * a column qualifier that is prefixed the "compressed-" like "compressed-csv" for example. This is a class used to help compress the date and frequency values
 * that are aggregated together to by the FrequencyTransformIterator and manipulated in the FrequencyFamilyCounter The main idea is to compress date/frequency
 * pairs of the form yyyyMMdd,Frequency like 201906190x20. The year can be represented by a single base 127 digit that will only need one byte of storage -
 * there will be a base year (1970) that the the byte value can be added to re- create the year. The month can be encoded in one byte and so can the day of the
 * month. So we only to store 3 bytes in the Accumulo value to store the yyyyMMdd format. 3/8 the original size is better than the compression ratio for run
 * length encoding. By tranforming the frequency count corresponding to the date to a Base127 representation whe can get all counts below 128 to compress to a
 * single byte instead of 3 bytes. All counts from 128 to 1270 can be collapsed into only 2 base127 chars.
 */

public class DateFrequencyValue {
    
    private static final Logger log = LoggerFactory.getLogger(DateFrequencyValue.class);
    
    public DateFrequencyValue() {}
    
    public DateFrequencyValue(String content) {
        
    }
    
    /**
     *
     * @param dateToFrequencyValueMap
     *            the keys should be dates in yyyyMMdd format
     * @return Value the value to store in accumulo
     */
    public Value serialize(HashMap<String,Long> dateToFrequencyValueMap, boolean compress) {
        
        Value serializedMap;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.reset();
        
        if (compress) {
            for (Map.Entry<String,Long> entry : dateToFrequencyValueMap.entrySet()) {
                Base127Compress keyContent = new Base127Compress(entry.getKey(), true);
                try {
                    baos.write(keyContent.getCompressedYear());
                    baos.write(keyContent.getCompressedMonth());
                    baos.write(keyContent.getCompressedDay());
                } catch (StringIndexOutOfBoundsException exception) {
                    writeUncompressedKey(baos, entry);
                    log.error("We wrote this key uncompressed when compressed was specified " + entry.getKey(), exception);
                }
                baos.write(Long.valueOf(entry.getValue()).byteValue());
                baos.write('\u0000'); // null byte terminator
            }
        } else {
            for (Map.Entry<String,Long> entry : dateToFrequencyValueMap.entrySet()) {
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
    
    private void writeUncompressedKey(ByteArrayOutputStream baos, Map.Entry<String,Long> entry) {
        try {
            baos.write(entry.getKey().getBytes(Charset.defaultCharset()));
        } catch (IOException ioe) {
            log.error("The key could not be serialized: " + entry.getKey());
        }
    }
    
    public HashMap<String,Long> deserialize(Value oldValue) {
        
        HashMap<String,Long> dateFrequencyMap = new HashMap<>();
        if (oldValue == null || oldValue.toString().isEmpty())
            return dateFrequencyMap;
        
        String[] kvps = oldValue.toString().split("\u0000");
        
        for (String kvp : kvps) {
            try {
                dateFrequencyMap.put(kvp.substring(0, 2), Long.valueOf(kvp.substring(3)));
            } catch (Exception e) {
                log.error("The value " + oldValue.toString() + " could not be deserialized properly", e);
            }
        }
        
        return dateFrequencyMap;
    }
    
    /**
     * This is a helper class that will compress the yyyyMMdd and the frequency date concatenated to it without a delimiter
     */
    private static class Base127Compress {
        
        private static int BASE_YEAR = 1970;
        private String content;
        private boolean compressedContent = false;
        
        public Base127Compress(String content, boolean compress) {
            this.content = content;
            this.compressedContent = compress;
        }
        
        public byte getCompressedYear() {
            
            byte abyte = (byte) content.charAt(3);
            return abyte;
        }
        
        public byte getCompressedMonth() {
            byte abyte = (byte) content.charAt(5);
            return abyte;
        }
        
        public byte getCompressedDay() {
            byte abyte = (byte) content.charAt(7);
            return abyte;
        }
        
        public byte[] getCompressedValue() {
            // byte[] tranformedValue = {(byte) (0)};
            
            String longValueString = content.substring(7);
            
            /*
             * try { if (longValueString != null) tranformedValue = numToBytes(Long.valueOf(longValueString)); } catch (Exception e) {
             * log.info("DateFrequencyValue.getValue called numToBytes and it didn't work", e); }
             */
            
            return longValueString.getBytes(Charset.defaultCharset());
            
        }
        
        public byte[] numToBytes(long num) {
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
        
    }
    
}
