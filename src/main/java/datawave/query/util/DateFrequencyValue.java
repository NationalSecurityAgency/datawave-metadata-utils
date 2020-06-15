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
 * a column qualifier that is prefixed with the string "compressed-" like "compressed-csv" for example. This is a class used to help compress the date and
 * frequency values that are aggregated together to by the FrequencyTransformIterator and manipulated in the FrequencyFamilyCounter The main idea is to compress
 * date/frequency pairs of the form yyyyMMdd,Frequency like 201906190x20. The year can be represented by a single base 127 digit that will only need one byte of
 * storage - there will be a base year (1970) that the the byte value can be added to re-create the year. The month can be encoded in one byte and so can the
 * day of the month. So we only to store 3 bytes in the Accumulo value to store the yyyyMMdd format. 3/8 the original size is better than the compression ratio
 * for run length encoding. By tranforming the frequency count corresponding to the date to a Base127 representation whe can get all counts below 128 to
 * compress to a single byte instead of 3 bytes. All counts from 128 to 1270 can be collapsed into only 2 base127 chars.
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
                Base127Compression keyContent = new Base127Compression(entry.getKey(), true);
                try {
                    baos.write(keyContent.getCompressedYear());
                    baos.write(keyContent.getCompressedMonth());
                    baos.write(keyContent.getCompressedDay());
                } catch (StringIndexOutOfBoundsException exception) {
                    writeUncompressedKey(baos, entry);
                    log.error("We wrote this key uncompressed when compressed was specified " + entry.getKey(), exception);
                }
                if (entry.getValue() != null || entry.getValue().toString().isEmpty())
                    baos.write(Long.valueOf(entry.getValue()).byteValue());
                else
                    baos.write('0');
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
            Long deserializedFrequency;
            
            try {
                log.info("deserialize is attempting to cast this value to Long " + kvp.substring(3));
                deserializedFrequency = Long.valueOf(kvp.substring(3));
                
            } catch (NumberFormatException numberFormatException) {
                deserializedFrequency = 0L;
                log.info("The frequency for kvp " + kvp + " could not be dertermined by Long.valueof");
                log.error("The value " + kvp.substring(3) + " could not be deserialized properly", numberFormatException);
            }
            
            dateFrequencyMap.put(kvp.substring(0, 2), deserializedFrequency);
            
        }
        
        return dateFrequencyMap;
    }
    
    /**
     * This is a helper class that will compress the yyyyMMdd and the frequency date concatenated to it without a delimiter
     */
    private static class Base127Compression {
        
        private String content;
        private boolean doCompression = false;
        private Base127Year year;
        private Base127Month month;
        private Base127Day day;
        private Base127Frequency frequency;
        
        public Base127Compression(String content, boolean compress) {
            this.content = content;
            doCompression = compress;
            if (doCompression)
                year = new Base127Year(content.substring(0, 3), doCompression);
            else
                year = new Base127Year((byte) content.charAt(0), doCompression);
            
        }
        
        public byte getCompressedYear() {
            
            byte abyte = year.year;
            return abyte;
        }
        
        public byte getCompressedMonth() {
            byte abyte = (byte) content.getBytes(Charset.defaultCharset())[5];
            return abyte;
        }
        
        public byte getCompressedDay() {
            byte abyte = (byte) content.getBytes(Charset.defaultCharset())[7];
            return abyte;
        }
        
        public byte[] getCompressedFrequency() {
            // byte[] tranformedValue = {(byte) (0)};
            
            String longValueString = String.valueOf(content).substring(7, content.length());
            
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
        
        private class Base127Month {
            public byte month;
            
            public Base127Month(byte value) {
                if (value < 1 || value > 12) {
                    log.error("Month out of Base127 encoded range");
                }
                month = value;
            }
            
            public byte encode(String month) {
                byte encMonth = '\u0001';
                return encMonth;
            }
            
            public String decode(byte month) {
                String decodeMonth = "01";
                return decodeMonth;
            }
            
        }
        
        private class Base127Day {
            public byte day;
            
            public Base127Day(byte value) {
                if (value < 0 || value > 31)
                    log.error("Day is out of range");
            }
            
            public byte encode(String day) {
                byte encDay = '\u0001';
                return encDay;
            }
            
            public String decode(byte day) {
                String decodeDay = "01";
                return decodeDay;
            }
            
        }
        
        private class Base127Year {
            private byte year;
            private String yearStr = null;
            private boolean compressed = false;
            
            private int BASE_YEAR = 1970;
            
            public Base127Year(byte compressedYear, boolean compress) {
                if (compressedYear == '\u0000')
                    log.error("Date value can't be set to null byte");
                year = compressedYear;
                compressed = compress;
                if (compressed)
                    yearStr = decode(year);
            }
            
            public Base127Year(String uncompressedYear, boolean compress) {
                yearStr = uncompressedYear;
                if (compress)
                    this.year = encode(yearStr);
                compressed = compress;
            }
            
            public byte encode(String year) {
                if (year == null)
                    return '\u0001';
                int intYear = Integer.parseInt(year) - BASE_YEAR;
                if (intYear < 1 || intYear > 127)
                    log.error("Year is out of range and can't be encoded in a single byte");
                
                return (byte) intYear;
            }
            
            public String decode(byte compressedYear) {
                int year = BASE_YEAR + (int) compressedYear;
                return String.valueOf(year);
            }
            
        }
        
        private class Base127Frequency {
            
        }
    }
    
}
