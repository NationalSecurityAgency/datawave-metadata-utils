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
    public Value serialize(HashMap<String,Integer> dateToFrequencyValueMap, boolean compress) {
        
        Value serializedMap;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.reset();
        
        if (compress) {
            for (Map.Entry<String,Integer> entry : dateToFrequencyValueMap.entrySet()) {
                Base256Compression keyContent = new Base256Compression(entry.getKey(), true);
                try {
                    baos.write(keyContent.getCompressedYear());
                    baos.write(keyContent.getCompressedMonth());
                    baos.write(keyContent.getCompressedDay());
                } catch (StringIndexOutOfBoundsException exception) {
                    writeUncompressedKey(baos, entry);
                    log.error("We wrote this key uncompressed when compressed was specified " + entry.getKey(), exception);
                }
                if (entry.getValue() != null || entry.getValue().toString().isEmpty())
                    try {
                        baos.write(String.valueOf(entry.getValue()).getBytes(Charset.defaultCharset()));
                    } catch (IOException ioe) {
                        log.error("could not write out the Long value for frequency");
                    }
                
                else
                    baos.write('0');
                baos.write('\u0000'); // null byte terminator
            }
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
    
    /**
     * This is a helper class that will compress the yyyyMMdd and the frequency date concatenated to it without a delimiter
     */
    public static class Base256Compression {
        
        private String content;
        private boolean doCompression = false;
        private Base127Year year;
        private Base127Month month;
        private Base127Day day;
        private Base127Frequency frequency;
        
        public Base256Compression(String content, boolean compress) {
            this.content = content;
            doCompression = compress;
            
            if (doCompression) {
                year = new Base127Year(content.substring(0, 3), doCompression);
                month = new Base127Month(content.substring(4, 5), doCompression);
                
            } else {
                year = new Base127Year((byte) content.charAt(0), doCompression);
                month = new Base127Month((byte) content.charAt(1), doCompression);
            }
            
        }
        
        public byte getCompressedYear() {
            
            byte abyte = year.yearByte;
            return abyte;
        }
        
        public byte getCompressedMonth() {
            byte abyte = month.monthCompressed;
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
        
        public static long bytesToLong(byte[] byteArray) {
            long result = 0l;
            if (byteArray == null)
                return 0l;
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
        
        private class Base127Month {
            public byte monthCompressed;
            public String monthExpanded;
            public boolean doCompression;
            
            public Base127Month(byte value, boolean compress) {
                if (value < 1 || value > 12) {
                    log.error("Month out of Base127 encoded range");
                }
                monthCompressed = value;
                monthExpanded = decode(value);
                doCompression = compress;
                
            }
            
            public Base127Month(String value, boolean compress) {
                if (value.length() != 2) {
                    log.error("Month needs to be two characters to encode " + value);
                }
                monthCompressed = encode(value);
                monthExpanded = value;
            }
            
            public byte encode(String month) {
                if (month.length() != 2) {
                    log.error("Month needs to be two characters to encode " + month);
                }
                int nMonth = Integer.valueOf(month.charAt(1));
                
                if (month.charAt(0) == '1')
                    nMonth += 10;
                
                return (byte) nMonth;
                
            }
            
            public String decode(byte month) {
                if ((int) month < 1 || (int) month > 12) {
                    log.error("Byte code for month is out of range");
                    return "01";
                }
                
                String decodeMonth = String.valueOf((int) month);
                if ((int) month < 10)
                    decodeMonth = "0" + decodeMonth;
                
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
            private byte yearByte;
            private String yearStr = null;
            private boolean compressed = false;
            
            private int BASE_YEAR = 1970;
            
            public Base127Year(byte compressedYear, boolean compress) {
                if (compressedYear == '\u0000')
                    log.error("Date value can't be set to null byte");
                yearByte = compressedYear;
                compressed = compress;
                if (compressed)
                    yearStr = decode(yearByte);
            }
            
            public Base127Year(String uncompressedYear, boolean compress) {
                yearStr = uncompressedYear;
                if (compress)
                    yearByte = encode(yearStr);
                compressed = compress;
            }
            
            public byte encode(String year) {
                
                if (year == null || year.length() != 4) {
                    log.error("Need a four digit year. Encoding " + year + "failed.");
                    return '\u0001';
                    
                }
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
