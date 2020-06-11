package datawave.query.util;

/**
 * This is a class used to help compress the date and frequency values that are aggregated
 * together to by the FrequencyTransformIterator and manipulated in the FrequencyFamilyCounter
 * The main idea is to compress date/frequency pairs of the form yyyyMMdd,Frequency like
 * 201906190x20.  The year can be represented by a single base 127 digit that will only need one
 * byte of storage - there will be a base year (1970) that the the byte value can be added to re-
 * create the year.  The month can be encoded in one byte and so can the day of the month. So we only
 * to store 3 bytes in the Accumulo value to store the yyyyMMdd format.  3/8 the original size is better
 * than the compression ratio for run length encoding.  By tranforming the frequency count corresponding
 * to the date to a Base127 representation whe can get all counts below 128 to compress to a single byte
 * instead of 3 bytes.  All counts from 128 to 1270 can be collapsed into only 2 base127 chars.
 */

public class DateFrequencyValue {
    
    Base127 value = null;
    
    public DateFrequencyValue() {
        
    }
    
    public DateFrequencyValue(byte[] content) {
        
    }


    
    private class Base127 {
        private byte[] content;
        
        public Base127(byte[] content) {
            this.content = content;
        }
        
    }
    
}
