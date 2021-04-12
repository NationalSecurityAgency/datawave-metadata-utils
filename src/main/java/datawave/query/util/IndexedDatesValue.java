package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IndexedDatesValue implements Comparable<IndexedDatesValue> {
    
    private static final Logger log = LoggerFactory.getLogger(IndexedDatesValue.class);
    
    private YearMonthDay startDay;
    
    private BitSet indexedDatesBitSet = null;
    public static final int YYMMDDSIZE = 8;
    public static final int COMPARE_FAILED = Integer.MAX_VALUE;
    
    public IndexedDatesValue(YearMonthDay startDate) {
        startDay = startDate;
        indexedDatesBitSet = new BitSet(1);
        indexedDatesBitSet.set(0);
        
    }
    
    public IndexedDatesValue() {
        startDay = null;
    }
    
    public YearMonthDay getStartDay() {
        return startDay;
    }
    
    public void setStartDay(YearMonthDay startDay) {
        this.startDay = startDay;
    }
    
    /**
     * Serialize a Bitset representing indexed dates and initial date the field was indexed.
     *
     * build byte array with the start day at the beginning and the bytes of the bit set remaining
     *
     * @return
     */
    public Value serialize() {
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream(YYMMDDSIZE + indexedDatesBitSet.size());
        try {
            baos.write(startDay.getYyyymmdd().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.error("Could not write the initial day field was indexed");
        }
        if (indexedDatesBitSet != null && !indexedDatesBitSet.isEmpty()) {
            try {
                baos.write(indexedDatesBitSet.toByteArray());
            } catch (IOException e) {
                log.error("Could not write index set to the output stream.");
            }
        }
        
        return new Value(baos.toByteArray());
    }
    
    public static IndexedDatesValue deserialize(byte[] accumuloValue) {
        
        if (accumuloValue == null)
            return null;
        
        String yyyyMMdd = new String(Arrays.copyOfRange(accumuloValue, 0, YYMMDDSIZE), StandardCharsets.UTF_8);
        YearMonthDay startDay;
        try {
            startDay = new YearMonthDay(yyyyMMdd);
        } catch (Exception e) {
            log.warn("Accumulo value was not originally an IndexedDatesValue - Start date was not deserialized");
            return new IndexedDatesValue();
        }
        IndexedDatesValue indexedDates = new IndexedDatesValue(startDay);
        YearMonthDay nextday = startDay;
        if (accumuloValue.length > 8) {
            BitSet theIndexedDates = BitSet.valueOf(Arrays.copyOfRange(accumuloValue, YYMMDDSIZE, accumuloValue.length));
            indexedDates.setIndexedDatesBitSet(theIndexedDates);
            log.info("The number of dates is " + theIndexedDates.length());
        }
        
        return indexedDates;
    }
    
    public static IndexedDatesValue deserialize(Value accumuloValue) {
        String yyyyMMdd = new String(Arrays.copyOfRange(accumuloValue.get(), 0, YYMMDDSIZE), StandardCharsets.UTF_8);
        YearMonthDay startDay;
        try {
            startDay = new YearMonthDay(yyyyMMdd);
        } catch (Exception e) {
            log.warn("Accumulo value was not originally an IndexedDatesValue - Start date was not deserialized");
            return new IndexedDatesValue();
        }
        IndexedDatesValue indexedDates = new IndexedDatesValue(startDay);
        YearMonthDay nextday = startDay;
        if (accumuloValue.get().length > 8) {
            BitSet theIndexedDates = BitSet.valueOf(Arrays.copyOfRange(accumuloValue.get(), YYMMDDSIZE, accumuloValue.get().length));
            indexedDates.setIndexedDatesBitSet(theIndexedDates);
            log.info("The number of dates is " + theIndexedDates.length());
        }
        
        return indexedDates;
    }
    
    /**
     * Gets the bit for index which represents the number of days from the initial indexed date for a field.
     *
     */
    public BitSet getIndexedDatesBitSet() {
        return indexedDatesBitSet;
    }
    
    /**
     * Sets the bit for index which represents the number of days from the initial indexed date for a field.
     * 
     * @param baseIndexOffset
     */
    public void addDateIndex(int baseIndexOffset) {
        if (indexedDatesBitSet != null)
            indexedDatesBitSet.set(baseIndexOffset);
        else
            log.info("Need to initialize the indexDateSet");
    }
    
    /**
     * Sets the indexedDateSet if created externally in another class.
     * 
     * @param externalDateSet
     */
    public void setIndexedDatesBitSet(BitSet externalDateSet) {
        if (externalDateSet != null)
            this.indexedDatesBitSet = externalDateSet;
    }
    
    @Override
    public String toString() {
        return "Start date: " + startDay + " Bitset: " + this.indexedDatesBitSet.toString();
    }
    
    @Override
    public int compareTo(IndexedDatesValue other) {
        if (other.startDay == null || startDay == null) {
            log.error("The start day should never be null");
            return COMPARE_FAILED;
        }
        
        return this.startDay.compareTo(other.getStartDay());
    }
    
}
