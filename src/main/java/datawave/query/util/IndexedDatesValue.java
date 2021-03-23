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
    private TreeSet<YearMonthDay> indexedDatesSet = new TreeSet<>();
    private BitSet indexedDatesBitSet = null;
    public static final int YYMMDDSIZE = 8;
    public static final int COMPARE_FAILED = Integer.MAX_VALUE;
    
    public IndexedDatesValue(YearMonthDay startDate) {
        startDay = startDate;
        indexedDatesBitSet = new BitSet(1);
        indexedDatesBitSet.set(0);
        if (startDate != null)
            indexedDatesSet.add(startDate);
    }
    
    public IndexedDatesValue() {
        startDay = null;
    }
    
    public YearMonthDay getStartDay() {
        return startDay;
    }
    
    public void setStartDay(YearMonthDay startDay) {
        this.startDay = startDay;
        indexedDatesSet.add(startDay);
    }
    
    /**
     * Serialize a Bitset representing indexed dates and initial date the field was indexed.
     *
     * build byte array with the start day at the beginning and the bytes of the bit set remaining
     *
     * @return
     */
    public Value serialize() {
        
        try {
            if (startDay == null) {
                log.warn("Start date was not initialized before serialization");
                startDay = indexedDatesSet.first();
            }
            
        } catch (NoSuchElementException noSuchElementException) {
            log.warn("No such element exception occurs when the indexedDateSet is not populated.");
            log.warn("Default Empty IndexedDatesValue was created and never populated");
            return new Value();
        }
        
        // TODO Remove this try catch block and get the TreeSet<YearMonthDay> out of class
        // The indexedDatesSet is used for testing the class so the test needs to change.
        try {
            YearMonthDay firstDay = indexedDatesSet.first();
            
            if (!firstDay.equals(startDay)) {
                log.warn("First day in treeset should be the start day");
                log.warn("Start day will now be initialized to firstDate in the sorted treeset");
                startDay = firstDay;
            }
            
            YearMonthDay lastDay = indexedDatesSet.last();
            // TODO Do a better job estimating the size using the ordinals
            // Estimate the span of dates with firstDay and lastDay
            int bitSetSize;
            if (firstDay.equals(lastDay))
                bitSetSize = 1;
            else if (lastDay.getYear() == firstDay.getYear())
                bitSetSize = lastDay.getJulian() - firstDay.getJulian() + 1;
            else // Estimate the span of dates with firstDay and lastDay
                bitSetSize = (lastDay.getYear() - firstDay.getYear() + 1) * 366;
            indexedDatesBitSet = new BitSet(bitSetSize);
            int dayIndex = 0;
            YearMonthDay nextDay = startDay;
            
            for (YearMonthDay ymd : indexedDatesSet) {
                if (ymd.compareTo(nextDay) == 0) {
                    indexedDatesBitSet.set(dayIndex);
                    dayIndex++;
                    nextDay = YearMonthDay.nextDay(nextDay.getYyyymmdd());
                } else {
                    do {
                        dayIndex++;
                        nextDay = YearMonthDay.nextDay(nextDay.getYyyymmdd());
                        if (ymd.compareTo(nextDay) == 0) {
                            indexedDatesBitSet.set(dayIndex);
                        }
                        
                    } while (nextDay.compareTo(ymd) < 0);
                }
                
            }
        } catch (NoSuchElementException noSuchElementException) {
            log.warn("The empty contructor was called and bitset and start day never populated.");
            if (indexedDatesBitSet == null)
                return new Value();
        }
        
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
        TreeSet<YearMonthDay> indexedDatesSet = new TreeSet<>();
        indexedDatesSet.add(startDay);
        YearMonthDay nextday = startDay;
        if (accumuloValue.length > 8) {
            BitSet theIndexedDates = BitSet.valueOf(Arrays.copyOfRange(accumuloValue, YYMMDDSIZE, accumuloValue.length));
            indexedDates.setIndexedDatesBitSet(theIndexedDates);
            for (int i = 1; i < theIndexedDates.size(); i++) {
                nextday = YearMonthDay.nextDay(nextday.getYyyymmdd());
                if (theIndexedDates.get(i)) {
                    indexedDatesSet.add(nextday);
                }
            }
            indexedDates.setIndexedDatesSet(indexedDatesSet);
            log.info("The number of dates is " + indexedDatesSet.size());
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
        TreeSet<YearMonthDay> indexedDatesSet = new TreeSet<>();
        indexedDatesSet.add(startDay);
        YearMonthDay nextday = startDay;
        if (accumuloValue.get().length > 8) {
            BitSet theIndexedDates = BitSet.valueOf(Arrays.copyOfRange(accumuloValue.get(), YYMMDDSIZE, accumuloValue.get().length));
            indexedDates.setIndexedDatesBitSet(theIndexedDates);
            for (int i = 1; i < theIndexedDates.size(); i++) {
                nextday = YearMonthDay.nextDay(nextday.getYyyymmdd());
                if (theIndexedDates.get(i)) {
                    indexedDatesSet.add(nextday);
                }
            }
            indexedDates.setIndexedDatesSet(indexedDatesSet);
            log.info("The number of dates is " + indexedDatesSet.size());
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
     * Returns the TreeSet of dates a field was indexed
     * 
     * @return
     */
    public TreeSet<YearMonthDay> getIndexedDatesSet() {
        return indexedDatesSet;
    }
    
    public void setIndexedDatesSet(TreeSet<YearMonthDay> indexedDatesSet) {
        this.indexedDatesSet = indexedDatesSet;
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
     * Puts a date a field was indexed into the TreeSet to be used during serialization to create the bit set called indexedDateSet.
     * 
     * @param indexedDate
     */
    
    public void addIndexedDate(YearMonthDay indexedDate) {
        indexedDatesSet.add(indexedDate);
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
    
    public void clear() {
        indexedDatesSet.clear();
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
