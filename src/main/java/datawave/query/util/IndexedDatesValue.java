package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.TreeSet;

public class IndexedDatesValue {
    
    private static final Logger log = LoggerFactory.getLogger(IndexedDatesValue.class);
    
    private YearMonthDay startDay;
    private TreeSet<YearMonthDay> indexedDatesSet = new TreeSet<>();
    private BitSet indexedDatesBitSet;
    public static final int YYMMDDSIZE = 8;

    public IndexedDatesValue(){
    }
    
    public IndexedDatesValue(YearMonthDay startDate) {
        startDay = startDate;
        indexedDatesBitSet = new BitSet(1);
        indexedDatesBitSet.set(0);
        indexedDatesSet.add(startDate);
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

        //TODO the next line is wrong.  The size of the BitSet needs.
        //to be the length of days in the span of days in indexedDatesSet
        //
        YearMonthDay firstDay = indexedDatesSet.first();
        if (!firstDay.equals(startDay))
            log.error("First day in treeset should be the start day");
        YearMonthDay lastDay = indexedDatesSet.last();
        //Estimate the span of dates with firstDay and lastDay
        int bitSetSize;
        if (lastDay.getYear() == firstDay.getYear())
            bitSetSize = lastDay.getJulian() - firstDay.getJulian() + 1;
        else  //Estimate the span of dates with firstDay and lastDay
            bitSetSize = (lastDay.getYear() - firstDay.getYear() + 1) * 366;
        indexedDatesBitSet = new BitSet(bitSetSize);
        int dayIndex = 0;
        YearMonthDay nextDay = startDay;
        

        for (YearMonthDay ymd : indexedDatesSet) {
            System.out.println("Processing date  " + ymd);

            if (ymd.compareTo(nextDay) == 0) {
                System.out.println("Setting index bit for  " + ymd);
                indexedDatesBitSet.set(dayIndex);
                dayIndex++;
                nextDay = YearMonthDay.nextDay(nextDay.getYyyymmdd());
            }
            else{
                do {
                    dayIndex++;
                    nextDay = YearMonthDay.nextDay(nextDay.getYyyymmdd());
                    if (ymd.compareTo(nextDay) == 0) {
                        System.out.println("Setting index bit for  " + ymd);
                        indexedDatesBitSet.set(dayIndex);
                    }


                }while (nextDay.compareTo(ymd) < 0);
            }

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

    public static IndexedDatesValue deserialize(Value accumuloValue) {
        String yyyyMMdd = new String(Arrays.copyOfRange(accumuloValue.get(), 0, YYMMDDSIZE), StandardCharsets.UTF_8);
        YearMonthDay startDay = new YearMonthDay(yyyyMMdd);
        IndexedDatesValue indexedDates = new IndexedDatesValue(startDay);
        TreeSet<YearMonthDay> indexedDatesSet = new TreeSet<>();
        indexedDatesSet.add(startDay);
        YearMonthDay nextday = startDay;
        if (accumuloValue.get().length > 8) {
            BitSet theIndexedDates = BitSet.valueOf(Arrays.copyOfRange(accumuloValue.get(), YYMMDDSIZE, accumuloValue.get().length));
            indexedDates.setIndexedDatesBitSet(theIndexedDates);
            log.info("The number of dates is " + theIndexedDates.size());
            for (int i = 1;i < theIndexedDates.size(); i++ ){
                nextday = YearMonthDay.nextDay(nextday.getYyyymmdd());
                if (theIndexedDates.get(i))
                {
                     indexedDatesSet.add(nextday);
                }
            }
            indexedDates.setIndexedDatesSet(indexedDatesSet);
        }

        return indexedDates;
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

    public void clear(){
        indexedDatesSet.clear();
    }
    
    @Override
    public String toString() {
        return "Start date: " + startDay + " Bitset: " + this.indexedDatesBitSet.toString();
    }
    
    public static void main(String args[]) {
     /*   BitSet bits1 = new BitSet(24);
        BitSet bits2 = new BitSet(24);
        BitSet bits3 = new BitSet(24);

        // set some bits
        for (int i = 0; i < 24; i++) {
            if ((i % 4) == 0)
                bits1.set(i);
            bits2.set(i);
            bits3.set(i);
        }
        
        System.out.println("Initial pattern in bits1: ");
        System.out.println(bits1);
        System.out.println("\nInitial pattern in bits2: ");
        System.out.println(bits2);
        
        // AND bits
        bits2.and(bits1);
        System.out.println("\nbits2 AND bits1: ");
        System.out.println(bits2);
        
        // OR bits
        bits2.or(bits1);
        System.out.println("\nbits2 OR bits1: ");
        System.out.println(bits2);
        
        // XOR bits
        bits2.xor(bits1);
        System.out.println("\nbits2 XOR bits1: ");
        System.out.println(bits2);

        System.out.println("Bitset should be the same as:");
        System.out.println(bits3);*/

        IndexedDatesValue datesValue = new IndexedDatesValue(new YearMonthDay("20081225"));



        YearMonthDay nextDay = new YearMonthDay("20081225");
        System.out.println(nextDay);

        int i = 0;
        while (i < 10){
            nextDay = YearMonthDay.nextDay(nextDay.getYyyymmdd());
            if ((i % 2) == 0)
                 datesValue.addIndexedDate(nextDay);
            //System.out.println(nextDay);
            i++;
        }

        Value value = datesValue.serialize();
        
        IndexedDatesValue datesValue2 = IndexedDatesValue.deserialize(value);
        System.out.println(datesValue2);
        for (YearMonthDay yearMonthDay: datesValue2.getIndexedDatesSet())
        {
            System.out.println(yearMonthDay);
        }





    }
    
}
