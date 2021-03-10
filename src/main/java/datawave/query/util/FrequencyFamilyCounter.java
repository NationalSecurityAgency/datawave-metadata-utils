package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.util.Frequency;
import datawave.query.util.YearMonthDay;
import java.util.TreeMap;

/**
 * This class contains the TreeMap of date to frequency values that get changed during the tranformRange operation in the FrequencyTransformIterator. It mainly
 * performs the task of a SummingCombiner of the date frequency values. The class uses the DateFrequencyValue class to serialize and deserialize values into and
 * out of Accumulo.
 */

public class FrequencyFamilyCounter {
    
    private TreeMap<YearMonthDay,Frequency> dateToFrequencyValueMap = new TreeMap<>();
    private static final int SIMPLEDATE_LENGTH = 8;
    private DateFrequencyValue serializer = new DateFrequencyValue();
    
    private static final Logger log = LoggerFactory.getLogger(FrequencyFamilyCounter.class);
    
    public FrequencyFamilyCounter() {}
    
    public void clear() {
        dateToFrequencyValueMap.clear();
    }
    
    public TreeMap<YearMonthDay,Frequency> getDateToFrequencyValueMap() {
        return dateToFrequencyValueMap;
    }
    
    /**
     * Takes the value of a compressed f column record and resets the dateToFrequencyValueMap with its contents.
     *
     * @param oldValue
     */
    public void deserializeCompressedValue(Value oldValue) {
        deserializeCompressedValue(oldValue, null);
    }
    
    /**
     * Takes the value of a compressed f column record and resets the dateToFrequencyValueMap with its contents.
     *
     * @param oldValue
     * @param ageOffDate
     *            yyyyMMdd ageoff date, null implies no ageoff
     */
    public void deserializeCompressedValue(Value oldValue, String ageOffDate) {
        dateToFrequencyValueMap = serializer.deserialize(oldValue, ageOffDate, false, null, false);
    }
    
    /**
     * Takes the value of a compressed f column record and resets the dateToFrequencyValueMap with its contents.
     *
     * @param oldValue
     * @param beginDate
     *            yyyyMMdd ageoff date, null implies no begin date
     * @param endDate
     *            yyyyMMdd ageoff date, null implies no end date
     */
    public void deserializeCompressedValue(Value oldValue, String beginDate, String endDate) {
        dateToFrequencyValueMap = serializer.deserialize(oldValue, beginDate, true, endDate, true);
    }
    
    /**
     * Takes the value of a compressed f column record and aggregates it into dateToFrequencyValueMap current.
     *
     * @param oldValue
     */
    public void aggregateCompressedValue(Value oldValue) {
        aggregateCompressedValue(oldValue, null);
    }
    
    /**
     * Takes the value of a compressed f column record and aggregates it into dateToFrequencyValueMap current.
     *
     * @param oldValue
     * @param ageOffDate
     *            yyyyMMdd ageoff date, null implies no ageoff
     */
    public void aggregateCompressedValue(Value oldValue, String ageOffDate) {
        dateToFrequencyValueMap = serializer.deserialize(dateToFrequencyValueMap, oldValue, ageOffDate, false, null, false);
    }
    
    /**
     * Performs what the SummingCombiner used to perform for date and frequency values. The function accumulutes ingest frequencies for individual dates
     */
    
    public void aggregateRecord(String key, int value) {
        
        insertIntoMap(key, value);
    }
    
    /**
     * Inserts a key and value into the qualifiedToFrequencyValueMap and converts the string value to a long
     * 
     * @param key
     *            The value for "key" is the old style ColumnQualifier of this format: csv\x0020160426
     * @param value
     *            value is a an Integer in string format that might be hexadecimal, decimal or octal
     */
    public void insertIntoMap(String key, int value) {
        String cleanKey;
        
        // Assuming that as SimpleDate is at the end of the key passed in. yyyyMMdd
        // The value for "key" is the old style ColumnQualifier of this format:
        // csv\x0020160426 and the value is a an Integer in string format that
        // might be hexadecimal, decimal or octal
        if (key != null) {
            if (key.length() > SIMPLEDATE_LENGTH) {
                cleanKey = key.substring(key.length() - SIMPLEDATE_LENGTH);
            } else {
                cleanKey = key;
            }
        } else
            return;
        
        if (log.isTraceEnabled())
            log.trace("inserting key: " + cleanKey + " value: " + value);
        
        try {
            
            if (!dateToFrequencyValueMap.containsKey(new YearMonthDay(cleanKey)))
                dateToFrequencyValueMap.put(new YearMonthDay(cleanKey), new Frequency(value));
            else {
                Frequency lastValue = dateToFrequencyValueMap.get(new YearMonthDay(cleanKey));
                lastValue.addFrequency(value);
                dateToFrequencyValueMap.put(new YearMonthDay(cleanKey), lastValue);
            }
            
        } catch (Exception e) {
            log.error("Key " + key + " and value: " + value + " could not be inserted into new record");
        }
    }
    
    /**
     * Return the serialized value of the qualifierToFrequencyValueMap Presently only called by the FrequencyTransformIterator after aggregating a rows
     * frequency records
     * 
     * @return Value
     */
    public Value serialize() {
        return serializer.serialize(dateToFrequencyValueMap);
    }
    
}
