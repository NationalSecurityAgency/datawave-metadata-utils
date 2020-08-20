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
    
    public FrequencyFamilyCounter() {
        
    }
    
    public void initialize(Value value) {
        deserializeCompressedValue(value);
    }
    
    public void clear() {
        dateToFrequencyValueMap.clear();
    }
    
    public TreeMap<YearMonthDay,Frequency> getDateToFrequencyValueMap() {
        return dateToFrequencyValueMap;
    }
    
    /**
     * Takes the value of a compressed f column record and creates the qualifierToFrequencyValueMap so that other records that are not yet aggregated can be
     * added to the compressed record. After those records are aggregated they are discarded.
     *
     * @param oldValue
     */
    public void deserializeCompressedValue(Value oldValue) {
        dateToFrequencyValueMap = serializer.deserialize(oldValue);
    }
    
    /**
     * Performs what the SummingCombiner used to perform for date and frequency values. The function accumulutes ingest frequencies for individual dates
     */
    
    public void aggregateRecord(String key, String value) {
        
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
    public void insertIntoMap(String key, String value) {
        int parsedLong;
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
        
        if (value.isEmpty())
            return;
        
        try {
            parsedLong = Integer.parseUnsignedInt(value);
        } catch (NumberFormatException nfe) {
            try {
                if (log.isTraceEnabled()) {
                    log.trace("Long.parseLong could not parse " + value + " to long for this key " + cleanKey);
                    log.trace("Trying to use Long.decode");
                }
                parsedLong = Integer.decode(value);
                if (log.isTraceEnabled())
                    log.trace("Long.decode processed " + value);
            } catch (NumberFormatException nfe2) {
                log.error("Long.decode could not parse " + value + " to long for this key " + cleanKey, nfe2);
                log.error("Key " + key + " and value: " + value + " could not be inserted into new record");
                return;
            }
            
        }
        
        try {
            
            if (!dateToFrequencyValueMap.containsKey(new YearMonthDay(cleanKey)))
                dateToFrequencyValueMap.put(new YearMonthDay(cleanKey), new Frequency(parsedLong));
            else {
                Frequency lastValue = dateToFrequencyValueMap.get(new YearMonthDay(cleanKey));
                lastValue.addFrequency(parsedLong);
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
