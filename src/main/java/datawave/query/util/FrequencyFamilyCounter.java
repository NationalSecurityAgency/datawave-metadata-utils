package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FrequencyFamilyCounter {
    
    private long total = 0L;
    private HashMap<String,Integer> dateToFrequencyValueMap = new HashMap<>();
    private static final int SIMPLEDATE_LENGTH = 8;
    private DateFrequencyValue serializer = new DateFrequencyValue();
    private boolean usesGZipCompression;
    
    // private static Pattern SimpleDatePattern = Pattern.compile("^(19|20)\\d\\d[- /.] (0[1-9]|1[012])[- /.] (0[1-9]|[12][0-9]|3[01])$");
    
    private static final Logger log = LoggerFactory.getLogger(FrequencyFamilyCounter.class);
    
    public FrequencyFamilyCounter(boolean usesGZip) {
        usesGZipCompression = usesGZip;
    }
    
    public void initialize(Value value) {
        deserializeCompressedValue(value);
    }
    
    public void clear() {
        dateToFrequencyValueMap.clear();
    }
    
    public HashMap<String,Integer> getDateToFrequencyValueMap() {
        return dateToFrequencyValueMap;
    }
    
    /**
     * Takes the value of a compressed f column record and creates the qualifierToFrequencyValueMap so that other records that are not yet aggregated can be
     * added to the compressed record. After those records are aggregated they are discarded.
     *
     * @param oldValue
     */
    public void deserializeCompressedValue(Value oldValue) {
        
        HashMap<String,Integer> uncompressedValueMap = serializer.deserialize(oldValue, usesGZipCompression);
        dateToFrequencyValueMap.clear();
        dateToFrequencyValueMap.putAll(uncompressedValueMap);
    }
    
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
        
        log.info("inserting key: " + cleanKey + " value: " + value);
        if (value.isEmpty())
            return;
        
        try {
            parsedLong = Integer.parseUnsignedInt(value);
            total += parsedLong;
        } catch (NumberFormatException nfe) {
            try {
                log.info("Long.parseLong could not parse " + value + " to long for this key " + cleanKey, nfe);
                log.info("Trying to use Long.decode");
                parsedLong = Integer.decode(value);
                total += parsedLong;
            } catch (NumberFormatException nfe2) {
                log.error("Long.decode could not parse " + value + " to long for this key " + cleanKey, nfe2);
                log.error("Key " + key + " and value: " + value + " could not be inserted into new record");
                return;
            }
            
        }
        
        try {
            
            if (!dateToFrequencyValueMap.containsKey(cleanKey))
                dateToFrequencyValueMap.put(cleanKey, parsedLong);
            else {
                
                int lastValue = dateToFrequencyValueMap.get(cleanKey);
                dateToFrequencyValueMap.put(cleanKey, lastValue + parsedLong);
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
        return serializer.serialize(dateToFrequencyValueMap, usesGZipCompression);
    }
    
}
