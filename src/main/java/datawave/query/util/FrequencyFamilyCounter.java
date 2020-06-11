package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FrequencyFamilyCounter {
    
    private long total = 0L;
    private HashMap<String,Long> dateToFrequencyValueMap = new HashMap<>();
    private static final int SIMPLEDATE_LENGTH = 8;
    
    // private static Pattern SimpleDatePattern = Pattern.compile("^(19|20)\\d\\d[- /.] (0[1-9]|1[012])[- /.] (0[1-9]|[12][0-9]|3[01])$");
    
    private static final Logger log = LoggerFactory.getLogger(FrequencyFamilyCounter.class);
    
    public FrequencyFamilyCounter() {}
    
    public void initialize(Value value) {
        deserializeCompressedValue(value);
    }
    
    public void clear() {
        dateToFrequencyValueMap.clear();
        total = 0;
    }
    
    public void add(long newCounts) {
        total += newCounts;
    }
    
    public long getTotal() {
        return total;
    }
    
    public HashMap<String,Long> getDateToFrequencyValueMap() {
        return dateToFrequencyValueMap;
    }
    
    /**
     * Takes the value of a compressed f column record and creates the qualifierToFrequencyValueMap so that other records that are not yet aggregated can be
     * added to the compressed record. After those records are aggregated they are discarded.
     *
     * @param oldValue
     */
    public void deserializeCompressedValue(Value oldValue) {
        String[] kvps = oldValue.toString().split("\\|");
        log.info("deserializeCompressedValue: there are " + kvps.length + " key value pairs.");
        for (String kvp : kvps) {
            String[] pair = kvp.split("^");
            if (pair.length == 2) {
                log.info("deserializeCompressedValue -- cq: " + pair[0] + " value: " + pair[1]);
                String key = pair[0];
                String value = pair[1];
                log.info("deserializeCompressedValue key: " + pair[0] + " value: " + pair[2]);
                insertIntoMap(key, value);
                
            }
        }
        log.info("The contents of the frequency map are " + dateToFrequencyValueMap.toString());
    }
    
    public void aggregateRecord(String key, String value) {
        
        insertIntoMap(key, value);
    }
    
    /**
     * Inserts a key and value into the qualifiedToFrequencyValueMap and converts the string value to a long
     * 
     * @param key
     * @param value
     */
    public void insertIntoMap(String key, String value) {
        long parsedLong;
        String cleanKey = "null";
        
        // Assuming that as SimpleDate is at the end of the key passed in. yyyyMMdd
        if (key != null) {
            if (key.length() > SIMPLEDATE_LENGTH) {
                cleanKey = key.substring(key.length() - SIMPLEDATE_LENGTH);
            } else if (key.length() <= SIMPLEDATE_LENGTH) {
                cleanKey = key;
            }
        } else
            return;
        
        log.info("inserting key: " + cleanKey + " value: " + value);
        if (value.isEmpty())
            return;
        
        try {
            parsedLong = Long.parseLong(value);
            total += parsedLong;
        } catch (Exception e) {
            try {
                log.info("Could not parse " + value + " to long for this key " + cleanKey, e);
                log.info("Trying to use Byte.decode");
                parsedLong = Long.parseLong(String.valueOf(Byte.decode(value)));
                total += parsedLong;
            } catch (Exception e2) {
                log.error("Could not parse " + value + " to long for this key " + cleanKey, e2);
                return;
            }
            
        }
        
        try {
            
            if (!dateToFrequencyValueMap.containsKey(cleanKey))
                dateToFrequencyValueMap.put(cleanKey, parsedLong);
            else {
                
                long lastValue = dateToFrequencyValueMap.get(cleanKey);
                dateToFrequencyValueMap.put(cleanKey, lastValue + parsedLong);
            }
        } catch (Exception e) {
            log.error("Error inserting into map", e);
        }
    }
    
    /**
     * Return the serialized value of the qualifierToFrequencyValueMap Presently only called by the FrequencyTransformIterator after aggregating a rows
     * frequency records
     * 
     * @return Value
     */
    public Value serialize() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String,Long> entry : dateToFrequencyValueMap.entrySet()) {
            sb.append(entry.getKey()).append("^").append(entry.getValue()).append("|");
        }
        
        return new Value(sb.toString());
    }
    
}
