package datawave.query.util;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FrequencyFamilyCounter {
    
    private long total = 0L;
    private HashMap<String,Long> qualifierToFrequencyValueMap = new HashMap<>();
    
    private static final Logger log = LoggerFactory.getLogger(FrequencyFamilyCounter.class);
    
    public FrequencyFamilyCounter() {}
    
    public void initialize(Value value) {
        deserializeCompressedValue(value);
    }
    
    public void clear() {
        qualifierToFrequencyValueMap.clear();
        total = 0;
    }
    
    public void add(long newCounts) {
        total += newCounts;
    }
    
    public long getTotal() {
        return total;
    }
    
    public HashMap<String,Long> getQualifierToFrequencyValueMap() {
        return qualifierToFrequencyValueMap;
    }
    
    public void deserializeCompressedValue(Value oldValue) {
        String[] kvps = oldValue.toString().split("|");
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
        log.info("The contents of the frequency map are " + qualifierToFrequencyValueMap.toString());
    }
    
    public void insertIntoMap(String key, String value) {
        long parsedLong;
        
        log.info("inserting key: " + key + " value: " + value);
        if (value.isEmpty())
            return;
        
        try {
            parsedLong = Long.parseLong(value);
            total += parsedLong;
        } catch (Exception e) {
            log.error("Could not parse " + value + " to long for this key " + key, e);
            return;
        }
        
        try {
            
            if (!qualifierToFrequencyValueMap.containsKey(key))
                qualifierToFrequencyValueMap.put(key, parsedLong);
            else {
                
                long lastValue = qualifierToFrequencyValueMap.get(key);
                qualifierToFrequencyValueMap.put(key, lastValue + parsedLong);
            }
        } catch (Exception e) {
            log.error("Error inserting into map", e);
        }
    }
    
    public Value serialize() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String,Long> entry : qualifierToFrequencyValueMap.entrySet()) {
            sb.append(entry.getKey()).append("^").append(entry.getValue()).append("|");
        }
        
        return new Value(sb.toString());
    }
    
}
