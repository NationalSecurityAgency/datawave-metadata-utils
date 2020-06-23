package datawave.util;

import datawave.query.util.DateFrequencyValue;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DateFrequencyValueTest {
    DateFrequencyValue dateFrequencyValue = new DateFrequencyValue();
    HashMap<String,Integer> dateFrequencyUncompressed = new HashMap<>();
    
    private static final Logger log = LoggerFactory.getLogger(DateFrequencyValueTest.class);
    
    @Before
    public void initialize() {
        dateFrequencyUncompressed = new HashMap<>();
        dateFrequencyUncompressed.put("20190120", 1192919929);
        dateFrequencyUncompressed.put("20190121", Integer.MAX_VALUE);
        dateFrequencyUncompressed.put("20190122", 2_000_000_000);
        dateFrequencyUncompressed.put("20190123", 255);
        dateFrequencyUncompressed.put("20200120", 1192919929);
        dateFrequencyUncompressed.put("20200121", Integer.MAX_VALUE);
        dateFrequencyUncompressed.put("20200122", 2_000_000_000);
        dateFrequencyUncompressed.put("20200123", 255);
        
    }
    
    @Test
    public void DateFrequencyValueTest() {
        dateFrequencyValue.serialize(dateFrequencyUncompressed, true);
        dateFrequencyValue.dumpCompressedDateFrequencies();
        byte[] compressedMapBytes = dateFrequencyValue.getCompressedDateFrequencyMapBytes();
        Value accumlo_value = new Value(compressedMapBytes);
        HashMap<String,Integer> restored = dateFrequencyValue.deserialize(accumlo_value);
        for (Map.Entry<String,Integer> entry : restored.entrySet()) {
            log.info("key is: " + entry.getKey() + " value is: " + entry.getValue());
        }
        
    }
    
}
