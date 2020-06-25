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
        // TODO Build a loop that does an entry every day for two years
        dateFrequencyUncompressed.put("20190101", 1);
        dateFrequencyUncompressed.put("20190102", 2);
        dateFrequencyUncompressed.put("20190131", 31);
        dateFrequencyUncompressed.put("20190228", Integer.MAX_VALUE);
        dateFrequencyUncompressed.put("20190301", 3000);
        dateFrequencyUncompressed.put("20190330", 330);
        dateFrequencyUncompressed.put("20191220", 1220);
        dateFrequencyUncompressed.put("20191231", 1231);
        
        dateFrequencyUncompressed.put("20200101", 1);
        dateFrequencyUncompressed.put("20200102", 2);
        dateFrequencyUncompressed.put("20200131", 31);
        dateFrequencyUncompressed.put("20200229", Integer.MAX_VALUE);
        dateFrequencyUncompressed.put("20200301", 3000);
        dateFrequencyUncompressed.put("20200330", 330);
        dateFrequencyUncompressed.put("20201220", 1220);
        dateFrequencyUncompressed.put("20201231", 1231);
        
    }
    
    @Test
    public void DateFrequencyValueTest() {
        dateFrequencyValue.serialize(dateFrequencyUncompressed, true);
        dateFrequencyValue.dumpCompressedDateFrequencies();
        byte[] compressedMapBytes = dateFrequencyValue.getCompressedDateFrequencyMapBytes();
        Assert.assertTrue(compressedMapBytes != null);
        Value accumlo_value = new Value(compressedMapBytes);
        HashMap<String,Integer> restored = dateFrequencyValue.deserialize(accumlo_value);
        
        for (Map.Entry<String,Integer> entry : restored.entrySet()) {
            log.info("key is: " + entry.getKey() + " value is: " + entry.getValue());
        }
        log.info("The restored size is " + restored.size());
        log.info("The size of the unprocessed frequency map is " + dateFrequencyUncompressed.size());
        Assert.assertTrue(dateFrequencyUncompressed.size() == 16);
        Assert.assertTrue(restored.size() == 16);
        
    }
    
}
