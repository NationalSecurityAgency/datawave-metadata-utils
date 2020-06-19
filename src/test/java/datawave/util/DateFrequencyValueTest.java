package datawave.util;

import datawave.query.util.DateFrequencyValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class DateFrequencyValueTest {
    DateFrequencyValue dateFrequencyValue = new DateFrequencyValue();
    HashMap<String,Integer> dateFrequencyUncompressed = new HashMap<>();
    
    @Before
    public void initialize() {
        dateFrequencyUncompressed = new HashMap<>();
        dateFrequencyUncompressed.put("01202019", 1192919929);
        dateFrequencyUncompressed.put("01212019", Integer.MAX_VALUE);
        dateFrequencyUncompressed.put("01222019", 2_000_000_000);
        dateFrequencyUncompressed.put("01232019", 255);
        
    }
    
    @Test
    public void DateFrequencyValueTest() {
        dateFrequencyValue.serialize(dateFrequencyUncompressed, true);
        
    }
    
}
