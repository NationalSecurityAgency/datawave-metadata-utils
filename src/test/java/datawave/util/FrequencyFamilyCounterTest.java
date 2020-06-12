package datawave.util;

import datawave.query.util.FrequencyFamilyCounter;
import org.junit.Assert;
import org.junit.Test;

public class FrequencyFamilyCounterTest {
    
    private FrequencyFamilyCounter counter = new FrequencyFamilyCounter();
    
    @Test
    public void testCounter() {
        Assert.assertTrue(counter != null);
    }
    
}
