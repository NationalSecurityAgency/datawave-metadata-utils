package datawave.util;

import datawave.query.util.DateFrequencyValue;
import org.junit.Assert;
import org.junit.Test;

public class Base256CompressionTest {
    
    @Test
    public void nullBytePrefixedByteArrayTest() {
        byte[] testArray = {'\u0000', '\u0000', '\u0001', '\u0001'};
        int result = DateFrequencyValue.Base256Compression.bytesToInteger(testArray[0], testArray[1], testArray[2], testArray[3]);
        Assert.assertTrue(result == 257);
        byte[] testArray2 = {'\u0000', '\u0000', '\u0000', '\u0001'};
        result = DateFrequencyValue.Base256Compression.bytesToInteger(testArray2[0], testArray2[1], testArray2[2], testArray2[3]);
        Assert.assertTrue(result == 1);
    }
    
}
