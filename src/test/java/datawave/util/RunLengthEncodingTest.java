package datawave.util;

import datawave.query.util.RunLengthEncoding;
import org.junit.Assert;
import org.junit.Test;

public class RunLengthEncodingTest {
    
    @Test
    public void encodingTest() {
        
        Assert.assertTrue(new String(new char[] {'3', '\u0000'}).equals(RunLengthEncoding.encode(new String(new char[] {'\u0000', '\u0000', '\u0000'}))));
        Assert.assertTrue("4W".equals(RunLengthEncoding.encode("WWWW")));
        Assert.assertTrue("5w4i7k3i6p5e4d2i1a".equals(RunLengthEncoding.encode("wwwwwiiiikkkkkkkiiippppppeeeeeddddiia")));
        Assert.assertTrue("12B1N12B3N24B1N14B".equals(RunLengthEncoding.encode("BBBBBBBBBBBBNBBBBBBBBBBBBNNNBBBBBBBBBBBBBBBBBBBBBBBBNBBBBBBBBBBBBBB")));
        Assert.assertTrue("12W1B12W3B24W1B14W".equals(RunLengthEncoding.encode("WWWWWWWWWWWWBWWWWWWWWWWWWBBBWWWWWWWWWWWWWWWWWWWWWWWWBWWWWWWWWWWWWWW")));
        Assert.assertTrue("1W1B1W1B1W1B1W1B1W1B1W1B1W1B".equals(RunLengthEncoding.encode("WBWBWBWBWBWBWB")));
        
    }
    
    @Test
    public void decodingTest() {
        Assert.assertTrue("W".equals(RunLengthEncoding.decode("1W")));
        Assert.assertTrue(new String(new byte[] {'\u0000', '\u0000', '\u0000'}).equals(RunLengthEncoding.decode(new String(new byte[] {'3', '\u0000'}))));
        Assert.assertTrue(new String(new char[] {'\u0000', '\u0000', '\u0000'}).equals(RunLengthEncoding.decode(new String(new char[] {'3', '\u0000'}))));
        Assert.assertTrue("\u0000\u0000\u0000".equals(RunLengthEncoding.decode(new String(new char[] {'3', '\u0000'}))));
        Assert.assertTrue("WWWW".equals(RunLengthEncoding.decode("4W")));
        Assert.assertTrue("WWWWWWWWWWWWBWWWWWWWWWWWWBBBWWWWWWWWWWWWWWWWWWWWWWWWBWWWWWWWWWWWWWW".equals(RunLengthEncoding.decode("12W1B12W3B24W1B14W")));
        Assert.assertTrue("WBWBWBWBWBWBWB".equals(RunLengthEncoding.decode("1W1B1W1B1W1B1W1B1W1B1W1B1W1B")));
        Assert.assertTrue("WBWBWBWBWBWBWB".equals(RunLengthEncoding.decode("1W1B1W1B1W1B1W1B1W1B1W1B1W1B")));
        
    }
}
