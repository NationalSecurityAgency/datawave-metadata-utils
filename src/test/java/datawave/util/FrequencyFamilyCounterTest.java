package datawave.util;

import datawave.query.util.FrequencyFamilyCounter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

//TODO Get this to really work
public class FrequencyFamilyCounterTest {
    
    private FrequencyFamilyCounter counter = new FrequencyFamilyCounter();
    
    @Test
    public void testFrequencyColumnCompression() {
        ProcessBuilder builder = new ProcessBuilder("accumulo shell -u root -p secret --execute-file resources/frequencyColumnCompressionData.txt");
        
        Process process = null;
        try {
            process = builder.start();
        } catch (IOException ioException) {
            System.out.println("Could not start the Datawave Quickstart " + ioException.getMessage());
        }
        if (process != null) {
            try {
                process.waitFor(3, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
                
            }
        } else {
            System.out.println("Datawave Failed to start");
            return;
        }
        
        if (process != null && process.exitValue() != 0)
            System.out.println("Datawave Failed to start");
        
    }
    
}
