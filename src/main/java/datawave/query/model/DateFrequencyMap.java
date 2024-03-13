package datawave.query.model;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class DateFrequencyMap implements Writable {
    
    private final SortedMap<String,Frequency> dateToFrequencies;
    
    public DateFrequencyMap() {
        this.dateToFrequencies = new TreeMap<>();
    }
    
    public DateFrequencyMap(SortedMap<String,Frequency> dateToFrequencies) {
        Preconditions.checkNotNull(dateToFrequencies, "date-frequency map must not be null");
        this.dateToFrequencies = dateToFrequencies;
    }
    
    public DateFrequencyMap(byte[] bytes) throws IOException {
        this();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        readFields(dataIn);
        dataIn.close();
    }
    
    public void setFrequency(String date, long value) {
        dateToFrequencies.put(date, new Frequency(value));
    }
    
    public void put(String date, long value) {
        dateToFrequencies.put(date, new Frequency(value));
    }
    
    public void increment(String date, long value) {
        dateToFrequencies.computeIfAbsent(date, (k) -> new Frequency()).incrementBy(value);
    }
    
    public void incrementAll(DateFrequencyMap map) {
        for (Map.Entry<String,Frequency> entry : map.dateToFrequencies.entrySet()) {
            increment(entry.getKey(), entry.getValue().getValue());
        }
    }
    
    public Frequency getFrequency(String date) {
        return dateToFrequencies.get(date);
    }
    
    public boolean containsKey(String date) {
        return dateToFrequencies.containsKey(date);
    }
    
    public SortedMap<String,Frequency> getFrequencies(Collection<String> dates) {
        SortedMap<String,Frequency> map = new TreeMap<>();
        for (String date : dates) {
            if (this.dateToFrequencies.containsKey(date)) {
                map.put(date, this.dateToFrequencies.get(date));
            }
        }
        return map;
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Write the map's size.
        WritableUtils.writeVInt(dataOutput, dateToFrequencies.size());
        
        // Write each entry.
        for (Map.Entry<String,Frequency> entry : dateToFrequencies.entrySet()) {
            WritableUtils.writeString(dataOutput, entry.getKey());
            entry.getValue().write(dataOutput);
        }
    }
    
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // Clear the map.
        this.dateToFrequencies.clear();
        
        // Read how many entries to expect.
        int entries = WritableUtils.readVInt(dataInput);
        
        // Read each entry.
        for (int i = 0; i < entries; i++) {
            // Read the date key.
            String date = WritableUtils.readString(dataInput);
            // Read the frequency value.
            Frequency value = new Frequency();
            value.readFields(dataInput);
            this.dateToFrequencies.put(date, value);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateFrequencyMap that = (DateFrequencyMap) o;
        return Objects.equals(dateToFrequencies, that.dateToFrequencies);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(dateToFrequencies);
    }
    
    @Override
    public String toString() {
        return dateToFrequencies.toString();
    }
}
