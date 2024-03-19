package datawave.query.model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;


public class DateFrequencyMap implements Writable {
    
    private final SortedMap<String,Frequency> dateToFrequencies;
    
    public DateFrequencyMap() {
        this.dateToFrequencies = new TreeMap<>();
    }
    
    public DateFrequencyMap(byte[] bytes) throws IOException {
        this();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        readFields(dataIn);
        dataIn.close();
    }
    
    /**
     * Associates the given frequency with the given date in this {@link DateFrequencyMap}. If the map previously contained a mapping for the given date, the
     * old frequency is replaced by the new frequency.
     * @param date the date
     * @param frequency the frequency
     */
    public void put(String date, long frequency) {
        put(date, new Frequency(frequency));
    }
    
    /**
     * Associates the given frequency with the given date in this {@link DateFrequencyMap}. If the map previously contained a mapping for the given date, the
     * old frequency is replaced by the new frequency.
     * @param date the date
     * @param frequency the frequency
     */
    public void put(String date, Frequency frequency) {
        dateToFrequencies.put(date, frequency);
    }
    
    /**
     * Increments the frequency associated with the given date by the given addend. If a mapping does not previously exist for the date, a new mapping will be
     * added with the given addend as the frequency.
     * @param date the date
     * @param addend the addend
     */
    public void increment(String date, long addend) {
        dateToFrequencies.computeIfAbsent(date, (k) -> new Frequency()).add(addend);
    }
    
    /**
     * Increment all frequencies in this {@link DateFrequencyMap} by the frequencies in the given map. If the given map contains mappings for dates not present
     * in this map, those mappings will be added to this map.
     * @param map the map
     */
    public void incrementAll(DateFrequencyMap map) {
        for (Map.Entry<String,Frequency> entry : map.dateToFrequencies.entrySet()) {
            increment(entry.getKey(), entry.getValue().getValue());
        }
    }
    
    /**
     * Return the frequency associated with the given date, or null if no such mapping exists.
     * @param date the date
     * @return the count
     */
    public Frequency get(String date) {
        return dateToFrequencies.get(date);
    }
    
    /**
     * Return whether this map contains a mapping for the given date.
     * @param date the date
     * @return true if a mapping exists for the given date, or false otherwise
     */
    public boolean contains(String date) {
        return dateToFrequencies.containsKey(date);
    }
    
    /**
     * Clear all mappings in this {@link DateFrequencyMap}.
     */
    public void clear() {
        this.dateToFrequencies.clear();
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
