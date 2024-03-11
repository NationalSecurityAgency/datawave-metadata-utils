package datawave.query.model;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Frequency implements WritableComparable<Frequency> {
    
    private long value;
    
    public Frequency() {
        this(0L);
    }
    
    public Frequency(long value) {
        this.value = value;
    }
    
    public long getValue() {
        return value;
    }
    
    public void incrementBy(long increment) {
        this.value += increment;
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVLong(dataOutput, value);
    }
    
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value = WritableUtils.readVLong(dataInput);
    }
    
    @Override
    public int compareTo(Frequency o) {
        return Long.compare(this.value, o.value);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Frequency frequency = (Frequency) o;
        return value == frequency.value;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
    
    @Override
    public String toString() {
        return Long.toString(value);
    }
}
