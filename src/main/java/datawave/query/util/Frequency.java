package datawave.query.util;

public class Frequency {
    int value;
    
    public Frequency(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    }
    
    void addFrequency(int value) {
        this.value += value;
    }
    
    @Override
    public String toString() {
        return "Frequency{" + "value=" + value + '}';
    }
    
    @Override
    public int hashCode() {
        return value;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Frequency) {
            if (((Frequency) obj).value == value)
                return true;
        }
        return false;
    }
}
