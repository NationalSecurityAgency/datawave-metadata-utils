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
    
    public boolean equals(Frequency obj) {
        if (obj.value == value)
            return true;
        else
            return false;
    }
}
