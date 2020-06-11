package datawave.query.util;

public class DateFrequencyValue {
    
    Base127 value = null;
    
    public DateFrequencyValue() {
        
    }
    
    public DateFrequencyValue(byte[] content) {
        
    }
    
    private class Base127 {
        private byte[] content;
        
        public Base127(byte[] content) {
            this.content = content;
        }
        
    }
    
}
