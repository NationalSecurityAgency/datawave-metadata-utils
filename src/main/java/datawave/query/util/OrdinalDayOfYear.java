package datawave.query.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class OrdinalDayOfYear {
    private String mmDD;
    private int ordinalDay;
    private int year;
    private GregorianCalendar gregorianCalendar;
    
    /*
     * Used during deserializaton in DateFrequencyValue
     */
    public OrdinalDayOfYear(int ordinal, int theyear) {
        ordinalDay = ordinal;
        year = theyear;
        gregorianCalendar = new GregorianCalendar(theyear, 1, 1);
        gregorianCalendar.set(GregorianCalendar.DAY_OF_YEAR, ordinal);
        mmDD = calculateMMDD(ordinalDay);
        
    }
    
    public String getMmDD() {
        return mmDD;
    }
    
    private String calculateMMDD(int ordinal) {
        SimpleDateFormat formatter = new SimpleDateFormat("MMdd");
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.DAY_OF_YEAR, ordinal);
        String result = formatter.format(c.getTime());
        return result;
    }
    
}
