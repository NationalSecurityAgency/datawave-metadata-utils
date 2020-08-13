package datawave.query.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class OrdinalDayOfYear {
    private String mmDD;
    private int ordinalDay;
    private int year;
    private boolean isLeapYear = false;
    private GregorianCalendar gregorianCalendar;
    
    public static final int[] MONTH_LENGTH = new int[] {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    public static final int[] LEAP_MONTH_LENGTH = new int[] {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    
    public static final String[] MONTHSTRINGS = new String[] {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
    
    private static final Logger log = LoggerFactory.getLogger(OrdinalDayOfYear.class);
    
    /*
     * Used during deserializaton in DateFrequencyValue
     */
    public OrdinalDayOfYear(int ordinal, int theyear) {
        ordinalDay = ordinal;
        year = theyear;
        gregorianCalendar = new GregorianCalendar(theyear, 1, 1);
        gregorianCalendar.set(GregorianCalendar.DAY_OF_YEAR, ordinal);
        Date date = gregorianCalendar.getGregorianChange();
        String strDate = date.toString();
        isLeapYear = gregorianCalendar.isLeapYear(theyear);
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
