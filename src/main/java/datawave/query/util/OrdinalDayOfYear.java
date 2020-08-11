package datawave.query.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        gregorianCalendar = new GregorianCalendar(theyear, 1, 1);
        gregorianCalendar.set(GregorianCalendar.DAY_OF_YEAR, ordinal);
        Date date = gregorianCalendar.getGregorianChange();
        String strDate = date.toString();
        isLeapYear = gregorianCalendar.isLeapYear(theyear);
        mmDD = calculateMMDD(ordinalDay);
        year = theyear;
    }
    
    public String getMmDD() {
        return mmDD;
    }
    
    private String calculateMMDD(int ordinal) {
        if (ordinal > 366)
            log.error("The ordinal is out of range" + ordinal);
        
        int remainingPossibleOrdinals = ordinal;
        int daysInMonthIndex = 0;
        
        int[] monthLegths;
        
        if (isLeapYear)
            monthLegths = LEAP_MONTH_LENGTH;
        else
            monthLegths = MONTH_LENGTH;
        
        while (daysInMonthIndex < 12) {
            for (String checkMonth : MONTHSTRINGS) {
                
                if (remainingPossibleOrdinals - monthLegths[daysInMonthIndex] <= 0) {
                    buildMMDD(remainingPossibleOrdinals, checkMonth);
                    return mmDD;
                }
                
                remainingPossibleOrdinals -= monthLegths[daysInMonthIndex];
                daysInMonthIndex++;
            }
            
        }
        
        if (remainingPossibleOrdinals < 0)
            log.error("counld not transform ordinal " + ordinal + " to yyyyMMdd format");
        mmDD = "1313";
        return mmDD;
    }
    
    private void buildMMDD(int ordinal, String month) {
        if (ordinal < 10)
            mmDD = month + "0" + ordinal;
        else
            mmDD = month + ordinal;
        
        if (ordinal > 31)
            log.error("Ordinal is over 31 - not correct :" + ordinal);
    }
    
}
