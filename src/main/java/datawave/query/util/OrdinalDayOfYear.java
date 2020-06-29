package datawave.query.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class OrdinalDayOfYear {
    private String mmDD;
    private boolean isInLeapYear;
    private int ordinalDay;
    private String year;
    public static final String[] LEAP_YEARS = new String[] {"2020", "2024", "2028", "2032", "2036", "2040", "2044", "2048"};
    
    enum DAYSINMONTH {
        JAN(31), FEB(29), MAR(31), APR(30), MAY(31), JUN(30), JUL(31), AUG(31), SEP(30), OCT(31), NOV(30), DEC(31);
        DAYSINMONTH(int i) {
            numdays = i;
        }
        
        private int numdays = 0;
        
        private int getNumdays() {
            return numdays;
        }
    }
    
    public static final String[] MONTHSTRINGS = new String[] {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
    
    private static final Logger log = LoggerFactory.getLogger(OrdinalDayOfYear.class);
    
    /*
     * Used during serialization n DateFrequencyValue
     */
    public OrdinalDayOfYear(String monthDay, String theYear) {
        mmDD = monthDay;
        year = theYear;
        isInLeapYear = isLeapYear(year);
        ordinalDay = calculateOrdinal();
    }
    
    /*
     * Used during deserializaton in DateFrequencyValue
     */
    public OrdinalDayOfYear(int ordinal, int year) {
        isInLeapYear = isLeapYear(String.valueOf(year));
        ordinalDay = ordinal;
        mmDD = calculateMMDD(ordinalDay);
        
    }
    
    private int calculateOrdinal() {
        String month = mmDD.substring(0, 2);
        String day = mmDD.substring(2);
        Integer ordinal = 0;
        
        int daysInMonthIndex = 0;
        try {
            while (daysInMonthIndex < 12) {
                
                for (String checkMonth : MONTHSTRINGS) {
                    
                    if (month.equals(checkMonth)) {
                        ordinal += dayOfMonthToInteger(day);
                        return ordinal;
                    }
                    
                    ordinal += DAYSINMONTH.values()[daysInMonthIndex].numdays;
                    daysInMonthIndex++;
                }
                
            }
        } catch (ArrayIndexOutOfBoundsException arrayIndexOutOfBoundsException) {
            log.error("Could not create the ordinal right", arrayIndexOutOfBoundsException);
        }
        
        log.error("The ordinal from the MMDD string could not be calculated for " + mmDD);
        
        return DateFrequencyValue.DAYS_IN_LEAP_YEAR + 1;
    }
    
    private Integer dayOfMonthToInteger(String day) {
        Integer ordinalInsideMonth;
        if (day.substring(0, 1).equals("0")) {
            String daySubstring = day.substring(1);
            ordinalInsideMonth = Integer.parseInt(daySubstring);
        } else
            ordinalInsideMonth = Integer.parseInt(day);
        return ordinalInsideMonth;
    }
    
    public String getMmDD() {
        return mmDD;
    }
    
    public boolean isInLeapYear() {
        return isInLeapYear;
    }
    
    private String calculateMMDD(int ordinal) {
        if (ordinal > 366)
            log.error("The ordinal is out of range" + ordinal);
        
        int remainingPossibleOrdinals = ordinal;
        int daysInMonthIndex = 0;
        
        while (daysInMonthIndex < 12) {
            for (String checkMonth : MONTHSTRINGS) {
                
                if (remainingPossibleOrdinals - DAYSINMONTH.values()[daysInMonthIndex].numdays <= 0) {
                    buildMMDD(remainingPossibleOrdinals, checkMonth);
                    return mmDD;
                }
                
                remainingPossibleOrdinals -= DAYSINMONTH.values()[daysInMonthIndex].numdays;
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
    
    public int getOrdinalDay() {
        
        return ordinalDay;
    }
    
    public static boolean isLeapYear(String yyyy) {
        return Arrays.asList(LEAP_YEARS).contains(yyyy);
    }
    
}
