package datawave.query.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class OrdinalDayOfYear {
    private String mmDD;
    private boolean isInLeapYear;
    private int ordinalDay;
    public static String[] LEAP_YEARS = new String[] {"2020", "2024", "2028", "2032", "2036", "2040", "2044", "2048"};
    
    private static final Logger log = LoggerFactory.getLogger(OrdinalDayOfYear.class);
    
    /*
     * Used during serialization n DateFrequencyValue
     */
    public OrdinalDayOfYear(String monthDay, String year) {
        mmDD = monthDay;
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
        Integer ordinal = 1;
        if (month.equals("01")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 31; // Add all the days in January whether or not leap year. The mmDD is Feb or later.
        
        if (month.equals("02")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        ordinal += 29; // Add all the days in non leap Feb
        
        if (month.equals("03")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 31;
        
        if (month.equals("04")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 30;
        
        if (month.equals("05")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 31;
        
        if (month.equals("06")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 30;
        
        if (month.equals("07")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 31;
        
        if (month.equals("08")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 30;
        
        if (month.equals("09")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 30;
        
        if (month.equals("10")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 31;
        
        if (month.equals("11")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        ordinal += 30;
        
        if (month.equals("12")) {
            ordinal += dayOfMonthToInteger(day);
            return ordinal;
        }
        
        log.error("The ordinal from the MMDD string could not be calculated for " + mmDD);
        
        return DateFrequencyValue.DAYS_IN_LEAP_YEAR + 1;
    }
    
    private Integer dayOfMonthToInteger(String day) {
        Integer ordinal;
        if (day.substring(0, 1).equals("0")) {
            String daySubstring = day.substring(1);
            ordinal = Integer.parseInt(daySubstring);
        } else
            ordinal = Integer.parseInt(day);
        return ordinal;
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
        
        if (ordinal - 31 <= 0) // You are in January
        {
            String month = "01";
            buildMMDD(ordinal, month);
            return mmDD;
            
        }
        remainingPossibleOrdinals -= 31;
        
        if (!isInLeapYear) {
            if (remainingPossibleOrdinals - 28 <= 0) // Ordinal is in February
            {
                String month = "02";
                buildMMDD(remainingPossibleOrdinals, month);
                return mmDD;
                
            }
        } else {
            if (remainingPossibleOrdinals - 29 <= 0) // Ordinal is in February
            {
                String month = "02";
                buildMMDD(remainingPossibleOrdinals, month);
                return mmDD;
            }
        }
        
        remainingPossibleOrdinals -= 29;
        
        if (remainingPossibleOrdinals - 31 <= 0) // You are in March
        {
            String month = "03";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
            
        }
        remainingPossibleOrdinals -= 31;
        
        if (remainingPossibleOrdinals - 30 <= 0) // You are in April
        {
            String month = "04";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
            
        }
        remainingPossibleOrdinals -= 30;
        
        if (remainingPossibleOrdinals - 31 <= 0) // You are in May
        {
            String month = "05";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
            
        }
        
        remainingPossibleOrdinals -= 31;
        
        if (remainingPossibleOrdinals - 30 <= 0) // You are in June
        {
            String month = "06";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
        }
        
        remainingPossibleOrdinals -= 30;
        
        if (remainingPossibleOrdinals - 31 <= 0) // You are in July
        {
            String month = "07";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
        }
        
        remainingPossibleOrdinals -= 31;
        
        if (remainingPossibleOrdinals - 30 <= 0) // You are in August
        {
            String month = "08";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
        }
        
        remainingPossibleOrdinals -= 30;
        
        if (remainingPossibleOrdinals - 30 <= 0) // You are in Sept
        {
            String month = "09";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
        }
        
        remainingPossibleOrdinals -= 30;
        
        if (remainingPossibleOrdinals - 31 <= 0) // You are in Oct
        {
            String month = "10";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
        }
        
        remainingPossibleOrdinals -= 31;
        
        if (remainingPossibleOrdinals - 30 <= 0) // You are in Nov
        {
            String month = "11";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
        }
        
        remainingPossibleOrdinals -= 30;
        
        if (remainingPossibleOrdinals - 31 <= 0) // You are in Dec
        {
            String month = "12";
            buildMMDD(remainingPossibleOrdinals, month);
            return mmDD;
        }
        
        remainingPossibleOrdinals -= 31;
        
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
    }
    
    public int getOrdinalDay() {
        
        return ordinalDay;
    }
    
    private static boolean isLeapYear(String yyyy) {
        return Arrays.asList(LEAP_YEARS).contains(yyyy);
    }
    
}
