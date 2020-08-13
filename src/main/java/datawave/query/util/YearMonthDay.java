package datawave.query.util;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class YearMonthDay implements Comparable<YearMonthDay> {
    int year;
    int julian;
    String yyyymmdd;
    
    public YearMonthDay(String value) {
        // parse value into year and ordinal
        yyyymmdd = value;
        year = Integer.parseInt(value.substring(0, 4));
        String month = value.substring(4, 6);
        int nmonth = month.charAt(0) == '0' ? Integer.parseInt(month.substring(1, 2)) : Integer.parseInt(month);
        String day = value.substring(6, 8);
        int nday = day.charAt(0) == '0' ? Integer.parseInt(day.substring(1, 2)) : Integer.parseInt(day);
        GregorianCalendar gc = new GregorianCalendar(year, nmonth - 1, nday);
        julian = gc.get(Calendar.DAY_OF_YEAR);
        
    }
    
    public int getYear() {
        return year;
    }
    
    public int getJulian() {
        return julian;
    }
    
    public String getYyyymmdd() {
        return yyyymmdd;
    }
    
    @Override
    public String toString() {
        return "String date: " + yyyymmdd + " ordinal day: " + julian;
    }
    
    @Override
    public int compareTo(YearMonthDay yearMonthDay) {
        if (year > yearMonthDay.year)
            return 1;
        if (year < yearMonthDay.year)
            return -1;
        if (julian > yearMonthDay.julian)
            return 1;
        if (julian < yearMonthDay.julian)
            return -1;
        
        return 0;
    }
    
}
