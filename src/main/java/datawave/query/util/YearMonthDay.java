package datawave.query.util;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

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
    
    public boolean withinBounds(String start, boolean startInclusive, String end, boolean endInclusive) {
        return ((start == null || (startInclusive ? yyyymmdd.compareTo(start) >= 0 : yyyymmdd.compareTo(start) > 0))
                        && (end == null || (endInclusive ? yyyymmdd.compareTo(end) <= 0 : yyyymmdd.compareTo(end) < 0)));
    }
    
    @Override
    public String toString() {
        return "String date: " + yyyymmdd + " ordinal day: " + julian;
    }
    
    @Override
    public int compareTo(YearMonthDay yearMonthDay) {
        return new CompareToBuilder().append(year, yearMonthDay.year).append(julian, yearMonthDay.julian).toComparison();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(year).append(julian).toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof YearMonthDay) {
            YearMonthDay yearMonthDay = (YearMonthDay) o;
            return new EqualsBuilder().append(year, yearMonthDay.year).append(julian, yearMonthDay.julian).isEquals();
        }
        return false;
    }
    
}
