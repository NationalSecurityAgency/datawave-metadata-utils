package datawave.query.model;

import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableSortedSet;

/**
 * This class represents a set of calculated field index holes for a given fieldName and datatype. A field index hole is effectively a date where a frequency
 * row was seen, but an index and/or reversed indexed row was not.
 */
public class IndexFieldHole {

    private static ThreadLocal<Calendar> calendar = ThreadLocal.withInitial(() -> Calendar.getInstance());
    private final String fieldName;
    private final String datatype;
    private final SortedSet<Pair<Date,Date>> dateRanges;
    
    public IndexFieldHole(String fieldName, String dataType, Collection<Pair<Date,Date>> holes) {
        this.fieldName = fieldName;
        this.datatype = dataType;
        // Ensure the date range set is immutable.
        ImmutableSortedSet.Builder<Pair<Date,Date>> builder = new ImmutableSortedSet.Builder<>(Comparator.naturalOrder());
        holes.forEach(p -> builder.add(new ImmutablePair<>(floor(p.getLeft()), ceil(p.getRight()))));
        dateRanges = builder.build();
    }

    /**
     * return the date at 00:00:00
     * @param d
     * @return d with time reset to 00:00:00
     */
    private static Date floor(Date d) {
        Calendar begin = calendar.get();
        begin.setTime(d);
        begin.set(Calendar.HOUR, 0);
        begin.set(Calendar.MINUTE, 0);
        begin.set(Calendar.SECOND, 0);
        begin.set(Calendar.MILLISECOND, 0);
        return begin.getTime();
    }

    /**
     * return the date at 23:59:59
     * @param d
     * @return d with time reset to 23:59:59
     */
    private static Date ceil(Date d) {
        Calendar end = calendar.get();
        end.setTime(d);
        end.set(Calendar.HOUR, 23);
        end.set(Calendar.MINUTE, 59);
        end.set(Calendar.SECOND, 59);
        end.set(Calendar.MILLISECOND, 999);
        return end.getTime();
    }
    
    /**
     * Return the field name.
     * 
     * @return the field name.
     */
    public String getFieldName() {
        return fieldName;
    }
    
    /**
     * Return the datatype.
     * 
     * @return the datatype.
     */
    public String getDatatype() {
        return datatype;
    }
    
    /**
     * Returns the set of date ranges that span over field index holes for the fieldName and datatype of this {@link IndexFieldHole}. Each date range represents
     * a span of consecutive days for which a frequency row exist, but an index row does not. All date ranges are start(inclusive)-end(inclusive).
     * 
     * @return the date ranges
     */
    public SortedSet<Pair<Date,Date>> getDateRanges() {
        return dateRanges;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexFieldHole indexHole = (IndexFieldHole) o;
        return Objects.equals(fieldName, indexHole.fieldName) && Objects.equals(datatype, indexHole.datatype)
                        && Objects.equals(dateRanges, indexHole.dateRanges);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fieldName, datatype, dateRanges);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", IndexFieldHole.class.getSimpleName() + "[", "]").add("fieldName='" + fieldName + "'").add("dataType='" + datatype + "'")
                        .add("dateRanges=" + dateRanges).toString();
    }
}
