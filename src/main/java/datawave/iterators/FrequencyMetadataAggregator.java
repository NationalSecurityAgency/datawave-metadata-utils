package datawave.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import datawave.marking.MarkingFunctions;
import datawave.query.model.DateFrequencyMap;

/**
 * Aggregates entries in the metadata table for the "f", "i", and "ri" columns. When initially ingested, entries for these columns have a column qualifier with
 * the format {@code <datatype>\0<yyyyMMdd>}, and a value containing a possibly partial frequency count for the date in the column qualifier. Entries with the
 * same row, column family, datatype, and column family will be aggregated into a single entry where the column qualifier consists of the datatype and the value
 * consists of an encoded {@link DateFrequencyMap} with the dates and counts seen. Additionally, this aggregator will handle the case where we have a previously
 * aggregated entry and freshly ingested rows that need to be aggregated together. <br>
 * <br>
 * This iterator supports the following options:
 * <ul>
 * <li>{@value COMBINE_VISIBILITIES}: Defaults to false. If true, entries will be aggregated by row, column family, and datatype only, and the column visibility
 * will be a combination of all column visibilities seen for the row/column family/datatype combo. This option is meant to be used when scanning only, and not
 * for compaction.</li>
 * </ul>
 */
public class FrequencyMetadataAggregator extends WrappingIterator implements OptionDescriber {
    
    public static final String COMBINE_VISIBILITIES = "COMBINE_VISIBILITIES";
    
    private static final Logger log = Logger.getLogger(FrequencyMetadataAggregator.class);
    private static final String NULL_BYTE = "\0";
    private static final MarkingFunctions markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
    
    private SortedKeyValueIterator<Key,Value> source;
    private boolean combineVisibilities;
    private Key topKey;
    private Value topValue;
    
    private final TreeMap<Key,Value> cache;
    private final Map<ColumnVisibility,DateFrequencyMap> visibilityToDateFrequencies;
    private final Map<ColumnVisibility,Long> visibilityToMaxTimestamp;
    
    private Text currentRow;
    private Text currentColumnFamily;
    private String currentDatatype;
    private String currentDate;
    private ColumnVisibility currentVisibility;
    private long currentTimestamp;
    private boolean isCurrentAggregated;
    
    public FrequencyMetadataAggregator() {
        cache = new TreeMap<>();
        visibilityToDateFrequencies = new HashMap<>();
        visibilityToMaxTimestamp = new HashMap<>();
    }
    
    public FrequencyMetadataAggregator(FrequencyMetadataAggregator other, IteratorEnvironment env) {
        this();
        source = other.getSource().deepCopy(env);
        combineVisibilities = other.combineVisibilities;
        cache.putAll(other.cache);
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new FrequencyMetadataAggregator(this, env);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(COMBINE_VISIBILITIES, "Boolean value denoting whether to combine entries with different visibilities. Defaults to false.");
        
        return new IteratorOptions(getClass().getSimpleName(), "An iterator used to collapse frequency columns in the metadata table", options, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        // Check if entries with different column visibilities should be combined.
        if (options.containsKey(COMBINE_VISIBILITIES)) {
            combineVisibilities = Boolean.parseBoolean(options.get(COMBINE_VISIBILITIES));
            if (log.isTraceEnabled()) {
                log.trace("combine visibilities: " + combineVisibilities);
            }
        }
        
        return true;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options)) {
            throw new IllegalArgumentException("Invalid options given: " + options);
        }
        
        this.source = source;
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        log.trace("seeking");
        
        source.seek(range, columnFamilies, inclusive);
        
        // Establish the first top key.
        next();
        
        if (log.isTraceEnabled()) {
            log.trace("first top key after seek: " + topKey);
        }
    }
    
    @Override
    public Key getTopKey() {
        return topKey;
    }
    
    @Override
    public Value getTopValue() {
        return topValue;
    }
    
    @Override
    public boolean hasTop() {
        return topKey != null;
    }
    
    @Override
    public void next() throws IOException {
        log.trace("Fetching next");
        if (!popCache()) {
            log.trace("No entries in cache");
            if (source.hasTop()) {
                log.trace("Source has top, updating cache");
                updateCache();
            } else {
                log.trace("Source does not have top");
            }
            popCache();
        }
    }
    
    /**
     * Set {@link #topKey} and {@link #topValue} to the next available entry in the cache. Returns true if the cache was not empty, or false otherwise.
     */
    private boolean popCache() {
        topKey = null;
        topValue = null;
        
        if (!cache.isEmpty()) {
            Map.Entry<Key,Value> entry = cache.pollFirstEntry();
            topKey = entry.getKey();
            topValue = entry.getValue();
            return true;
        }
        return false;
    }
    
    /**
     * Reset all current tracking variables.
     */
    private void resetCurrent() {
        currentRow = null;
        currentColumnFamily = null;
        currentDatatype = null;
        currentDate = null;
        currentVisibility = null;
        currentTimestamp = 0L;
        isCurrentAggregated = false;
        visibilityToDateFrequencies.clear();
        visibilityToMaxTimestamp.clear();
    }
    
    /**
     * Iterate over the source entries, aggregate all entries for the next row/column family/datatype combination, and add them to the cache.
     */
    private void updateCache() throws IOException {
        log.trace("Updating cache");
        
        resetCurrent();
        
        while (true) {
            // If the source does not have any more entries, wrap up the last batch of entries.
            if (!source.hasTop()) {
                log.trace("Source does not have top");
                wrapUpCurrent();
                return;
            }
            
            Key key = source.getTopKey();
            if (log.isTraceEnabled()) {
                log.trace("updateCache examining key " + key);
            }
            
            // If the current entry has a different row, column family, or datatype from the previous entry, wrap up and return the current
            // batch of entries.
            if (differsFromPrev(key)) {
                wrapUpCurrent();
                return;
            }
            
            // Aggregate the current entry.
            aggregateCurrent();
            
            // Advance to the next entry from the source.
            source.next();
        }
    }
    
    /**
     * Return true if the current entry is not the first entry seen in the current call to {@link #updateCache()} and has a different row, column family, or
     * datatype from the previous entry, or false otherwise.
     */
    private boolean differsFromPrev(Key key) {
        // Update the current row if null.
        if (currentRow == null) {
            currentRow = key.getRow();
            if (log.isTraceEnabled()) {
                log.trace("Set current row to " + currentRow);
            }
            // Check if we're on a new field.
        } else if (!currentRow.equals(key.getRow())) {
            if (log.isTraceEnabled()) {
                log.trace("Next row " + key.getRow() + " differs from prev " + currentRow);
            }
            return true;
        }
        
        // Update the current column family if null.
        if (currentColumnFamily == null) {
            currentColumnFamily = key.getColumnFamily();
            if (log.isTraceEnabled()) {
                log.trace("Set current column family to " + currentColumnFamily);
            }
            // Check if we're on a new column family.
        } else if (!currentColumnFamily.equals(key.getColumnFamily())) {
            if (log.isTraceEnabled()) {
                log.trace("Next column family " + key.getColumnFamily() + " differs from prev " + currentColumnFamily);
            }
            return true;
        }
        
        String columnQualifier = key.getColumnQualifier().toString();
        int separatorPos = columnQualifier.indexOf(NULL_BYTE);
        // If a null byte was not found, the column qualifier has the format <datatype> and is from a previously aggregated entry. Otherwise, the column
        // qualifier has the format <datatype>\0<yyyyMMdd> and is from a non-aggregated entry.
        isCurrentAggregated = separatorPos == -1;
        String datatype;
        if (isCurrentAggregated) {
            datatype = columnQualifier;
        } else {
            datatype = columnQualifier.substring(0, separatorPos);
            currentDate = columnQualifier.substring((separatorPos + 1));
            if (log.isTraceEnabled()) {
                log.trace("Set current date to " + currentDate);
            }
        }
        
        // Update the current datatype if null.
        if (currentDatatype == null) {
            currentDatatype = datatype;
            if (log.isTraceEnabled()) {
                log.trace("Set current datatype to " + currentDatatype);
            }
            // Check if we're on a new datatype.
        } else if (!currentDatatype.equals(datatype)) {
            if (log.isTraceEnabled()) {
                log.trace("Next datatype " + datatype + " differs from prev " + currentDatatype);
            }
            return true;
        }
        
        // Update the current visibility and timestamp.
        currentVisibility = new ColumnVisibility(key.getColumnVisibility());
        currentTimestamp = key.getTimestamp();
        return false;
    }
    
    /**
     * Aggregate the current entry.
     */
    private void aggregateCurrent() {
        Value value = source.getTopValue();
        // Fetch the date-frequency map for the current column visibility, creating one if not present.
        DateFrequencyMap dateFrequencies = visibilityToDateFrequencies.computeIfAbsent(currentVisibility, (k) -> new DateFrequencyMap());
        
        // If the current entry has an aggregated value, parse it as such and merge it with the date-frequency map.
        if (isCurrentAggregated) {
            try {
                DateFrequencyMap entryMap = new DateFrequencyMap(value.get());
                dateFrequencies.incrementAll(entryMap);
            } catch (IOException e) {
                Key key = source.getTopKey();
                log.error("Failed to parse date frequency map from value for key " + key, e);
                throw new IllegalArgumentException("Failed to parse date frequency map from value for key " + key, e);
            }
        } else {
            // If the current entry does not have an aggregated value, it has a count for a specific date. Increment the count for the date in the map.
            long count = LongCombiner.VAR_LEN_ENCODER.decode(value.get());
            dateFrequencies.increment(currentDate, count);
        }
        
        // If the current timestamp is later than the previously tracked timestamp for the current column visibility, update the tracked timestamp.
        if (visibilityToMaxTimestamp.containsKey(currentVisibility)) {
            long prevTimestamp = visibilityToMaxTimestamp.get(currentVisibility);
            if (prevTimestamp < currentTimestamp) {
                visibilityToMaxTimestamp.put(currentVisibility, currentTimestamp);
            }
        } else {
            visibilityToMaxTimestamp.put(currentVisibility, currentTimestamp);
        }
    }
    
    /**
     * Create the entries to be returned by {@link #next()} and add them to the cache.
     */
    private void wrapUpCurrent() {
        if (log.isTraceEnabled()) {
            log.trace("Wrapping up for row: " + currentRow + ", cf: " + currentColumnFamily + ", cq: " + currentDatatype);
        }
        
        cache.putAll(buildTopEntries());
        resetCurrent();
    }
    
    /**
     * Build and return a sorted map of the key-value entries that should be made available to be returned by {@link #next()}.
     */
    private Map<Key,Value> buildTopEntries() {
        if (log.isTraceEnabled()) {
            log.trace("buildTopKeys, currentRow: " + currentRow);
            log.trace("buildTopKeys, currentColumnFamily: " + currentColumnFamily);
            log.trace("buildTopKeys, currentDatatype: " + currentDatatype);
        }
        
        Text columnQualifier = new Text(currentDatatype);
        
        // If we are combining all entries regardless of column visibility, we will end up with one entry to return.
        if (combineVisibilities) {
            // Combine the visibilities and frequencies, and find the latest timestamp.
            ColumnVisibility combined = combineAllVisibilities();
            long latestTimestamp = getLatestTimestamp();
            DateFrequencyMap combinedFrequencies = combineAllDateFrequencies();
            
            // Return the single key-value pair.
            Key key = new Key(currentRow, currentColumnFamily, columnQualifier, combined, latestTimestamp);
            Value value = new Value(WritableUtils.toByteArray(combinedFrequencies));
            return Collections.singletonMap(key, value);
        } else {
            Map<Key,Value> entries = new HashMap<>();
            // Create a key-value pair for each distinct column visibility.
            for (Map.Entry<ColumnVisibility,DateFrequencyMap> entry : visibilityToDateFrequencies.entrySet()) {
                ColumnVisibility visibility = entry.getKey();
                long timestamp = visibilityToMaxTimestamp.get(visibility);
                Key key = new Key(currentRow, currentColumnFamily, columnQualifier, visibility, timestamp);
                Value value = new Value(WritableUtils.toByteArray(entry.getValue()));
                entries.put(key, value);
            }
            return entries;
        }
    }
    
    /**
     * Return a {@link ColumnVisibility} that is the combination of all visibilities present in {@link #visibilityToDateFrequencies}.
     */
    private ColumnVisibility combineAllVisibilities() {
        Set<ColumnVisibility> visibilities = visibilityToDateFrequencies.keySet();
        try {
            return markingFunctions.combine(visibilities);
        } catch (MarkingFunctions.Exception e) {
            log.error("Failed to combine visibilities " + visibilities);
            throw new IllegalArgumentException("Failed to combine visibilities " + visibilities, e);
        }
    }
    
    /**
     * Return the latest timestamp present in {@link #visibilityToMaxTimestamp}.
     */
    private long getLatestTimestamp() {
        long max = 0L;
        for (long timestamp : visibilityToMaxTimestamp.values()) {
            max = Math.max(max, timestamp);
        }
        return max;
    }
    
    /**
     * Return a {@link DateFrequencyMap} that contains all date counts present in {@link #visibilityToDateFrequencies}.
     */
    private DateFrequencyMap combineAllDateFrequencies() {
        DateFrequencyMap combined = new DateFrequencyMap();
        for (DateFrequencyMap map : visibilityToDateFrequencies.values()) {
            combined.incrementAll(map);
        }
        return combined;
        
    }
}
