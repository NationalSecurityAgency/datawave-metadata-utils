package datawave.iterators;

import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import java.io.IOException;

public class FrequencyMetadataTransformingIterator extends WrappingIterator {
    
    private Key topKey;
    private Value topValue;
    
    public FrequencyMetadataTransformingIterator(FrequencyMetadataTransformingIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new FrequencyMetadataTransformingIterator(this, env);
    }
    
    @Override
    public void next() throws IOException {
        SortedKeyValueIterator<Key,Value> source = getSource();
        if (source.hasTop()) {
            Key key = source.getTopKey();
            if (MetadataHelper.isAggregatedFrequencyKey(key)) {
                this.topKey = key;
                this.topValue = source.getTopValue();
            } else {
            
            }
        } else {
            this.topKey = null;
            this.topValue = null;
        }
    }
    
    @Override
    public Key getTopKey() {
        return this.topKey;
    }
    
    @Override
    public Value getTopValue() {
        return this.topValue;
    }
    
    @Override
    public boolean hasTop() {
        return this.topKey != null;
    }
}
