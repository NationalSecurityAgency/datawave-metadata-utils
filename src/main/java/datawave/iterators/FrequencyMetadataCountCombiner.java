package datawave.iterators;

import datawave.query.model.DateFrequencyMap;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class FrequencyMetadataCountCombiner extends Combiner {
    
    private static final Logger log = Logger.getLogger(FrequencyMetadataCountCombiner.class);
    
    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        if (MetadataHelper.isAggregatedFrequencyKey(key)) {
            return reduceAggregatedValues(iter);
        } else {
            return reduceNonAggregatedValues(iter);
        }
    }
    
    private Value reduceAggregatedValues(Iterator<Value> iter) {
        Value first = iter.next();
        if (!iter.hasNext()) {
            return first;
        } else {
            try {
                DateFrequencyMap combined = new DateFrequencyMap(first.get());
                while (iter.hasNext()) {
                    // TODO - Possibly more efficient ways to handle combining the maps.
                    DateFrequencyMap map = new DateFrequencyMap(iter.next().get());
                    combined.incrementAll(map);
                }
                return new Value(WritableUtils.toByteArray(combined));
            } catch (IOException e) {
                log.trace("Failed to parse date-frequency map from value");
                // TODO - Evaluate in more depth how best to handle this.
                return first;
            }
        }
    }
    
    private Value reduceNonAggregatedValues(Iterator<Value> iter) {
        long sum = 0L;
        while (iter.hasNext()) {
            Long next = LongCombiner.VAR_LEN_ENCODER.decode(iter.next().get());
            sum = LongCombiner.safeAdd(sum, next);
        }
        return new Value(LongCombiner.VAR_LEN_ENCODER.encode(sum));
    }
}
