package datawave.query.model;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class ModelKeyParser {
    
    public static final String NULL_BYTE = "\0";
    public static final Value NULL_VALUE = new Value(new byte[0]);
    
    @Deprecated
    public static final String INDEX_ONLY = "index_only";
    
    private static Logger log = Logger.getLogger(ModelKeyParser.class);
    
    public static FieldMapping parseKey(Key key) {
        String row = key.getRow().toString();
        String[] colf = key.getColumnFamily().toString().split(NULL_BYTE);
        String[] colq = key.getColumnQualifier().toString().split(NULL_BYTE);
        String cv = key.getColumnVisibility().toString();
        
        String datatype = null;
        Direction direction;
        String dataField;
        String modelField;
        
        if (colf.length == 1) {
            // Older style, no datatype
        } else if (colf.length == 2) {
            datatype = colf[1];
        } else {
            throw new IllegalArgumentException("Key in unknown format, colf parts: " + colf.length);
        }
        if (2 == colq.length) {
            direction = Direction.getDirection(colq[1]);
            if (Direction.REVERSE.equals(direction)) {
                dataField = row;
                modelField = colq[0];
            } else {
                dataField = colq[0];
                modelField = row;
            }
        } else if (3 == colq.length && Direction.FORWARD == Direction.getDirection(colq[2])) {
            dataField = colq[0];
            modelField = row;
            direction = Direction.FORWARD; // we already checked it in the if condition
        } else {
            log.error("Error parsing key: " + key);
            throw new IllegalArgumentException("Key in unknown format, colq parts: " + colq.length);
        }
        
        return new FieldMapping(datatype, dataField, modelField, direction, cv);
    }
    
    public static Key createKey(FieldMapping mapping, String modelName) {
        ColumnVisibility cv = new ColumnVisibility(mapping.getColumnVisibility());
        
        String inName = Direction.REVERSE.equals(mapping.getDirection()) ? mapping.getFieldName() : mapping.getModelFieldName();
        String outName = Direction.REVERSE.equals(mapping.getDirection()) ? mapping.getModelFieldName() : mapping.getFieldName();
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        return new Key(inName, // Row
                        modelName + dataType, // ColFam
                        outName + NULL_BYTE + mapping.getDirection().getValue(), // ColQual
                        cv, // Visibility
                        System.currentTimeMillis() // Timestamp
        );
    }
    
    public static Mutation createMutation(FieldMapping mapping, String modelName) {
        ColumnVisibility cv = new ColumnVisibility(mapping.getColumnVisibility());
        Mutation m;
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        
        if (Direction.REVERSE.equals(mapping.getDirection())) {
            // Reverse mappings should not have indexOnly designators. If they do, scrub it off.
            m = new Mutation(mapping.getFieldName());
            m.put(modelName + dataType, mapping.getModelFieldName() + NULL_BYTE + mapping.getDirection().getValue(), cv, System.currentTimeMillis(),
                            NULL_VALUE);
            return m;
        } else {
            m = new Mutation(mapping.getModelFieldName());
            m.put(modelName + dataType, mapping.getFieldName() + NULL_BYTE + mapping.getDirection().getValue(), cv, System.currentTimeMillis(), NULL_VALUE);
            return m;
        }
    }
    
    public static Mutation createDeleteMutation(FieldMapping mapping, String modelName) {
        ColumnVisibility cv = new ColumnVisibility(mapping.getColumnVisibility());
        Mutation m;
        String dataType = StringUtils.isEmpty(mapping.getDatatype()) ? "" : NULL_BYTE + mapping.getDatatype().trim();
        
        if (Direction.REVERSE.equals(mapping.getDirection())) {
            m = new Mutation(mapping.getFieldName());
            m.putDelete(modelName + dataType, mapping.getModelFieldName() + NULL_BYTE + mapping.getDirection().getValue(), cv, System.currentTimeMillis());
            return m;
        } else {
            m = new Mutation(mapping.getModelFieldName());
            m.putDelete(modelName + dataType, mapping.getFieldName() + NULL_BYTE + mapping.getDirection().getValue(), cv, System.currentTimeMillis());
            m.putDelete(modelName + dataType, mapping.getFieldName() + NULL_BYTE + INDEX_ONLY + NULL_BYTE + mapping.getDirection().getValue(), cv,
                            System.currentTimeMillis());
            return m;
        }
    }
    
}
