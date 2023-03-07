package datawave.query.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeMetadataTest {
    
    private TypeMetadata typeMetadata;
    
    @Before
    public void setup() {
        typeMetadata = new TypeMetadata();
        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");
        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");
        typeMetadata.put("field2", "ingest3", "NumberType");
        typeMetadata.put("field3", "ingest3", "LcType");
    }
    
    @Test
    public void testGetNormalizers() {
        Set<String> norms1 = typeMetadata.getNormalizerNamesForField("field2");
        assertEquals(3, norms1.size());
        assertTrue(norms1.contains("[IntegerType]"));
        assertTrue(norms1.contains("[LcType]"));
        assertTrue(norms1.contains("[NumberType]"));
        
        Set<String> norms2 = typeMetadata.getNormalizerNamesForField("field42");
        assertEquals(1, norms2.size());
        assertTrue(norms2.contains("[]"));
        
        // empty request should return empty set
        Set<String> norms3 = typeMetadata.getNormalizerNamesForField("");
        assertEquals(0, norms3.size());
        
        // null request should return empty set - in theory, calling this makes no sense
        Set<String> norms4 = typeMetadata.getNormalizerNamesForField();
        assertEquals(0, norms4.size());
    }
    
    @Test
    public void testGetDataTypes() {
        Set<String> types1 = typeMetadata.getDataTypesForField("field1");
        assertEquals(2, types1.size());
        assertTrue(types1.contains("ingest1"));
        assertTrue(types1.contains("ingest2"));
        
        Set<String> types2 = typeMetadata.getDataTypesForField("field3");
        assertEquals(1, types2.size());
        assertTrue(types2.contains("ingest3"));
        
        // empty request should return empty set
        Set<String> types3 = typeMetadata.getDataTypesForField("");
        assertEquals(0, types3.size());
        
        // null request should return empty set - in theory, calling this makes no sense
        Set<String> types4 = typeMetadata.getDataTypesForField();
        assertEquals(0, types4.size());
    }
    
}
