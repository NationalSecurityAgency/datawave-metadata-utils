package datawave.query.util;

import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeMetadataTest {
    
    @Test
    public void testGetDataTypes() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");
        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");
        typeMetadata.put("field2", "ingest3", "NumberType");
        typeMetadata.put("field3", "ingest3", "LcType");
        
        Set<String> types1 = typeMetadata.getDataTypesForField("field2");
        assertEquals(3, types1.size());
        assertTrue(types1.contains("[IntegerType]"));
        assertTrue(types1.contains("[LcType]"));
        assertTrue(types1.contains("[NumberType]"));
        
        Set<String> types2 = typeMetadata.getDataTypesForField("field42");
        assertEquals(1, types2.size());
        assertTrue(types2.contains("[]"));
        
        Set<String> types3 = typeMetadata.getDataTypesForField("");
        assertEquals(0, types3.size());
        
        Set<String> types4 = typeMetadata.getDataTypesForField();
        assertEquals(0, types4.size());
    }
    
}
