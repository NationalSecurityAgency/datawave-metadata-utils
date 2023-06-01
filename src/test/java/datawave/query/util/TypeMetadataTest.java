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
        typeMetadata.put("FIELD1", "datatypeA", "LcType");
        typeMetadata.put("FIELD1", "datatypeB", "DateType");
        typeMetadata.put("FIELD2", "datatypeA", "IntegerType");
        typeMetadata.put("FIELD2", "datatypeB", "LcType");
        typeMetadata.put("FIELD2", "datatypeC", "NumberType");
        typeMetadata.put("FIELD3", "datatypeC", "LcType");
    }
    
    @Test
    public void testGetNormalizers() {
        Set<String> norms1 = typeMetadata.getNormalizerNamesForField("FIELD2");
        assertEquals(3, norms1.size());
        assertTrue(norms1.contains("IntegerType"));
        assertTrue(norms1.contains("LcType"));
        assertTrue(norms1.contains("NumberType"));
        
        Set<String> norms2 = typeMetadata.getNormalizerNamesForField("field42");
        assertEquals(0, norms2.size());
        
        // empty request should return empty set
        Set<String> norms3 = typeMetadata.getNormalizerNamesForField("");
        assertEquals(0, norms3.size());
    }
    
    @Test
    public void testGetDataTypes() {
        Set<String> types1 = typeMetadata.getDataTypesForField("FIELD1");
        assertEquals(2, types1.size());
        assertTrue(types1.contains("datatypeA"));
        assertTrue(types1.contains("datatypeB"));
        
        Set<String> types2 = typeMetadata.getDataTypesForField("FIELD3");
        assertEquals(1, types2.size());
        assertTrue(types2.contains("datatypeC"));
        
        // empty request should return empty set
        Set<String> types3 = typeMetadata.getDataTypesForField("");
        assertEquals(0, types3.size());
    }
    
    @Test
    public void testCreateSerializedMap() {
        TypeMetadata typeMetadata = new TypeMetadata();

        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");

        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");

        String asString = typeMetadata.toString();
        
        TypeMetadata fromString = new TypeMetadata(asString);
        
        System.out.println(asString);
    }

    @Test
    public void testReadOldSerializedFormat() {
        TypeMetadata typeMetadata = new TypeMetadata();

        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");

        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");

        String asString = typeMetadata.toString();

        TypeMetadata fromString = new TypeMetadata(asString);
    }

    @Test
    public void testReadNewSerializedFormat() throws Exception {
        TypeMetadata typeMetadata = new TypeMetadata();

        String newFormat = "dts:[0:ingest1,1:ingest2];types:[0:DateType,1:IntegerType,2:LcType];FIELD1:[1:0,0:2];FIELD2:[1:2,0:1]";

        TypeMetadata fromString = new TypeMetadata(newFormat, true);
    }
}
