package datawave.query.util;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        
        Set<String> norms2 = typeMetadata.getNormalizerNamesForField("FIELD42");
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
    public void testReadNewSerializedFormatMultipleFieldsAndTypes() throws Exception {
        String newFormat = "dts:[0:ingest1,1:ingest2];types:[0:DateType,1:IntegerType,2:LcType];FIELD1:[0:2,1:0];FIELD2:[0:1,1:2]";
        
        TypeMetadata fromString = new TypeMetadata(newFormat, true);
        
        Set<String> types1 = fromString.getDataTypesForField("FIELD1");
        assertEquals(2, types1.size());
        assertTrue(types1.contains("ingest1"));
        assertTrue(types1.contains("ingest2"));
        
        Set<String> normalizers = fromString.getNormalizerNamesForField("FIELD1");
        assertEquals(2, normalizers.size());
        assertTrue(normalizers.contains("DateType"));
        assertTrue(normalizers.contains("LcType"));
    }
    
    @Test
    public void testReadNewSerializedFormatSingleFieldAndType() throws Exception {
        String newFormat = "dts:[0:ingest1];types:[0:DateType];FIELD1:[0:0]";
        
        TypeMetadata fromString = new TypeMetadata(newFormat, true);
        
        Set<String> types1 = fromString.getDataTypesForField("FIELD1");
        assertEquals(1, types1.size());
        assertTrue(types1.contains("ingest1"));
        
        Set<String> normalizers = fromString.getNormalizerNamesForField("FIELD1");
        assertEquals(1, normalizers.size());
        assertTrue(normalizers.contains("DateType"));
    }
    
    @Test
    public void testWriteNewSerializedFormat() {
        TypeMetadata typeMetadata = new TypeMetadata();
        
        typeMetadata.put("FIELD1", "ingest1", "LcType");
        typeMetadata.put("FIELD1", "ingest2", "DateType");
        
        typeMetadata.put("FIELD2", "ingest1", "IntegerType");
        typeMetadata.put("FIELD2", "ingest2", "LcType");
        
        String newString = typeMetadata.toNewString();
        
        String expectedString = "dts:[0:ingest1,1:ingest2];types:[0:DateType,1:IntegerType,2:LcType];FIELD1:[0:2,1:0];FIELD2:[0:1,1:2]";
        
        assertEquals(expectedString, newString);
    }
    
    public void testReduceEmptyTypeMetadata() {
        TypeMetadata reduced = typeMetadata.reduce(Collections.emptySet());
        assertTrue(reduced.typeMetadata.isEmpty());
    }
    
    @Test
    public void testReductionNoFieldsMatch() {
        TypeMetadata reduced = typeMetadata.reduce(Collections.singleton("FIELD4"));
        assertTrue(reduced.typeMetadata.isEmpty());
    }
    
    @Test
    public void testReductionSomeFieldsMatch() {
        TypeMetadata reduced = typeMetadata.reduce(Sets.newHashSet("FIELD1", "FIELD2"));
        assertTrue(reduced.keySet().contains("FIELD1"));
        assertTrue(reduced.keySet().contains("FIELD2"));
        assertFalse(reduced.keySet().contains("FIELD3"));
    }
    
    @Test
    public void testReductionAllFieldsMatch() {
        TypeMetadata reduced = typeMetadata.reduce(Sets.newHashSet("FIELD1", "FIELD2", "FIELD3"));
        assertTrue(reduced.keySet().contains("FIELD1"));
        assertTrue(reduced.keySet().contains("FIELD2"));
        assertTrue(reduced.keySet().contains("FIELD3"));
        assertEquals(reduced, typeMetadata);
    }
}
