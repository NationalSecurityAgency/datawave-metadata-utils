package datawave.query.util;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

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
