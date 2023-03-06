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

        Set<String> bar = typeMetadata.getDataTypesForField("field2");
        assertEquals(3, bar.size());
        assertTrue(bar.contains("[IntegerType]"));
        assertTrue(bar.contains("[LcType]"));
        assertTrue(bar.contains("[NumberType]"));
    }

}
