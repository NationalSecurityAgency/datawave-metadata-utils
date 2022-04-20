package datawave.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class StringMultimapSchemaTest {
    
    private StringMultimapSchema stringMultimapSchema = new StringMultimapSchema();
    
    @Test
    public void testMultimapSchemaConfig() {
        Assertions.assertNull(stringMultimapSchema.getFieldName(0));
        Assertions.assertEquals("e", stringMultimapSchema.getFieldName(1));
        
        Assertions.assertEquals(0, stringMultimapSchema.getFieldNumber("bogusField"));
        Assertions.assertEquals(1, stringMultimapSchema.getFieldNumber("e"));
        
        Assertions.assertTrue(stringMultimapSchema.isInitialized(null));
        
        Assertions.assertEquals("Multimap", stringMultimapSchema.messageName());
        Assertions.assertEquals("com.google.common.collect.Multimap", stringMultimapSchema.messageFullName());
        Assertions.assertEquals(Multimap.class, stringMultimapSchema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        Multimap<String,String> sourceMultimap = ArrayListMultimap.create();
        sourceMultimap.put("this", "that");
        sourceMultimap.putAll("something", Arrays.asList("this", "that", "the other"));
        
        byte[] multimapBytes = ProtobufIOUtil.toByteArray(sourceMultimap, stringMultimapSchema, LinkedBuffer.allocate());
        
        Assertions.assertNotNull(multimapBytes);
        Assertions.assertTrue(multimapBytes.length > 0);
        
        Multimap<String,String> destMultimap = stringMultimapSchema.newMessage();
        ProtobufIOUtil.mergeFrom(multimapBytes, destMultimap, stringMultimapSchema);
        
        Assertions.assertEquals(sourceMultimap, destMultimap);
    }
    
    @Test
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        Multimap<String,String> destMultimap = stringMultimapSchema.newMessage();
        Assertions.assertThrows(RuntimeException.class, () -> ProtobufIOUtil.mergeFrom(someBytes, destMultimap, stringMultimapSchema));
    }
}
