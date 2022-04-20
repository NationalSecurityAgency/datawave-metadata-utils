package datawave.util;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Date;

public class DateSchemaTest {
    
    private DateSchema dateSchema = new DateSchema();
    
    @Test
    public void testDateSchemaConfig() {
        Assertions.assertNull(dateSchema.getFieldName(0));
        Assertions.assertEquals("dateMillis", dateSchema.getFieldName(1));
        
        Assertions.assertEquals(0, dateSchema.getFieldNumber("bogusField"));
        Assertions.assertEquals(1, dateSchema.getFieldNumber("dateMillis"));
        
        Assertions.assertTrue(dateSchema.isInitialized(null));
        
        Assertions.assertEquals("Date", dateSchema.messageName());
        Assertions.assertEquals("java.util.Date", dateSchema.messageFullName());
        Assertions.assertEquals(Date.class, dateSchema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        Date sourceDate = new Date(0);
        
        byte[] multimapBytes = ProtobufIOUtil.toByteArray(sourceDate, dateSchema, LinkedBuffer.allocate());
        
        Assertions.assertNotNull(multimapBytes);
        Assertions.assertTrue(multimapBytes.length > 0);
        
        Date destDate = dateSchema.newMessage();
        ProtobufIOUtil.mergeFrom(multimapBytes, destDate, dateSchema);
        
        Assertions.assertEquals(sourceDate, destDate);
    }
    
    @Test
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        Date destDate = dateSchema.newMessage();
        Assertions.assertThrows(RuntimeException.class, () -> ProtobufIOUtil.mergeFrom(someBytes, destDate, dateSchema));
    }
}
