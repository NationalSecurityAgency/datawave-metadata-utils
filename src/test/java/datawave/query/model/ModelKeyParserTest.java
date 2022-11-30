package datawave.query.model;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ModelKeyParser.class)
@PowerMockIgnore("org.apache.log4j")
public class ModelKeyParserTest {
    
    private static final String MODEL_NAME = "MODEL";
    private static final String FIELD_NAME = "field1";
    private static final String MODEL_FIELD_NAME = "mappedField1";
    private static final String DATATYPE = "test";
    private static final String COLVIZ = "PRIVATE";
    private static final Direction FORWARD = Direction.FORWARD;
    private static final Direction REVERSE = Direction.REVERSE;
    private static final Set<Authorizations> AUTHS = Collections.singleton(new Authorizations("PRIVATE, PUBLIC"));
    private static FieldMapping FORWARD_FIELD_MAPPING = null;
    private static FieldMapping LENIENT_MAPPING = null;
    private static FieldMapping REVERSE_FIELD_MAPPING = null;
    private static FieldMapping NULL_CV_MAPPING = null;
    private static Key FORWARD_KEY = null;
    private static Key LENIENT_KEY = null;
    private static Key REVERSE_KEY = null;
    private static Key NULL_CV_KEY = null;
    private static Mutation FORWARD_MUTATION = null;
    private static Mutation FORWARD_DELETE_MUTATION = null;
    private static Mutation LENIENT_MUTATION = null;
    private static Mutation LENIENT_DELETE_MUTATION = null;
    private static Mutation REVERSE_MUTATION = null;
    private static Mutation REVERSE_DELETE_MUTATION = null;
    
    private static long TIMESTAMP = System.currentTimeMillis();
    
    @Before
    public void setup() throws Exception {
        FORWARD_FIELD_MAPPING = new FieldMapping();
        FORWARD_FIELD_MAPPING.setColumnVisibility(COLVIZ);
        FORWARD_FIELD_MAPPING.setDatatype(DATATYPE);
        FORWARD_FIELD_MAPPING.setDirection(FORWARD);
        FORWARD_FIELD_MAPPING.setFieldName(FIELD_NAME);
        FORWARD_FIELD_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
        LENIENT_MAPPING = new FieldMapping();
        LENIENT_MAPPING.setColumnVisibility(COLVIZ);
        LENIENT_MAPPING.setDatatype(DATATYPE);
        LENIENT_MAPPING.setDirection(FORWARD);
        LENIENT_MAPPING.setFieldName(FieldMapping.LENIENT);
        LENIENT_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
        REVERSE_FIELD_MAPPING = new FieldMapping();
        REVERSE_FIELD_MAPPING.setColumnVisibility(COLVIZ);
        REVERSE_FIELD_MAPPING.setDatatype(DATATYPE);
        REVERSE_FIELD_MAPPING.setDirection(REVERSE);
        REVERSE_FIELD_MAPPING.setFieldName(FIELD_NAME);
        REVERSE_FIELD_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
        NULL_CV_MAPPING = new FieldMapping();
        NULL_CV_MAPPING.setColumnVisibility("");
        NULL_CV_MAPPING.setDatatype(DATATYPE);
        NULL_CV_MAPPING.setDirection(REVERSE);
        NULL_CV_MAPPING.setFieldName(FIELD_NAME);
        NULL_CV_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        COLVIZ, TIMESTAMP);
        LENIENT_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE,
                        FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        COLVIZ, TIMESTAMP);
        NULL_CV_KEY = new Key(FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        "", TIMESTAMP);
        FORWARD_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_MUTATION.put(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        FORWARD_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE,
                        FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ),
                        TIMESTAMP);
        LENIENT_MUTATION = new Mutation(MODEL_FIELD_NAME);
        LENIENT_MUTATION.put(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        LENIENT_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        LENIENT_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE,
                        FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP);
        LENIENT_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE,
                        FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + "index_only" + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        
        REVERSE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_MUTATION.put(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        REVERSE_DELETE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        
        PowerMock.mockStatic(System.class, System.class.getMethod("currentTimeMillis"));
    }
    
    @Test
    public void testForwardKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(FORWARD_KEY);
        Assert.assertEquals(FORWARD_FIELD_MAPPING, mapping);
        Assert.assertFalse(mapping.isLenient());
        
        // Test ForwardKeyParse with no datatype
        FORWARD_FIELD_MAPPING.setDatatype(null);
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        mapping = ModelKeyParser.parseKey(FORWARD_KEY);
        Assert.assertEquals(FORWARD_FIELD_MAPPING, mapping);
    }
    
    @Test
    public void testLenientKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(LENIENT_KEY);
        Assert.assertEquals(LENIENT_MAPPING, mapping);
        Assert.assertTrue(mapping.isLenient());
        
        // Test ForwardKeyParse with no datatype
        LENIENT_MAPPING.setDatatype(null);
        LENIENT_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        mapping = ModelKeyParser.parseKey(LENIENT_KEY);
        Assert.assertEquals(LENIENT_MAPPING, mapping);
    }
    
    @Test
    public void testReverseKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(REVERSE_KEY);
        Assert.assertEquals(REVERSE_FIELD_MAPPING, mapping);
        
        // Test ReverseKeyParse with no datatype
        REVERSE_FIELD_MAPPING.setDatatype(null);
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        mapping = ModelKeyParser.parseKey(REVERSE_KEY);
        Assert.assertEquals("ReverseKeyParse with no datatype failed.", REVERSE_FIELD_MAPPING, mapping);
    }
    
    @Test
    public void testForwardMappingParse() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Key k = ModelKeyParser.createKey(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        Assert.assertEquals(FORWARD_KEY, k);
        
        // Test forwardMappingParse with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        k = ModelKeyParser.createKey(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        PowerMock.verifyAll();
        Assert.assertEquals(FORWARD_KEY, k);
    }
    
    @Test
    public void testLenientMappingParse() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Key k = ModelKeyParser.createKey(LENIENT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        Assert.assertEquals(LENIENT_KEY, k);
        
        // Test LENIENTMappingParse with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        LENIENT_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        k = ModelKeyParser.createKey(LENIENT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        LENIENT_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        PowerMock.verifyAll();
        Assert.assertEquals(LENIENT_KEY, k);
    }
    
    @Test
    public void testReverseMappingParse() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Key k = ModelKeyParser.createKey(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        Assert.assertEquals(REVERSE_KEY, k);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        k = ModelKeyParser.createKey(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        Assert.assertEquals(REVERSE_KEY, k);
    }
    
    @Test
    public void testForwardCreateMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(FORWARD_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        m = ModelKeyParser.createMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        FORWARD_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_MUTATION.put(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(FORWARD_MUTATION, m);
    }
    
    @Test
    public void testLenientCreateMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createMutation(LENIENT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(LENIENT_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        LENIENT_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        m = ModelKeyParser.createMutation(LENIENT_MAPPING, MODEL_NAME);
        LENIENT_MUTATION = new Mutation(MODEL_FIELD_NAME);
        LENIENT_MUTATION.put(MODEL_NAME, FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(LENIENT_MUTATION, m);
    }
    
    @Test
    public void testReverseCreateMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(REVERSE_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        m = ModelKeyParser.createMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        REVERSE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_MUTATION.put(MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(REVERSE_MUTATION, m);
    }
    
    @Test
    public void testForwardCreateDeleteMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createDeleteMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(FORWARD_DELETE_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        FORWARD_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(FORWARD_DELETE_MUTATION, m);
    }
    
    @Test
    public void testLenientCreateDeleteMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createDeleteMutation(LENIENT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(LENIENT_DELETE_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        LENIENT_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        LENIENT_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        LENIENT_DELETE_MUTATION.putDelete(MODEL_NAME, FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ),
                        TIMESTAMP);
        LENIENT_DELETE_MUTATION.putDelete(MODEL_NAME,
                        FieldMapping.LENIENT + ModelKeyParser.NULL_BYTE + "index_only" + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(LENIENT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(LENIENT_DELETE_MUTATION, m);
    }
    
    @Test
    public void testReverseCreateDeleteMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createDeleteMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(REVERSE_DELETE_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        REVERSE_DELETE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_DELETE_MUTATION.putDelete(MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), new ColumnVisibility(COLVIZ),
                        TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(REVERSE_DELETE_MUTATION, m);
    }
    
    @Test
    public void testParseKeyNullCV() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(NULL_CV_KEY);
        Assert.assertEquals(NULL_CV_MAPPING, mapping);
    }
    
    @Test
    public void testForwardMappingIndexOnlyParse() throws Exception {
        
        // Test with datatype
        FieldMapping forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDatatype(DATATYPE);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);
        
        Key expectedForwardKey = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE,
                        FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Key k = ModelKeyParser.createKey(forwardMapping, MODEL_NAME);
        Assert.assertEquals(expectedForwardKey, k);
        
        // Test without datatype
        PowerMock.resetAll();
        forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);
        
        expectedForwardKey = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        k = ModelKeyParser.createKey(forwardMapping, MODEL_NAME);
        Assert.assertEquals(expectedForwardKey, k);
    }
    
    @Test
    public void testForwardIndexOnlyCreateMutation() throws Exception {
        FieldMapping forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility("PRIVATE");
        forwardMapping.setDatatype(DATATYPE);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);
        
        Mutation expectedforwardMutation = new Mutation(MODEL_FIELD_NAME);
        Text cf = new Text(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE);
        Text cq = new Text(FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue());
        expectedforwardMutation.put(cf, cq, new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createMutation(forwardMapping, MODEL_NAME);
        m.getUpdates();
        Assert.assertTrue("Expected true: expectedforwardMutation.equals(m)", expectedforwardMutation.equals(m));
        
        // Without Datatype
        PowerMock.resetAll();
        forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);
        
        expectedforwardMutation = new Mutation(MODEL_FIELD_NAME);
        cf = new Text(MODEL_NAME);
        cq = new Text(FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue());
        expectedforwardMutation.put(cf, cq, new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        m = ModelKeyParser.createMutation(forwardMapping, MODEL_NAME);
        m.getUpdates();
        Assert.assertTrue("Expected true: expectedforwardMutation.equals(m)", expectedforwardMutation.equals(m));
    }
    
    /**
     * Test boundary conditions on ForwardKeyParsing / Trigger failure conditions
     * 
     * @throws Exception
     */
    
    @Test(expected = IllegalArgumentException.class)
    public void testKeyWithNoDirection() throws Exception {
        // Test key with no direction
        Key keyNoDirection = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME, COLVIZ, TIMESTAMP);
        ModelKeyParser.parseKey(keyNoDirection);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testKeyWithInvalidDirection() throws Exception {
        Key keyWrongDirection = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "someInvalidDirection", COLVIZ, TIMESTAMP);
        ModelKeyParser.parseKey(keyWrongDirection);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testKeyWithTooManyPartsInColQualifier() throws Exception {
        Key keyTooManyParts = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue() + ModelKeyParser.NULL_BYTE
                        + "index_only" + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        ModelKeyParser.parseKey(keyTooManyParts);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testKeyWithIncorrectlyPositionedIndexOnlyAndDirection() throws Exception {
        // Correct cq: field\x00
        Key mismatchedParts = new Key(MODEL_FIELD_NAME, MODEL_NAME,
                        FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue() + ModelKeyParser.NULL_BYTE + "index_only", COLVIZ, TIMESTAMP);
        ModelKeyParser.parseKey(mismatchedParts);
        Assert.fail("Expected IllegalArgumentException on key with 'index_only' and 'forward' in wrong positions.");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testIndexOnlyOnAReverseKeyIsInvalid() throws Exception {
        // Test index_only on a reverse key.. reverse keys should not have index_only
        Key reverseIndexOnly = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + REVERSE.getValue(), COLVIZ,
                        TIMESTAMP);
        ModelKeyParser.parseKey(reverseIndexOnly);
    }
}
