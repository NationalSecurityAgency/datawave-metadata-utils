package datawave.query.util;

import com.google.common.collect.Maps;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.composite.CompositeMetadataHelper;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MetadataHelperTest {
    
    private static final String TABLE_METADATA = "testMetadataTable";
    private static final String[] AUTHS = {"FOO"};
    private static MetadataHelper mdh;
    private static AccumuloClient accumuloClient;
    
    private static AllFieldMetadataHelper createAllFieldMetadataHelper() {
        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        final Set<Authorizations> auths = Collections.singleton(new Authorizations(AUTHS));
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, accumuloClient, TABLE_METADATA, auths, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(accumuloClient, TABLE_METADATA, auths);
        
        return new AllFieldMetadataHelper(tmh, cmh, accumuloClient, TABLE_METADATA, auths, allMetadataAuths);
    }
    
    private static void addFields(Mutation m) throws TableNotFoundException {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = accumuloClient.createBatchWriter(TABLE_METADATA, config)) {
            writer.addMutation(m);
            writer.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Before
    public void setup() throws TableNotFoundException, AccumuloException, TableExistsException, AccumuloSecurityException {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(MetadataHelperTest.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
        mdh = new MetadataHelper(createAllFieldMetadataHelper(), Collections.emptySet(), accumuloClient, TABLE_METADATA, Collections.emptySet(),
                        Collections.emptySet());
    }
    
    @Test // we expect our row to be filtered out
    public void testSingleFieldFilter() throws TableNotFoundException {
        Mutation m = new Mutation("rowA");
        m.put("t", "dataTypeA", new Value("value"));
        addFields(m);
        
        testFilter(Collections.EMPTY_SET, mdh.getAllFields(Collections.singleton("rowA")));
        testFilter(Collections.singleton("rowA"), mdh.getAllFields(null));
        testFilter(Collections.EMPTY_SET, mdh.getAllFields(Collections.EMPTY_SET));
    }
    
    private void testFilter(Set<String> expected, Set<String> actual) throws TableNotFoundException {
        assertEquals(expected, actual);
    }
}
