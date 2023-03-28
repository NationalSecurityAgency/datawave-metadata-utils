package datawave.query.util;

import com.google.common.collect.Maps;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.composite.CompositeMetadataHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
    private static final InMemoryInstance instance = new InMemoryInstance();
    private static MetadataHelper mdh;
    
    private static AllFieldMetadataHelper createAllFieldMetadataHelper(Connector connector) {
        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        final Set<Authorizations> auths = Collections.singleton(new Authorizations(AUTHS));
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, connector, TABLE_METADATA, auths, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(connector, TABLE_METADATA, auths);
        
        return new AllFieldMetadataHelper(tmh, cmh, connector, TABLE_METADATA, auths, allMetadataAuths);
    }
    
    private static Connector getConnector() {
        try {
            return instance.getConnector("root", new PasswordToken(""));
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static void setupTable(Connector connector) throws AccumuloException, TableExistsException, AccumuloSecurityException {
        TableOperations tableOperations = connector.tableOperations();
        if (!tableOperations.exists(TABLE_METADATA)) {
            tableOperations.create(TABLE_METADATA);
        }
    }
    
    private static void addFields(Connector connector) throws TableNotFoundException {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = connector.createBatchWriter(TABLE_METADATA, config)) {
            Mutation m = new Mutation("rowA");
            m.put("t", "dataTypeA", new Value("value"));
            writer.addMutation(m);
            writer.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Before
    public void setup() throws TableNotFoundException, AccumuloException, TableExistsException, AccumuloSecurityException {
        setupTable(getConnector());
        addFields(getConnector());
        mdh = new MetadataHelper(createAllFieldMetadataHelper(getConnector()), Collections.emptySet(), getConnector(), TABLE_METADATA, Collections.emptySet(),
                        Collections.emptySet());
    }
    
    @Test // we expect our row to be filtered out
    public void testPopulatedTypeFilter() throws TableNotFoundException {
        assertEquals(Collections.EMPTY_SET, mdh.getAllFields(Collections.singleton("rowA")));
    }
    
    @Test // we expect a null filter to allow all the things
    public void testNullTypeFilter() throws TableNotFoundException {
        assertEquals(Collections.singleton("rowA"), mdh.getAllFields(null));
    }
    
    @Test // we expect an empty filter to allow none of the things
    public void testEmptyTypeFilter() throws TableNotFoundException {
        assertEquals(Collections.EMPTY_SET, mdh.getAllFields(Collections.EMPTY_SET));
    }
}
