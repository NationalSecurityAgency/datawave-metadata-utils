package datawave.iterators;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.ColumnFamilyConstants;
import datawave.query.model.DateFrequencyMap;
import datawave.security.util.ScannerHelper;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class FrequencyMetadataAggregatorTest {
    
    private static final String TABLE_METADATA = "metadata";
    private static final String[] AUTHS = {"FOO", "BAR", "COB"};
    private static final Set<Authorizations> AUTHS_SET = Collections.singleton(new Authorizations(AUTHS));
    private static final String NULL_BYTE = "\0";
    
    private AccumuloClient accumuloClient;
    private Boolean combineColumnVisibilities;
    
    @BeforeAll
    static void beforeAll() throws URISyntaxException {
        File dir = new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).toURI());
        File targetDir = dir.getParentFile();
        System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
    }
    
    @BeforeEach
    public void setUp() throws Exception {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(FrequencyMetadataAggregatorTest.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        accumuloClient.tableOperations().deleteRows(TABLE_METADATA, null, null);
        combineColumnVisibilities = null;
    }
    
    @Test
    public void testNoAggregatedEntries() throws TableNotFoundException, IOException {
        Mutations mutations = new Mutations();
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200101", 1L, "FOO", 1500000000L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200101", 1L, "FOO", 1500000001L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200101", 1L, "FOO", 1500000002L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200101", 1L, "FOO", 1500000003L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200102", 2L, "FOO", 1500000000L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200102", 2L, "FOO", 1500000001L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200102", 2L, "FOO", 1500000002L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200102", 2L, "FOO", 1500000003L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200103", 3L, "FOO", 1500000000L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200103", 3L, "FOO", 1500000001L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200103", 3L, "FOO", 1500000002L);
        mutations.addNonAggregatedRow("NAME", "f", "csv", "20200103", 3L, "FOO", 1500000003L);
        writeMutations(mutations);
    
        System.out.println("Writing mutations");
        
        Scanner scanner = createScanner();
        
        System.out.println("Creating scanner");
        
        for (Map.Entry<Key,Value> entry : scanner) {
            System.out.println("Key: " + entry.getKey());
            DateFrequencyMap map = new DateFrequencyMap(entry.getValue().get());
            System.out.println("Value: " + map);
        }
    }
    
    private Scanner createScanner() throws TableNotFoundException {
        Scanner scanner = ScannerHelper.createScanner(accumuloClient, TABLE_METADATA, AUTHS_SET);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_RI);
    
        IteratorSetting iteratorSetting = new IteratorSetting(10, FrequencyMetadataAggregator.class);
        if (combineColumnVisibilities != null) {
            iteratorSetting.addOption(FrequencyMetadataAggregator.COMBINE_VISIBILITIES, String.valueOf(combineColumnVisibilities));
        }
        scanner.addScanIterator(iteratorSetting);
        
        return scanner;
    }
    
    private void writeMutations(Mutations mutations) {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = accumuloClient.createBatchWriter(TABLE_METADATA, config)) {
            writer.addMutations(mutations.mutations);
            writer.flush();
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void givenCombineColumnVisibilities(Boolean combineColumnVisibilities) {
        this.combineColumnVisibilities = combineColumnVisibilities;
    }
    
    private static class Mutations {
        
        private final List<Mutation> mutations = new ArrayList<>();
    
        private void addNonAggregatedRow(String row, String colf, String datatype, String date, long count, String colv, long timestamp) {
            Value value = new Value(LongCombiner.VAR_LEN_ENCODER.encode(count));
            String colq = datatype + NULL_BYTE + date;
            addMutation(row, colf, colq, colv, timestamp, value);
        }
        
        private void addAggregatedRow(String row, String colf, String datatype, String date, long count, String colv, long timestamp) {
            DateFrequencyMap map = new DateFrequencyMap();
            map.increment(date, count);
            addAggregatedRow(row, colf, datatype, colv, timestamp, map);
        }
    
        private void addAggregatedRow(String row, String colf, String datatype, String colv, long timestamp, DateFrequencyMap map) {
            Value value = new Value(WritableUtils.toByteArray(map));
            addMutation(row, colf, datatype, colv, timestamp, value);
        }
        
        private void addMutation(String row, String colf, String colq, String colv, long timestamp, Value value) {
            Mutation mutation = new Mutation(row);
            mutation.put(colf, colq, new ColumnVisibility(colv), timestamp, value);
            mutations.add(mutation);
        }
    }
}
