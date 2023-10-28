package datawave.query.util;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

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
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.FieldIndexHole;
import datawave.util.time.DateHelper;

class AllFieldMetadataHelperTest {
    
    private static final String TABLE_METADATA = "metadata";
    private static final String[] AUTHS = {"FOO"};
    private static final String NULL_BYTE = "\0";
    private static final Value NULL_VALUE = new Value(new byte[0]);
    private AccumuloClient accumuloClient;
    private AllFieldMetadataHelper helper;
    
    @BeforeAll
    static void beforeAll() throws URISyntaxException {
        File dir = new File(ClassLoader.getSystemClassLoader().getResource(".").toURI());
        File targetDir = dir.getParentFile();
        System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
    }
    
    /**
     * Set up the accumulo client and initialize the helper.
     */
    @BeforeEach
    void setUp() throws AccumuloSecurityException, AccumuloException, TableExistsException {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(AllFieldMetadataHelper.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        final Set<Authorizations> auths = Collections.singleton(new Authorizations(AUTHS));
        TypeMetadataHelper typeMetadataHelper = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, accumuloClient, TABLE_METADATA, auths, false);
        CompositeMetadataHelper compositeMetadataHelper = new CompositeMetadataHelper(accumuloClient, TABLE_METADATA, auths);
        helper = new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, accumuloClient, TABLE_METADATA, auths, allMetadataAuths);
    }
    
    /**
     * Clear the metadata table after each test.
     */
    @AfterEach
    void tearDown() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        accumuloClient.tableOperations().deleteRows(TABLE_METADATA, null, null);
    }
    
    /**
     * Write the given mutations to the metadata table.
     */
    private void writeMutations(Collection<Mutation> mutations) {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = accumuloClient.createBatchWriter(TABLE_METADATA, config)) {
            writer.addMutations(mutations);
            writer.flush();
        } catch (MutationsRejectedException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Tests for {@link AllFieldMetadataHelper#getFieldIndexHoles()}.
     */
    @SuppressWarnings("unchecked")
    @Nested
    public class FieldIndexHoleTests {
        
        private final Supplier<Map<String,Map<String,FieldIndexHole>>> INDEX_FUNCTION = () -> {
            try {
                return helper.getFieldIndexHoles();
            } catch (TableNotFoundException | CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        };
        
        private final Supplier<Map<String,Map<String,FieldIndexHole>>> REVERSED_INDEX_FUNCTION = () -> {
            try {
                return helper.getReversedFieldIndexHoles();
            } catch (TableNotFoundException | CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        };
        
        private Supplier<Map<String,Map<String,FieldIndexHole>>> getIndexHoleFunction(String cf) {
            return cf.equals("i") ? INDEX_FUNCTION : REVERSED_INDEX_FUNCTION;
        }
        
        /**
         * Test against data that has no field index holes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testNoFieldIndexHoles(String cf) {
            // Create a series of frequency rows over date ranges, each with a matching index row for each date.
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200120");
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200120");
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200120");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200120");
            mutationCreator.addFrequencyMutations("NAME", "maze", "20200101", "20200120");
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200101", "20200120");
            mutationCreator.addFrequencyMutations("EVENT_DATE", "csv", "20200101", "20200120");
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "csv", "20200101", "20200120");
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200101", "20200120");
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200101", "20200120");
            mutationCreator.addFrequencyMutations("EVENT_DATE", "maze", "20200101", "20200120");
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "maze", "20200101", "20200120");
            writeMutations(mutationCreator.getMutations());
            
            // Verify that no index holes were found.
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            Assertions.assertTrue(fieldIndexHoles.isEmpty());
        }
        
        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForEntireFrequencyDateRange(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105"); // Do not create matching index rows for these.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105");
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForStartOfFrequencyDateRange(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200105");
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105");
            writeMutations(mutationCreator.getMutations());
    
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForEndOfFrequencyDateRange(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102");
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105");
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForMiddleOfFrequencyDateRange(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200110");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200103");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200107", "20200110");
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105");
            writeMutations(mutationCreator.getMutations());
    
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleFieldIndexHolesInFrequencyDateRange(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200120");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200106");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200110", "20200113");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200117", "20200118");
            writeMutations(mutationCreator.getMutations());
    
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleAtEndOfFrequencyDateRangeForNonLastCombo(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102");
            mutationCreator.addFrequencyMutations("ZETA", "csv", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200101", "20200105");
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where the expected index hole spans across multiple frequency ranges.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleSpanningMultipleFrequencyDateRanges(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105");
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200110", "20200115");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200103");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200113", "20200115");
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200105"), dateRange("20200110", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where everything is an index hole.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105");
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115");
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125");
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328");
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createFieldIndexHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where we have a number of index holes that span just a day.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles(String cf) {
            MutationCreator mutationCreator = new MutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104");
            // Index holes for NAME-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115");
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200111", "20200112");
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200114", "20200115");
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125");
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121");
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125");
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328");
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220");
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302");
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315");
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328");
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,FieldIndexHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createFieldIndexHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
    }
    
    private Map<String,Map<String,FieldIndexHole>> createFieldIndexHoleMap(FieldIndexHole... holes) {
        Map<String,Map<String,FieldIndexHole>> fieldIndexHoles = new HashMap<>();
        for (FieldIndexHole hole : holes) {
            Map<String,FieldIndexHole> datatypeMap = fieldIndexHoles.computeIfAbsent(hole.getFieldName(), k -> new HashMap<>());
            datatypeMap.put(hole.getDatatype(), hole);
        }
        return fieldIndexHoles;
    }
    
    @SafeVarargs
    private FieldIndexHole createFieldIndexHole(String field, String datatype, Pair<Date,Date>... dateRanges) {
        return new FieldIndexHole(field, datatype, Sets.newHashSet(dateRanges));
    }
    
    private Pair<Date,Date> dateRange(String start, String end) {
        return Pair.of(DateHelper.parse(start), DateHelper.parse(end));
    }
    
    /**
     * Helper class for creating mutations in bulk.
     */
    private static class MutationCreator {
        
        private final List<Mutation> mutations = new ArrayList<>();
        
        private void addFrequencyMutations(String fieldName, String datatype, String startDate, String endDate) {
            List<String> dates = getDatesInRange(startDate, endDate);
            dates.forEach(date -> addFrequencyMutation(fieldName, datatype, date));
        }
        
        private void addFrequencyMutation(String fieldName, String datatype, String date) {
            addMutation(fieldName, "f", datatype + NULL_BYTE + date, new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        }
        
        private void addIndexMutations(String cf, String fieldName, String datatype, String startDate, String endDate) {
            List<String> dates = getDatesInRange(startDate, endDate);
            dates.forEach(date -> addIndexMutation(cf, fieldName, datatype, date));
        }
        
        private void addIndexMutation(String cf, String fieldName, String datatype, String date) {
            addMutation(fieldName, cf, datatype + NULL_BYTE + date, NULL_VALUE);
        }
        
        private List<String> getDatesInRange(String startDateStr, String endDateStr) {
            Date startDate = DateHelper.parse(startDateStr);
            Date endDate = DateHelper.parse(endDateStr);
            
            List<String> dates = new ArrayList<>();
            dates.add(startDateStr);
            
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            while (true) {
                calendar.add(Calendar.DAY_OF_MONTH, 1);
                Date date = calendar.getTime();
                if (date.before(endDate) || date.equals(endDate)) {
                    dates.add(DateHelper.format(date));
                } else {
                    break;
                }
            }
            
            return dates;
        }
        
        private void addMutation(String row, String columnFamily, String columnQualifier, Value value) {
            Mutation mutation = new Mutation(row);
            mutation.put(columnFamily, columnQualifier, value);
            mutations.add(mutation);
        }
        
        private List<Mutation> getMutations() {
            return mutations;
        }
    }
}
