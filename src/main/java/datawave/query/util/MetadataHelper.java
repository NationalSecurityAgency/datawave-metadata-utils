package datawave.query.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import datawave.data.ColumnFamilyConstants;
import datawave.data.MetadataCardinalityCounts;
import datawave.data.type.Type;
import datawave.iterators.EdgeMetadataCombiner;
import datawave.iterators.filter.EdgeMetadataCQStrippingIterator;
import datawave.marking.MarkingFunctions;
import datawave.query.composite.CompositeMetadata;
import datawave.query.model.QueryModel;
import datawave.security.util.AuthorizationsMinimizer;
import datawave.security.util.ScannerHelper;
import datawave.util.StringUtils;
import datawave.util.UniversalSet;
import datawave.util.time.DateHelper;
import datawave.util.time.TraceStopwatch;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <p>
 * Helper class to fetch the set of field names which are only indexed, i.e. do not occur as attributes in the event.
 * </p>
 * 
 * <p>
 * This set would normally includes all tokenized content fields. In terms of keys in the DatawaveMetadata table, this set would contain all rows in the
 * {@code DatawaveMetadata} table which have a {@link ColumnFamilyConstants#COLF_I} but not a {@link ColumnFamilyConstants#COLF_E}
 * </p>
 * 
 * 
 * TODO -- Break this class apart
 * 
 */
@EnableCaching
@Component("metadataHelper")
@Scope("prototype")
public class MetadataHelper {
    private static final Logger log = LoggerFactory.getLogger(MetadataHelper.class);
    
    public static final String NULL_BYTE = "\0";
    
    protected static final Text PV = new Text("pv");
    
    public static final char AGGREGATED_FREQ_COL_QUAL = 'a';
    
    protected static final Function<MetadataEntry,String> toFieldName = new MetadataEntryToFieldName(), toDatatype = new MetadataEntryToDatatype();
    
    /**
     * Determine if this key is an F column and contains aggregated counts.
     */
    public static boolean isAggregatedFreqKey(Key key) {
        if (key.getColumnFamily().equals(ColumnFamilyConstants.COLF_F)) {
            return isAggregatedFreqCQ(key.getColumnQualifier());
        }
        return false;
    }
    
    /**
     * Determine if this CQ of an F column contains aggregated counts.
     */
    public static boolean isAggregatedFreqCQ(Text cq) {
        byte[] bytes = cq.getBytes();
        int len = cq.getLength();
        return ((len >= 2) && (bytes[len - 1] == (byte) AGGREGATED_FREQ_COL_QUAL) && (bytes[len - 2] == '\0'));
    }
    
    /**
     * Pull the datatype out of an aggregated F column CQ (more efficient than getDatatype(Key))
     */
    public static String getDataTypeFromAggregatedFreqCQ(Text cq) {
        try {
            return Text.decode(cq.getBytes(), 0, cq.getLength() - 2);
        } catch (CharacterCodingException cce) {
            throw new IllegalStateException("Unable to decode datatype from F column CQ " + cq.toString(), cce);
        }
    }
    
    /**
     * Pull the datatype out of a metadata key
     */
    public static String getDatatype(Key k) {
        String datatype = k.getColumnQualifier().toString();
        int index = datatype.indexOf('\0');
        if (index >= 0) {
            datatype = datatype.substring(0, index);
        }
        return datatype;
    }
    
    protected final Metadata metadata = new Metadata();
    
    protected final List<Text> metadataIndexColfs = Arrays.asList(ColumnFamilyConstants.COLF_I, ColumnFamilyConstants.COLF_RI);
    protected final List<Text> metadataNormalizedColfs = Arrays.asList(ColumnFamilyConstants.COLF_N);
    protected final List<Text> metadataTypeColfs = Arrays.asList(ColumnFamilyConstants.COLF_T);
    protected final List<Text> metadataCompositeIndexColfs = Arrays.asList(ColumnFamilyConstants.COLF_CI);
    protected final List<Text> metadataCardinalityColfs = Arrays.asList(ColumnFamilyConstants.COLF_COUNT);
    
    protected final Connector connector;
    protected final Instance instance;
    protected final String metadataTableName;
    protected final Set<Authorizations> auths;
    protected Set<Authorizations> fullUserAuths;
    
    protected final AllFieldMetadataHelper allFieldMetadataHelper;
    protected final Collection<Authorizations> allMetadataAuths;
    
    public MetadataHelper(AllFieldMetadataHelper allFieldMetadataHelper, Collection<Authorizations> allMetadataAuths, Connector connector,
                    String metadataTableName, Set<Authorizations> auths, Set<Authorizations> fullUserAuths) {
        Preconditions.checkNotNull(allFieldMetadataHelper, "An AllFieldMetadataHelper is required by MetadataHelper");
        this.allFieldMetadataHelper = allFieldMetadataHelper;
        
        Preconditions.checkNotNull(allMetadataAuths, "The set of all metadata authorization is required by MetadataHelper");
        this.allMetadataAuths = allMetadataAuths;
        
        Preconditions.checkNotNull(connector, "A valid Accumulo Connector is required by MetadataHelper");
        this.connector = connector;
        this.instance = connector.getInstance();
        
        Preconditions.checkNotNull(metadataTableName, "The name of the metadata table is required by MetadataHelper");
        this.metadataTableName = metadataTableName;
        
        Preconditions.checkNotNull(auths, "Authorizations are required by MetadataHelper");
        this.auths = auths;
        
        Preconditions.checkNotNull(fullUserAuths, "The full set of user authorizations is required by MetadataHelper");
        this.fullUserAuths = fullUserAuths;
        log.debug("initialized with auths subset: {}", this.auths);
        
        log.trace("Constructor connector: {} with auths: {} and metadata table name: {}", connector.getClass().getCanonicalName(), auths, metadataTableName);
    }
    
    /**
     * allMetadataAuths is a singleton Collection of one Authorizations instance that contains all of the auths required to see everything in the Metadata
     * table. userAuths is a Collection of Authorizations, every one of which must contain th
     * 
     * @param usersAuthsCollection
     * @param allMetadataAuthsCollection
     * @return
     */
    public static boolean userHasAllMetadataAuths(Collection<Authorizations> usersAuthsCollection, Collection<Authorizations> allMetadataAuthsCollection) {
        
        // first, minimize the usersAuths:
        Collection<Authorizations> minimizedCollection = AuthorizationsMinimizer.minimize(usersAuthsCollection);
        // now, the first entry in the minimized auths should have everything common to every Authorizations in the set
        // make sure that the first entry contains all the Authorizations in the allMetadataAuths
        Authorizations allMetadataAuths = allMetadataAuthsCollection.iterator().next(); // get the first (and only) one
        Authorizations minimized = minimizedCollection.iterator().next(); // get the first one, which has all auths common to all in the original collection
        return StreamSupport.stream(minimized.spliterator(), false).map(String::new).collect(Collectors.toSet())
                        .containsAll(StreamSupport.stream(allMetadataAuths.spliterator(), false).map(String::new).collect(Collectors.toSet()));
    }
    
    /**
     * allMetadataAuthsCollection is a singleton Collection of one Authorizations instance that contains all of the auths required to see everything in the
     * Metadata table. userAuthsCollection contains the user's auths. This method will return the retention of the user's auths from the
     * allMetadataAuthsCollection.
     *
     * @param usersAuthsCollection
     * @param allMetadataAuthsCollection
     * @return
     */
    public static Collection<String> getUsersMetadataAuthorizationSubset(Collection<Authorizations> usersAuthsCollection,
                    Collection<Authorizations> allMetadataAuthsCollection) {
        if (log.isTraceEnabled()) {
            log.trace("allMetadataAuthsCollection:" + allMetadataAuthsCollection);
            log.trace("usersAuthsCollection:" + usersAuthsCollection);
        }
        // first, minimize the usersAuths:
        Collection<Authorizations> minimizedCollection = AuthorizationsMinimizer.minimize(usersAuthsCollection);
        if (log.isTraceEnabled()) {
            log.trace("minimizedCollection:" + minimizedCollection);
        }
        
        // now, the first entry in the minimized auths should have everything common to every Authorizations in the set
        // make sure that the first entry contains all the Authorizations in the allMetadataAuths
        Authorizations allMetadataAuths = allMetadataAuthsCollection.iterator().next(); // get the first (and only) one
        Authorizations minimized = minimizedCollection.iterator().next(); // get the first one, which has all auths common to all in the original collection
        if (log.isTraceEnabled()) {
            log.trace("first of users auths minimized:" + minimized);
        }
        Set<String> minimizedUserAuths = StreamSupport.stream(minimized.spliterator(), false).map(String::new).collect(Collectors.toSet());
        
        Collection<String> minimizedAllMetadataAuths = StreamSupport.stream(allMetadataAuths.spliterator(), false).map(String::new).collect(Collectors.toSet());
        minimizedAllMetadataAuths.retainAll(minimizedUserAuths);
        if (log.isTraceEnabled()) {
            log.trace("minimized to:" + minimizedAllMetadataAuths);
        }
        return minimizedAllMetadataAuths;
    }
    
    private Set<Set<String>> getAllMetadataAuthsPowerSet(Collection<Authorizations> allMetadataAuthsCollection) {
        
        // first, minimize the usersAuths:
        Collection<Authorizations> minimizedCollection = AuthorizationsMinimizer.minimize(allMetadataAuthsCollection);
        // now, the first entry in the minimized auths should have everything common to every Authorizations in the set
        // make sure that the first entry contains all the Authorizations in the allMetadataAuths
        Authorizations minimized = minimizedCollection.iterator().next(); // get the first one, which has all auths common to all in the original collection
        Set<String> minimizedUserAuths = StreamSupport.stream(minimized.spliterator(), false).map(String::new).collect(Collectors.toSet());
        if (log.isDebugEnabled()) {
            log.debug("minimizedUserAuths:" + minimizedUserAuths + " with size " + minimizedUserAuths.size());
        }
        Set<Set<String>> powerset = Sets.powerSet(minimizedUserAuths);
        Set<Set<String>> set = Sets.newHashSet();
        for (Set<String> sub : powerset) {
            Set<String> serializableSet = Sets.newHashSet(sub);
            set.add(serializableSet);
        }
        return set;
    }
    
    public Map<Set<String>,TypeMetadata> getTypeMetadataMap() throws TableNotFoundException {
        Collection<Set<String>> powerset = getAllMetadataAuthsPowerSet(this.allMetadataAuths);
        if (log.isTraceEnabled()) {
            log.trace("powerset:" + powerset);
        }
        Map<Set<String>,TypeMetadata> map = Maps.newHashMap();
        
        for (Set<String> a : powerset) {
            if (log.isTraceEnabled()) {
                log.trace("get TypeMetadata with auths:" + a);
            }
            
            Authorizations at = new Authorizations(a.toArray(new String[a.size()]));
            
            if (log.isTraceEnabled()) {
                log.trace("made an Authorizations:" + at);
            }
            TypeMetadata tm = this.allFieldMetadataHelper.getTypeMetadataHelper().getTypeMetadataForAuths(Collections.singleton(at));
            map.put(a, tm);
        }
        return map;
    }
    
    public String getUsersMetadataAuthorizationSubset() {
        StringBuilder buf = new StringBuilder();
        if (this.auths != null && this.allMetadataAuths != null) {
            for (String auth : MetadataHelper.getUsersMetadataAuthorizationSubset(this.auths, this.allMetadataAuths)) {
                if (buf.length() != 0) {
                    buf.append("&");
                }
                buf.append(auth);
            }
        }
        return buf.toString();
    }
    
    public Collection<Authorizations> getAllMetadataAuths() {
        return allMetadataAuths;
    }
    
    public Set<Authorizations> getAuths() {
        return auths;
    }
    
    public Set<Authorizations> getFullUserAuths() {
        return fullUserAuths;
    }
    
    public AllFieldMetadataHelper getAllFieldMetadataHelper() {
        return this.allFieldMetadataHelper;
    }
    
    /**
     * Get the metadata fully populated
     *
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    public Metadata getMetadata() throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        return getMetadata(null);
    }
    
    /**
     * Get the metadata fully populated
     *
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    public Metadata getMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        return new Metadata(this, ingestTypeFilter);
    }
    
    /**
     * Fetch the {@link Set} of all fields contained in the database. This will provide a cached view of the fields which is updated every
     * {@code updateInterval} milliseconds.
     *
     * @return
     * @throws TableNotFoundException
     */
    public Set<String> getAllFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        Multimap<String,String> allFields = this.allFieldMetadataHelper.loadAllFields();
        if (log.isTraceEnabled())
            log.trace("loadAllFields() with auths:" + this.allFieldMetadataHelper.getAuths() + " returned " + allFields);
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(allFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(allFields.get(datatype));
            }
        }
        if (log.isTraceEnabled())
            log.trace("getAllFields(" + ingestTypeFilter + ") returning " + fields);
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get the fields that have values not in the same form as the event (excluding normalization). This would include index only fields, term frequency fields
     * (as the index may contain tokens), and composite fields.
     * 
     * @param ingestTypeFilter
     * @return the non-event fields
     */
    public Set<String> getNonEventFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Set<String> fields = new HashSet<>();
        fields.addAll(getIndexOnlyFields(ingestTypeFilter));
        fields.addAll(getTermFrequencyFields(ingestTypeFilter));
        Multimap<String,String> compToFieldMap = getCompositeToFieldMap(ingestTypeFilter);
        for (String compField : compToFieldMap.keySet()) {
            if (!isOverloadedCompositeField(compToFieldMap, compField)) {
                // a composite is only a non-event field if it is composed from 1 or more non-event fields
                for (String componentField : compToFieldMap.get(compField)) {
                    if (fields.contains(componentField)) {
                        fields.add(compField);
                        break;
                    }
                }
            }
        }
        
        return Collections.unmodifiableSet(fields);
    }
    
    static boolean isOverloadedCompositeField(Multimap<String,String> compositeFieldDefinitions, String compositeFieldName) {
        return isOverloadedCompositeField(compositeFieldDefinitions.get(compositeFieldName), compositeFieldName);
    }
    
    static boolean isOverloadedCompositeField(Collection<String> compFields, String compositeFieldName) {
        if (compFields != null && !compFields.isEmpty())
            return compFields.stream().findFirst().get().equals(compositeFieldName);
        return false;
    }
    
    /**
     * Fetch the {@link Set} of index-only fields.
     * 
     * @return
     * @throws TableNotFoundException
     */
    public Set<String> getIndexOnlyFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> indexOnlyFields = this.allFieldMetadataHelper.getIndexOnlyFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(indexOnlyFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(indexOnlyFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    public QueryModel getQueryModel(String modelTableName, String modelName) throws TableNotFoundException, ExecutionException {
        return getQueryModel(modelTableName, modelName, this.getIndexOnlyFields(null));
    }
    
    public QueryModel getQueryModel(String modelTableName, String modelName, Collection<String> unevaluatedFields) throws TableNotFoundException {
        return getQueryModel(modelTableName, modelName, unevaluatedFields, null);
    }
    
    /***
     * @param modelName
     * @return
     * @throws TableNotFoundException
     */
    public QueryModel getQueryModel(String modelTableName, String modelName, Collection<String> unevaluatedFields, Set<String> ingestTypeFilter)
                    throws TableNotFoundException {
        // Note that we used to cache this, however this method is dependent on some variables in the all fields metadata helper
        // @Cacheable(value = "getQueryModel", key = "{#root.target.auths,#p0,#p1,#p2,#p3}", cacheManager = "metadataHelperCacheManager")
        
        Preconditions.checkNotNull(modelTableName);
        Preconditions.checkNotNull(modelName);
        
        if (log.isTraceEnabled())
            log.trace("getQueryModel(" + modelTableName + "," + modelName + "," + unevaluatedFields + "," + ingestTypeFilter + ")");
        QueryModel queryModel = new QueryModel();
        
        TraceStopwatch stopWatch = new TraceStopwatch("MetadataHelper -- Building Query Model from instance");
        stopWatch.start();
        
        Set<String> unevalFields = null;
        if (log.isTraceEnabled())
            log.trace("using connector: " + connector.getClass().getCanonicalName() + " with auths: " + auths + " and model table name: " + modelTableName
                            + " looking at model " + modelName + " unevaluatedFields " + unevaluatedFields);
        
        Scanner scan = ScannerHelper.createScanner(connector, modelTableName, auths);
        scan.setRange(new Range());
        scan.fetchColumnFamily(new Text(modelName));
        Set<String> indexOnlyFields = new HashSet<>(); // will hold index only
                                                       // fields
        // We need the entire Model so we can do both directions.
        final Set<String> allFields = this.getAllFields(ingestTypeFilter);
        
        for (Map.Entry<Key,Value> entry : scan) {
            String original = entry.getKey().getRow().toString();
            Text cq = entry.getKey().getColumnQualifier();
            String[] parts = StringUtils.split(cq.toString(), "\0");
            if (parts.length > 1 && null != parts[0] && !parts[0].isEmpty()) {
                String replacement = parts[0];
                
                for (String part : parts) {
                    if ("forward".equalsIgnoreCase(part)) {
                        // Do not add a forward mapping entry
                        // when the replacement does not exist in the database
                        if (allFields.contains(replacement)) {
                            queryModel.addTermToModel(original, replacement);
                        } else if (log.isTraceEnabled()) {
                            log.trace("Ignoring forward mapping of " + replacement + " for " + original + " because the metadata table has no reference to it");
                        }
                    } else if ("reverse".equalsIgnoreCase(part)) {
                        queryModel.addTermToReverseModel(original, replacement);
                    } else if ("index_only".equalsIgnoreCase(part)) {
                        indexOnlyFields.add(replacement);
                    }
                }
            }
        }
        
        if (unevaluatedFields != null)
            unevalFields = new HashSet<>(unevaluatedFields);
        else
            unevalFields = new HashSet<>();
        
        unevalFields.addAll(indexOnlyFields);
        queryModel.setUnevaluatedFields(unevalFields);
        
        if (queryModel.getReverseQueryMapping().isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("empty query model for " + this);
            }
            if ("DatawaveMetadata".equals(modelTableName)) {
                log.error("Query Model should not be empty...");
            }
        }
        
        stopWatch.stop();
        
        return queryModel;
    }
    
    /***
     * @param modelTableName
     * @return a list of query model names
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getQueryModelNames", key = "{#root.target.auths,#table}", cacheManager = "metadataHelperCacheManager")
    public Set<String> getQueryModelNames(String modelTableName) throws TableNotFoundException {
        Preconditions.checkNotNull(modelTableName);
        
        if (log.isTraceEnabled())
            log.trace("getQueryModelNames(" + modelTableName + ")");
        
        TraceStopwatch stopWatch = new TraceStopwatch("MetadataHelper -- Getting query model names");
        stopWatch.start();
        
        if (log.isTraceEnabled())
            log.trace("using connector: " + connector.getClass().getCanonicalName() + " with auths: " + auths + " and model table name: " + modelTableName);
        
        Scanner scan = ScannerHelper.createScanner(connector, modelTableName, auths);
        scan.setRange(new Range());
        Set<String> modelNames = new HashSet<>();
        Set<Text> ignoreColfs = new HashSet<>();
        ignoreColfs.addAll(metadataIndexColfs);
        ignoreColfs.addAll(metadataNormalizedColfs);
        ignoreColfs.addAll(metadataTypeColfs);
        ignoreColfs.addAll(metadataCompositeIndexColfs);
        ignoreColfs.addAll(metadataCardinalityColfs);
        ignoreColfs.add(ColumnFamilyConstants.COLF_E);
        ignoreColfs.add(ColumnFamilyConstants.COLF_DESC);
        ignoreColfs.add(ColumnFamilyConstants.COLF_EDGE);
        ignoreColfs.add(ColumnFamilyConstants.COLF_F);
        ignoreColfs.add(ColumnFamilyConstants.COLF_H);
        ignoreColfs.add(ColumnFamilyConstants.COLF_VI);
        ignoreColfs.add(ColumnFamilyConstants.COLF_TF);
        ignoreColfs.add(ColumnFamilyConstants.COLF_VERSION);
        ignoreColfs.add(ColumnFamilyConstants.COLF_EXP);
        
        for (Map.Entry<Key,Value> entry : scan) {
            Text cf = entry.getKey().getColumnFamily();
            if (!ignoreColfs.contains(entry.getKey().getColumnFamily())) {
                if (entry.getKey().getColumnQualifier().toString().endsWith("\0forward")) {
                    modelNames.add(cf.toString());
                }
            }
        }
        
        stopWatch.stop();
        
        return modelNames;
    }
    
    public boolean isReverseIndexed(String fieldName, Set<String> ingestTypeFilter) throws TableNotFoundException {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(ingestTypeFilter);
        
        Entry<String,Entry<String,Set<String>>> entry = Maps.immutableEntry(metadataTableName, Maps.immutableEntry(fieldName, ingestTypeFilter));
        
        try {
            return this.allFieldMetadataHelper.isIndexed(ColumnFamilyConstants.COLF_RI, entry);
        } catch (InstantiationException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    public boolean isIndexed(String fieldName, Set<String> ingestTypeFilter) throws TableNotFoundException {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(ingestTypeFilter);
        
        Entry<String,Entry<String,Set<String>>> entry = Maps.immutableEntry(metadataTableName, Maps.immutableEntry(fieldName, ingestTypeFilter));
        
        try {
            return this.allFieldMetadataHelper.isIndexed(ColumnFamilyConstants.COLF_I, entry);
        } catch (InstantiationException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    /**
     * Returns a Set of all TextNormalizers in use by any type in Accumulo
     * 
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getFacets", key = "{#root.target.auths,#table}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> getFacets(String table) throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getFacets(" + this.auths + "," + table + ")");
        Multimap<String,String> fieldPivots = HashMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(connector, table, auths);
        Range range = new Range();
        
        bs.setRange(range);
        
        bs.fetchColumnFamily(PV);
        
        for (Entry<Key,Value> entry : bs) {
            Key key = entry.getKey();
            
            if (null != key.getRow()) {
                String[] parts = StringUtils.split(key.getRow().toString(), "\0");
                if (parts.length == 2) {
                    fieldPivots.put(parts[0], parts[1]);
                    fieldPivots.put(parts[1], parts[0]);
                    fieldPivots.put(parts[0], parts[0]);
                }
            } else {
                log.warn("Row null in ColumnFamilyConstants for key: " + key);
            }
        }
        
        return fieldPivots;
    }
    
    /**
     * Returns a Set of all counts / cardinalities
     *
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getTermCounts", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Map<String,Map<String,MetadataCardinalityCounts>> getTermCounts() throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getTermCounts(" + this.auths + "," + this.metadataTableName + ")");
        Map<String,Map<String,MetadataCardinalityCounts>> allCounts = Maps.newHashMap();
        
        if (log.isTraceEnabled())
            log.trace("getTermCounts from table: " + metadataTableName);
        
        Scanner bs = ScannerHelper.createScanner(connector, metadataTableName, auths);
        Range range = new Range();
        
        bs.setRange(range);
        
        // Fetch all of the index columns
        for (Text colf : metadataCardinalityColfs) {
            bs.fetchColumnFamily(colf);
        }
        
        try {
            for (Entry<Key,Value> entry : bs) {
                Key key = entry.getKey();
                
                if (null != key.getRow()) {
                    MetadataCardinalityCounts counts = new MetadataCardinalityCounts(key, entry.getValue());
                    Map<String,MetadataCardinalityCounts> values = allCounts.get(counts.getField());
                    if (values == null) {
                        values = Maps.newHashMapWithExpectedSize(5);
                        allCounts.put(counts.getField(), values);
                    }
                    values.put(counts.getFieldValue(), counts);
                } else {
                    log.warn("Row null in ColumnFamilyConstants for key: " + key);
                }
            }
        } finally {
            bs.close();
        }
        
        return Collections.unmodifiableMap(allCounts);
    }
    
    /**
     * Returns a Set of all Counts using the connector's principal's auths. This resulting informations cannot be exposed outside of the system.
     *
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getTermCountsWithRootAuths", key = "{#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Map<String,Map<String,MetadataCardinalityCounts>> getTermCountsWithRootAuths()
                    throws InstantiationException, IllegalAccessException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
        log.debug("cache fault for getTermCounts(" + this.auths + "," + this.metadataTableName + ")");
        Map<String,Map<String,MetadataCardinalityCounts>> allCounts = Maps.newHashMap();
        
        if (log.isTraceEnabled())
            log.trace("getTermCounts from table: " + metadataTableName);
        
        Authorizations rootAuths = connector.securityOperations().getUserAuthorizations(connector.whoami());
        
        Scanner bs = ScannerHelper.createScanner(connector, metadataTableName, Collections.singleton(rootAuths));
        Range range = new Range();
        
        bs.setRange(range);
        
        // Fetch all of the index columns
        for (Text colf : metadataCardinalityColfs) {
            bs.fetchColumnFamily(colf);
        }
        
        try {
            for (Entry<Key,Value> entry : bs) {
                Key key = entry.getKey();
                
                if (null != key.getRow()) {
                    MetadataCardinalityCounts counts = new MetadataCardinalityCounts(key, entry.getValue());
                    Map<String,MetadataCardinalityCounts> values = allCounts.get(counts.getField());
                    if (values == null) {
                        values = Maps.newHashMapWithExpectedSize(5);
                        allCounts.put(counts.getField(), values);
                    }
                    values.put(counts.getFieldValue(), counts);
                } else {
                    log.warn("Row null in ColumnFamilyConstants for key: " + key);
                }
            }
        } finally {
            bs.close();
        }
        
        return Collections.unmodifiableMap(allCounts);
    }
    
    /**
     * Returns a Set of all TextNormalizers in use by any type in Accumulo
     * 
     * @return
     * 
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getAllNormalized", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Set<String> getAllNormalized() throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getAllNormalized(" + this.auths + "," + this.metadataTableName + ")");
        Set<String> normalizedFields = Sets.newHashSetWithExpectedSize(10);
        if (log.isTraceEnabled())
            log.trace("getAllNormalized from table: " + metadataTableName);
        
        Scanner bs = ScannerHelper.createScanner(connector, metadataTableName, auths);
        Range range = new Range();
        
        bs.setRange(range);
        
        // Fetch all of the index columns
        for (Text colf : metadataNormalizedColfs) {
            bs.fetchColumnFamily(colf);
        }
        
        try {
            for (Entry<Key,Value> entry : bs) {
                Key key = entry.getKey();
                
                if (null != key.getRow()) {
                    normalizedFields.add(key.getRow().toString());
                } else {
                    log.warn("Row null in ColumnFamilyConstants for key: " + key);
                }
            }
        } finally {
            bs.close();
        }
        
        return Collections.unmodifiableSet(normalizedFields);
    }
    
    /**
     * Returns a Set of all Types in use by any type in Accumulo
     *
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    public Set<Type<?>> getAllDatatypes() throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return this.allFieldMetadataHelper.getAllDatatypes();
    }
    
    /**
     * A map of composite name to the ordered list of it for example, mapping of {@code COLOR -> ['COLOR_WHEELS,0', 'MAKE_COLOR,1' ]}. If called multiple time,
     * it returns the same cached map.
     * 
     * @return An unmodifiable Multimap
     * @throws TableNotFoundException
     */
    public Multimap<String,String> getCompositeToFieldMap() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeToFieldMap();
    }
    
    public Multimap<String,String> getCompositeToFieldMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        return this.allFieldMetadataHelper.getCompositeToFieldMap(ingestTypeFilter);
    }
    
    /**
     * A map of composite name to transition date.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     */
    public Map<String,Date> getCompositeTransitionDateMap() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeTransitionDateMap();
    }
    
    public Map<String,Date> getCompositeTransitionDateMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeTransitionDateMap(ingestTypeFilter);
    }
    
    /**
     * A map of composite name to field separator.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     */
    public Map<String,String> getCompositeFieldSeparatorMap() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeFieldSeparatorMap();
    }
    
    public Map<String,String> getCompositeFieldSeparatorMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeFieldSeparatorMap(ingestTypeFilter);
    }
    
    /**
     * Fetch the set of {@link Type}s that are configured for this <code>fieldName</code> as specified in the table pointed to by the
     * <code>metadataTableName</code> parameter.
     *
     * @param fieldName
     *            The name of the field to fetch the {@link Type}s for. If null then all dataTypes are returned.
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    public Set<Type<?>> getDatatypesForField(String fieldName) throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return getDatatypesForField(fieldName, null);
    }
    
    /**
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    public Set<Type<?>> getDatatypesForField(String fieldName, Set<String> ingestTypeFilter)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        
        Set<Type<?>> dataTypes = new HashSet<>();
        Multimap<String,Type<?>> mm = this.allFieldMetadataHelper.getFieldsToDatatypes(ingestTypeFilter);
        if (fieldName == null) {
            dataTypes.addAll(mm.values());
        } else {
            Collection<Type<?>> types = mm.asMap().get(fieldName.toUpperCase());
            if (types != null) {
                dataTypes.addAll(types);
            }
        }
        return dataTypes;
    }
    
    public TypeMetadata getTypeMetadata() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getTypeMetadata(null);
    }
    
    public TypeMetadata getTypeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getTypeMetadata(ingestTypeFilter);
    }
    
    public CompositeMetadata getCompositeMetadata() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeMetadata(null);
    }
    
    public CompositeMetadata getCompositeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeMetadata(ingestTypeFilter);
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}.
     *
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    @Cacheable(value = "getEdges", key = "{#root.target.fullUserAuths,#root.target.metadataTableName}")
    public SetMultimap<Key,Value> getEdges() throws TableNotFoundException, ExecutionException {
        log.debug("cache fault for getEdges(" + this.auths + ")");
        SetMultimap<Key,Value> edges = HashMultimap.create();
        if (log.isTraceEnabled())
            log.trace("getEdges from table: " + metadataTableName);
        // unlike other entries, the edges colf entries have many auths set. We'll use the fullUserAuths in the scanner instead
        // of the minimal set in this.auths
        Scanner scanner = ScannerHelper.createScanner(connector, metadataTableName, fullUserAuths);
        
        scanner.setRange(new Range());
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_EDGE);
        
        // First iterator strips the optional attribute2 and attribute3 off the cq, second one
        // combines the protocol buffer data.
        IteratorSetting stripConfig = new IteratorSetting(50, EdgeMetadataCQStrippingIterator.class);
        IteratorSetting combineConfig = new IteratorSetting(51, EdgeMetadataCombiner.class);
        combineConfig.addOption("columns", ColumnFamilyConstants.COLF_EDGE.toString());
        scanner.addScanIterator(stripConfig);
        scanner.addScanIterator(combineConfig);
        
        for (Map.Entry<Key,Value> entry : scanner) {
            edges.put(entry.getKey(), entry.getValue());
        }
        
        return Multimaps.unmodifiableSetMultimap(edges);
    }
    
    /**
     * Fetch the set of {@link Type}s that are configured for this <code>fieldName</code> as specified in the table pointed to by the
     * <code>metadataTableName</code> parameter.
     *
     * @param ingestTypeFilter
     *            Any projection of datatypes to limit the fetch for.
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    public Multimap<String,Type<?>> getFieldsToDatatypes(Set<String> ingestTypeFilter)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return this.allFieldMetadataHelper.getFieldsToDatatypes(ingestTypeFilter);
    }
    
    /**
     * Scans the metadata table and returns the set of fields that use the supplied normalizer.
     *
     * @param datawaveType
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return getFieldsForDatatype(datawaveType, null);
    }
    
    /**
     * Scans the metadata table and returns the set of fields that use the supplied normalizer.
     *
     * This method allows a client to specify data types to filter out. If the set is null, then it assumed the user wants all data types. If the set is empty,
     * then it assumed the user wants no data types. Otherwise, values that occur in the set will be used as a white list of data types.
     *
     * @param datawaveType
     * @param ingestTypeFilter
     * @return
     * @throws TableNotFoundException
     */
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType, Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getFieldsForDatatype(datawaveType, ingestTypeFilter);
    }
    
    /**
     * Pull an instance of the provided normalizer class name from the internal cache.
     *
     * @param datatypeClass
     *            The name of the normalizer class to instantiate.
     * @return An instanace of the normalizer class that was requested.
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public Type<?> getDatatypeFromClass(Class<? extends Type<?>> datatypeClass) throws InstantiationException, IllegalAccessException {
        return this.allFieldMetadataHelper.getDatatypeFromClass(datatypeClass);
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}.
     *
     * @return
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getTermFrequencyFields", key = "{#root.target.auths,#root.target.metadataTableName,#p0}", cacheManager = "metadataHelperCacheManager")
    public Set<String> getTermFrequencyFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> termFrequencyFields = loadTermFrequencyFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(termFrequencyFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(termFrequencyFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get index fields using the data type filter.
     *
     * @param ingestTypeFilter
     * @return
     * @throws TableNotFoundException
     */
    public Set<String> getIndexedFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> indexedFields = this.allFieldMetadataHelper.loadIndexedFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(indexedFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(indexedFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get reverse index fields using the data type filter.
     *
     * @param ingestTypeFilter
     * @return
     * @throws TableNotFoundException
     */
    public Set<String> getReverseIndexedFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> indexedFields = this.allFieldMetadataHelper.loadReverseIndexedFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(indexedFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(indexedFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get expansion fields using the data type filter.
     * 
     * @param ingestTypeFilter
     * @return
     * @throws TableNotFoundException
     */
    public Set<String> getExpansionFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> expansionFields = this.allFieldMetadataHelper.loadExpansionFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(expansionFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(expansionFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get the content fields which are those to be queried when using the content functions.
     * 
     * @param ingestTypeFilter
     * @return
     * @throws TableNotFoundException
     */
    public Set<String> getContentFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> contentFields = this.allFieldMetadataHelper.loadContentFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(contentFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(contentFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Sum all of the frequency counts for a field between a start and end date (inclusive)
     *
     * @param fieldName
     * @param begin
     * @param end
     * @return
     * @throws TableNotFoundException
     */
    public long getCardinalityForField(String fieldName, Date begin, Date end) throws TableNotFoundException {
        return getCardinalityForField(fieldName, (String) null, begin, end);
    }
    
    /**
     * Sum all of the frequency counts for a field in a datatype between a start and end date (inclusive)
     *
     * @param fieldName
     * @param datatype
     * @param begin
     * @param end
     * @return
     * @throws TableNotFoundException
     */
    public long getCardinalityForField(String fieldName, String datatype, Date begin, Date end) throws TableNotFoundException {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(begin);
        Preconditions.checkNotNull(end);
        Preconditions.checkArgument(begin.before(end) || begin.equals(end));
        
        log.trace("getCardinalityForField from table: " + metadataTableName);
        long count = 0;
        
        Text row = new Text(fieldName.toUpperCase());
        DateFrequencyValue serializer = new DateFrequencyValue();
        
        String beginYMD = DateHelper.format(begin);
        String endYMD = DateHelper.format(end);
        
        // Get all the rows in DatawaveMetadata for the field, only in the 'f'
        // colfamily
        try (Scanner bs = ScannerHelper.createScanner(connector, metadataTableName, auths)) {
            
            Key startKey = new Key(row);
            bs.setRange(new Range(startKey, startKey.followingKey(PartialKey.ROW)));
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            if (datatype != null) {
                IteratorSetting setting = new IteratorSetting(30, ColumnSliceFilter.class);
                setting.addOption(ColumnSliceFilter.START_BOUND, datatype + '\0');
                setting.addOption(ColumnSliceFilter.END_BOUND, datatype + '\0' + new String(Character.toChars(Character.MAX_CODE_POINT)));
                bs.addScanIterator(setting);
            }
            
            for (Entry<Key,Value> entry : bs) {
                count += serializer.count(entry.getValue(), beginYMD, true, endYMD, true);
            }
        }
        
        return count;
    }
    
    /**
     * Sum all of the frequency counts for a field in a set of datatypes between a start and end date (inclusive)
     *
     * @param fieldName
     * @param datatypes
     * @param begin
     * @param end
     * @return
     * @throws TableNotFoundException
     */
    public long getCardinalityForField(String fieldName, Set<String> datatypes, Date begin, Date end) throws TableNotFoundException {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(begin);
        Preconditions.checkNotNull(end);
        Preconditions.checkArgument(begin.before(end) || begin.equals(end));
        Preconditions.checkNotNull(datatypes);
        
        log.trace("getCardinalityForField from table: " + metadataTableName);
        
        long count = 0;
        
        Text row = new Text(fieldName.toUpperCase());
        DateFrequencyValue serializer = new DateFrequencyValue();
        
        String beginYMD = DateHelper.format(begin);
        String endYMD = DateHelper.format(end);
        
        // Get all the rows in DatawaveMetadata for the field, only in the 'f'
        // colfamily
        try (Scanner bs = ScannerHelper.createScanner(connector, metadataTableName, auths)) {
            
            Key startKey = new Key(row);
            bs.setRange(new Range(startKey, startKey.followingKey(PartialKey.ROW)));
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            TreeSet<String> sortedSet = new TreeSet(datatypes);
            if (!sortedSet.isEmpty()) {
                IteratorSetting setting = new IteratorSetting(30, ColumnSliceFilter.class);
                setting.addOption(ColumnSliceFilter.START_BOUND, sortedSet.first() + '\0');
                setting.addOption(ColumnSliceFilter.END_BOUND, sortedSet.last() + '\0' + new String(Character.toChars(Character.MAX_CODE_POINT)));
                bs.addScanIterator(setting);
            }
            
            for (Entry<Key,Value> entry : bs) {
                String datatype = getDataTypeFromAggregatedFreqCQ(entry.getKey().getColumnQualifier());
                if (datatypes.contains(datatype)) {
                    count += serializer.count(entry.getValue(), beginYMD, true, endYMD, true);
                }
            }
        }
        
        return count;
    }
    
    public Set<String> getDatatypes(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Set<String> datatypes = this.allFieldMetadataHelper.loadDatatypes();
        if (ingestTypeFilter != null && !ingestTypeFilter.isEmpty()) {
            datatypes = Sets.newHashSet(Sets.intersection(datatypes, ingestTypeFilter));
        }
        
        return Collections.unmodifiableSet(datatypes);
    }
    
    /**
     * Return the sum across all datatypes of the {@link ColumnFamilyConstants#COLF_F} on the given day.
     *
     * @param fieldName
     * @param date
     * @return
     */
    public Long getCountsByFieldInDay(String fieldName, String date) {
        return getCountsByFieldInDayWithTypes(fieldName, date, UniversalSet.instance());
    }
    
    /**
     * Return the sum across all datatypes of the {@link ColumnFamilyConstants#COLF_F} in a given date range.
     *
     * @param fieldName
     * @param begin
     * @param end
     * @return
     */
    public Long getCountsByFieldForDays(String fieldName, Date begin, Date end) {
        return getCountsByFieldForDays(fieldName, begin, end, UniversalSet.instance());
    }
    
    /**
     * Return the sum across all datatypes of the {@link ColumnFamilyConstants#COLF_F} on the given day.
     *
     * @param fieldName
     * @param date
     * @param datatypes
     * @return
     */
    public Long getCountsByFieldInDayWithTypes(String fieldName, String date, final Set<String> datatypes) {
        Date dateDate = DateHelper.parse(date);
        return getCountsByFieldForDays(fieldName, dateDate, dateDate, datatypes);
    }
    
    /**
     * Return the sum across all datatypes of the {@link ColumnFamilyConstants#COLF_F} on the given day.
     *
     * @param fieldName
     * @param begin
     * @param end
     * @param ingestTypeFilter
     * @return
     */
    public Long getCountsByFieldForDays(String fieldName, Date begin, Date end, Set<String> ingestTypeFilter) {
        try {
            return getCardinalityForField(fieldName, ingestTypeFilter, begin, end);
        } catch (TableNotFoundException te) {
            throw new RuntimeException(te);
        }
    }
    
    /**
     * Return a map of sums by datatype of the {@link ColumnFamilyConstants#COLF_F} for the given field and date
     *
     * @param identifier
     * @return
     */
    protected HashMap<String,Long> getCountsByFieldInDayWithTypes(Entry<String,String> identifier) throws TableNotFoundException, IOException {
        String fieldName = identifier.getKey();
        YearMonthDay date = new YearMonthDay(identifier.getValue());
        
        final HashMap<String,Long> datatypeToCounts = Maps.newHashMap();
        DateFrequencyValue dateFrequencyValue = new DateFrequencyValue();
        
        try (Scanner scanner = ScannerHelper.createScanner(connector, metadataTableName, auths)) {
            scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            scanner.setRange(Range.exact(fieldName));
            
            for (Entry<Key,Value> countEntry : scanner) {
                TreeMap<YearMonthDay,Frequency> dateFrequencies = dateFrequencyValue.deserialize(countEntry.getValue());
                Frequency freq = dateFrequencies.get(date);
                if (freq != null && freq.getValue() > 0) {
                    datatypeToCounts.put(getDataTypeFromAggregatedFreqCQ(countEntry.getKey().getColumnQualifier()), new Long(freq.getValue()));
                }
            }
        }
        
        return datatypeToCounts;
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just fieldName. This does not de-duplicate field names
     *
     * @param from
     * @return
     */
    public static Iterable<String> fieldNames(Iterable<MetadataEntry> from) {
        return Iterables.transform(from, toFieldName);
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just fieldName, removing duplicates.
     *
     * @param from
     * @return
     */
    public static Set<String> uniqueFieldNames(Iterable<MetadataEntry> from) {
        return Sets.newHashSet(fieldNames(from));
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just datatype. This does not de-duplicate datatypes
     *
     * @param from
     * @return
     */
    public static Iterable<String> datatypes(Iterable<MetadataEntry> from) {
        return Iterables.transform(from, toDatatype);
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just datatype, removing duplicates.
     *
     * @param from
     * @return
     */
    public static Set<String> uniqueDatatypes(Iterable<MetadataEntry> from) {
        return Sets.newHashSet(datatypes(from));
    }
    
    /**
     * Fetches the first entry from each row in the table. This equates to the set of all fields that have occurred in the database. Returns a multimap of
     * datatype to field
     * 
     * @throws TableNotFoundException
     */
    protected Multimap<String,String> loadAllFields() throws TableNotFoundException {
        Multimap<String,String> fields = HashMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(connector, metadataTableName, auths);
        if (log.isTraceEnabled())
            log.trace("loadAllFields from table: " + metadataTableName);
        
        bs.setRange(new Range());
        
        // We don't want to fetch all columns because that could include model
        // field names
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_T);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_RI);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_TF);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CI);
        
        Iterator<Entry<Key,Value>> iterator = bs.iterator();
        
        while (iterator.hasNext()) {
            Entry<Key,Value> entry = iterator.next();
            Key k = entry.getKey();
            
            String fieldname = k.getRow().toString();
            String datatype = getDatatype(k);
            
            fields.put(datatype, fieldname);
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetches results from metadata table and calculates the set of fieldNames which are indexed but do not appear as an attribute on the Event Returns a
     * multimap of datatype to field
     * 
     * @throws TableNotFoundException
     */
    protected Multimap<String,String> loadIndexOnlyFields() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getIndexOnlyFields();
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}. Returns a multimap of datatype to
     * field
     *
     * @return
     * @throws TableNotFoundException
     */
    protected Multimap<String,String> loadTermFrequencyFields() throws TableNotFoundException {
        Multimap<String,String> fields = HashMultimap.create();
        if (log.isTraceEnabled())
            log.trace("loadTermFrequencyFields from table: " + metadataTableName);
        // Scanner to the provided metadata table
        Scanner bs = ScannerHelper.createScanner(connector, metadataTableName, auths);
        
        bs.setRange(new Range());
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_TF);
        
        for (Entry<Key,Value> entry : bs) {
            fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    private static String getKey(Instance instance, String metadataTableName) {
        StringBuilder builder = new StringBuilder();
        builder.append(instance != null ? instance.getInstanceID() : null).append('\0');
        builder.append(metadataTableName).append('\0');
        return builder.toString();
    }
    
    private static String getKey(MetadataHelper helper) {
        return getKey(helper.instance, helper.metadataTableName);
    }
    
    @Override
    public String toString() {
        return getKey(this);
    }
    
    public static void basicIterator(Connector connector, String tableName, Collection<Authorizations> auths)
                    throws TableNotFoundException, InvalidProtocolBufferException {
        if (log.isTraceEnabled())
            log.trace("--- basicIterator ---" + tableName);
        Scanner scanner = connector.createScanner(tableName, auths.iterator().next());
        Range range = new Range();
        scanner.setRange(range);
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        while (iter.hasNext()) {
            Entry<Key,Value> entry = iter.next();
            Key k = entry.getKey();
            if (log.isTraceEnabled())
                log.trace("Key: " + k);
        }
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
}
