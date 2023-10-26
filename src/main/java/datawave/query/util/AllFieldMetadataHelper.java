package datawave.query.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.tdunning.math.stats.Sort;

import datawave.data.ColumnFamilyConstants;
import datawave.data.type.Type;
import datawave.query.composite.CompositeMetadata;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.FieldIndexHole;
import datawave.security.util.AuthorizationsMinimizer;
import datawave.security.util.ScannerHelper;
import datawave.util.time.DateHelper;

@EnableCaching
@Component("allFieldMetadataHelper")
@Scope("prototype")
public class AllFieldMetadataHelper {
    private static final Logger log = LoggerFactory.getLogger(AllFieldMetadataHelper.class);
    
    public static final String NULL_BYTE = "\0";
    
    protected static final Function<MetadataEntry,String> toFieldName = new MetadataEntryToFieldName(), toDatatype = new MetadataEntryToDatatype();
    
    protected final Metadata metadata = new Metadata();
    
    protected final List<Text> metadataIndexColfs = Arrays.asList(ColumnFamilyConstants.COLF_I, ColumnFamilyConstants.COLF_RI);
    protected final List<Text> metadataNormalizedColfs = Arrays.asList(ColumnFamilyConstants.COLF_N);
    protected final List<Text> metadataTypeColfs = Arrays.asList(ColumnFamilyConstants.COLF_T);
    protected final List<Text> metadataCompositeIndexColfs = Arrays.asList(ColumnFamilyConstants.COLF_CI);
    
    protected final AccumuloClient accumuloClient;
    protected final String metadataTableName;
    protected final Set<Authorizations> auths;
    protected final Set<Authorizations> fullUserAuths;
    
    protected final TypeMetadataHelper typeMetadataHelper;
    protected final CompositeMetadataHelper compositeMetadataHelper;
    
    /**
     * Initializes the instance with a provided update interval.
     *
     * @param client
     *            A client connection to Accumulo
     * @param metadataTableName
     *            The name of the DatawaveMetadata table
     * @param auths
     *            Any {@link Authorizations} to use
     */
    public AllFieldMetadataHelper(TypeMetadataHelper typeMetadataHelper, CompositeMetadataHelper compositeMetadataHelper, AccumuloClient client,
                    String metadataTableName, Set<Authorizations> auths, Set<Authorizations> fullUserAuths) {
        Preconditions.checkNotNull(typeMetadataHelper, "A TypeMetadataHelper is required by AllFieldMetadataHelper");
        this.typeMetadataHelper = typeMetadataHelper;
        
        Preconditions.checkNotNull(compositeMetadataHelper, "A CompositeMetadataHelper is required by AllFieldMetadataHelper");
        this.compositeMetadataHelper = compositeMetadataHelper;
        
        Preconditions.checkNotNull(client, "A valid AccumuloClient is required by AllFieldMetadataHelper");
        this.accumuloClient = client;
        
        Preconditions.checkNotNull(metadataTableName, "The name of the metadata table is required by AllFieldMetadataHelper");
        this.metadataTableName = metadataTableName;
        
        Preconditions.checkNotNull(auths, "Authorizations are required by AllFieldMetadataHelper");
        this.auths = auths;
        
        Preconditions.checkNotNull(fullUserAuths, "The full set of user authorizations is required by AllFieldMetadataHelper");
        this.fullUserAuths = fullUserAuths;
        
        log.trace("Constructor  connector: {} and metadata table name: {}", accumuloClient.getClass().getCanonicalName(), metadataTableName);
    }
    
    protected String getDatatype(Key k) {
        String datatype = k.getColumnQualifier().toString();
        int index = datatype.indexOf('\0');
        if (index >= 0) {
            datatype = datatype.substring(0, index);
        }
        return datatype;
    }
    
    protected String getCompositeFieldName(Key k) {
        Text colq = k.getColumnQualifier();
        String compositeFieldName = k.getColumnQualifier().toString();
        int index = compositeFieldName.indexOf('\0');
        if (index >= 0) {
            compositeFieldName = compositeFieldName.substring(index + 1);
            index = compositeFieldName.indexOf(',');
            if (index != -1) {
                compositeFieldName = compositeFieldName.substring(0, index);
            }
        }
        return compositeFieldName;
    }
    
    public Set<Authorizations> getAuths() {
        return auths;
    }
    
    public Set<Authorizations> getFullUserAuths() {
        return fullUserAuths;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public TypeMetadataHelper getTypeMetadataHelper() {
        return typeMetadataHelper;
    }
    
    /**
     * Method that determines whether or not a column exists in the metadata table for the given key.
     * 
     * @param colf
     * @param key
     * @return
     * @throws TableNotFoundException
     * @throws InstantiationException
     * @throws ExecutionException
     */
    @Cacheable(value = "isIndexed", key = "{#root.target.auths,#root.target.metadataTableName,#colf,#key}", cacheManager = "metadataHelperCacheManager",
                    sync = true)
    // using cache with higher maximumSize
    public Boolean isIndexed(Text colf, Entry<String,Entry<String,Set<String>>> key) throws TableNotFoundException, InstantiationException, ExecutionException {
        log.debug("cache fault for isIndexed(" + this.auths + "," + this.metadataTableName + "," + colf + "," + key + ")");
        Preconditions.checkNotNull(key);
        
        final String tableName = key.getKey();
        final String fieldName = key.getValue().getKey();
        final Set<String> datatype = key.getValue().getValue();
        
        Preconditions.checkNotNull(fieldName);
        
        // FieldNames are "normalized" to be all upper case
        String upCaseFieldName = fieldName.toUpperCase();
        
        // Scanner to the provided metadata table
        Scanner scanner = ScannerHelper.createScanner(accumuloClient, tableName, auths);
        
        Range range = new Range(upCaseFieldName);
        scanner.setRange(range);
        scanner.fetchColumnFamily(colf);
        
        boolean result = false;
        for (Entry<Key,Value> entry : scanner) {
            // Get the column qualifier from the key. It contains the ingesttype
            // and datatype class
            if (null != entry.getKey().getColumnQualifier()) {
                String colq = entry.getKey().getColumnQualifier().toString();
                
                // there should not be a null byte and Normalizer class in the 'i' entry for version3+
                int idx = colq.indexOf(NULL_BYTE);
                if (idx != -1) {
                    colq = colq.substring(0, idx);
                }
                
                // If types are specified and this type is not in the list,
                // skip it.
                if (datatype == null || datatype.isEmpty() || datatype.contains(colq)) {
                    result = true;
                    break;
                }
            } else {
                log.warn("ColumnQualifier null in ColumnFamilyConstants for key: " + entry.getKey());
            }
        }
        return result;
    }
    
    /**
     * Returns a Set of all Types in use by any type in Accumulo
     * 
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getAllDatatypes", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Set<Type<?>> getAllDatatypes() throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getAllDatatypes(" + this.auths + "," + this.metadataTableName + ")");
        Set<Type<?>> datatypes = Sets.newHashSetWithExpectedSize(10);
        if (log.isTraceEnabled())
            log.trace("getAllDatatypes from table: " + metadataTableName);
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        Range range = new Range();
        
        bs.setRange(range);
        
        // Fetch all of the index columns
        for (Text colf : metadataTypeColfs) {
            bs.fetchColumnFamily(colf);
        }
        
        for (Entry<Key,Value> entry : bs) {
            Key key = entry.getKey();
            
            // Get the column qualifier from the key. It contains the
            // datatype and normalizer class
            if (null != key.getColumnQualifier()) {
                String colq = key.getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                if (idx != -1) {
                    try {
                        @SuppressWarnings("unchecked")
                        Class<? extends Type<?>> clazz = (Class<? extends Type<?>>) Class.forName(colq.substring(idx + 1));
                        
                        datatypes.add(getDatatypeFromClass(clazz));
                    } catch (ClassNotFoundException e) {
                        log.error("Unable to find normalizer on class path: " + colq.substring(idx + 1), e);
                    }
                } else {
                    log.warn("ColumnFamilyConstants entry did not contain a null byte in the column qualifier: " + key);
                    
                }
            } else {
                log.warn("ColumnQualifier null in EventMetadata for key: " + key);
            }
        }
        
        return Collections.unmodifiableSet(datatypes);
        
    }
    
    /**
     * A map of composite name to the ordered list of it for example, mapping of {@code COLOR -> ['COLOR_WHEELS', 'MAKE_COLOR' ]}. If called multiple time, it
     * returns the same cached map.
     * 
     * @return An unmodifiable Multimap
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getCompositeToFieldMap", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> getCompositeToFieldMap() throws TableNotFoundException {
        log.debug("cache fault for getCompositeToFieldMap(" + this.auths + "," + this.metadataTableName + ")");
        return this.getCompositeToFieldMap(null);
    }
    
    @Cacheable(value = "getCompositeToFieldMap", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> getCompositeToFieldMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getCompositeToFieldMap(" + this.auths + "," + this.metadataTableName + "," + ingestTypeFilter + ")");
        
        ArrayListMultimap<String,String> compositeToFieldMap = ArrayListMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        Range range = new Range();
        
        bs.setRange(range);
        
        // Fetch all of the index columns
        for (Text colf : this.metadataCompositeIndexColfs) {
            bs.fetchColumnFamily(colf);
        }
        
        for (Entry<Key,Value> entry : bs) {
            String fieldName = entry.getKey().getRow().toString();
            if (null != entry.getKey().getColumnQualifier()) {
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                String type = colq.substring(0, idx);
                
                // If types are specified and this type is not in the list,
                // skip it.
                if (null != ingestTypeFilter && !ingestTypeFilter.isEmpty() && !ingestTypeFilter.contains(type)) {
                    continue;
                }
                
                if (idx != -1) {
                    String[] componentFields = colq.substring(idx + 1).split(",");
                    compositeToFieldMap.putAll(fieldName, Arrays.asList(componentFields));
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey());
                }
            } else {
                log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey());
            }
        }
        
        return Multimaps.unmodifiableMultimap(compositeToFieldMap);
    }
    
    /**
     * A map of composite name to transition date.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getCompositeTransitionDateMap", key = "{#root.target.auths,#root.target.metadataTableName}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,Date> getCompositeTransitionDateMap() throws TableNotFoundException {
        log.debug("cache fault for getCompositeTransitionDateMap(" + this.auths + "," + this.metadataTableName + ")");
        return this.getCompositeTransitionDateMap(null);
    }
    
    @Cacheable(value = "getCompositeTransitionDateMap", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,Date> getCompositeTransitionDateMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getCompositeTransitionDateMap(" + this.auths + "," + this.metadataTableName + "," + ingestTypeFilter + ")");
        
        Map<String,Date> tdMap = new HashMap<>();
        
        SimpleDateFormat dateFormat = new SimpleDateFormat(CompositeMetadataHelper.transitionDateFormat);
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        Range range = new Range();
        
        bs.setRange(range);
        
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CITD);
        
        for (Entry<Key,Value> entry : bs) {
            String fieldName = entry.getKey().getRow().toString();
            if (null != entry.getKey().getColumnQualifier()) {
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                String type = colq.substring(0, idx);
                
                // If types are specified and this type is not in the list,
                // skip it.
                if (null != ingestTypeFilter && !ingestTypeFilter.isEmpty() && !ingestTypeFilter.contains(type)) {
                    continue;
                }
                
                if (idx != -1) {
                    try {
                        Date transitionDate = dateFormat.parse(colq.substring(idx + 1));
                        tdMap.put(fieldName, transitionDate);
                    } catch (ParseException e) {
                        log.trace("Unable to parse composite field transition date", e);
                    }
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey());
                }
            } else {
                log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey());
            }
        }
        
        return Collections.unmodifiableMap(tdMap);
    }
    
    /**
     * A map of whindex field to creation date.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getWhindexCreationDateMap", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Map<String,Date> getWhindexCreationDateMap() throws TableNotFoundException {
        log.debug("cache fault for getWhindexCreationDateMap(" + this.auths + "," + this.metadataTableName + ")");
        return this.getWhindexCreationDateMap(null);
    }
    
    @Cacheable(value = "getWhindexCreationDateMap", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,Date> getWhindexCreationDateMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getWhindexCreationDateMap(" + this.auths + "," + this.metadataTableName + "," + ingestTypeFilter + ")");
        
        Map<String,Date> tdMap = new HashMap<>();
        
        // Note: Intentionally using the same transition date format as the composite fields.
        SimpleDateFormat dateFormat = new SimpleDateFormat(CompositeMetadataHelper.transitionDateFormat);
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        Range range = new Range();
        
        bs.setRange(range);
        
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_WCD);
        
        for (Entry<Key,Value> entry : bs) {
            String fieldName = entry.getKey().getRow().toString();
            if (null != entry.getKey().getColumnQualifier()) {
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                if (idx != -1) {
                    String type = colq.substring(0, idx);
                    
                    // If types are specified and this type is not in the list,
                    // skip it.
                    if (null != ingestTypeFilter && !ingestTypeFilter.isEmpty() && !ingestTypeFilter.contains(type)) {
                        continue;
                    }
                    
                    try {
                        Date transitionDate = dateFormat.parse(colq.substring(idx + 1));
                        tdMap.put(fieldName, transitionDate);
                    } catch (ParseException e) {
                        log.trace("Unable to parse whindex field creation date", e);
                    }
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey());
                }
            } else {
                log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey());
            }
        }
        
        return Collections.unmodifiableMap(tdMap);
    }
    
    /**
     * A map of composite name to field separator.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getCompositeFieldSeparatorMap", key = "{#root.target.auths,#root.target.metadataTableName}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,String> getCompositeFieldSeparatorMap() throws TableNotFoundException {
        log.debug("cache fault for getCompositeFieldSeparatorMap(" + this.auths + "," + this.metadataTableName + ")");
        return this.getCompositeFieldSeparatorMap(null);
    }
    
    @Cacheable(value = "getCompositeFieldSeparatorMap", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,String> getCompositeFieldSeparatorMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getCompositeFieldSeparatorMap(" + this.auths + "," + this.metadataTableName + "," + ingestTypeFilter + ")");
        
        Map<String,String> sepMap = new HashMap<>();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        Range range = new Range();
        
        bs.setRange(range);
        
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CISEP);
        
        for (Entry<Key,Value> entry : bs) {
            String fieldName = entry.getKey().getRow().toString();
            if (null != entry.getKey().getColumnQualifier()) {
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                String type = colq.substring(0, idx);
                
                // If types are specified and this type is not in the list,
                // skip it.
                if (null != ingestTypeFilter && ingestTypeFilter.size() > 0 && !ingestTypeFilter.contains(type)) {
                    continue;
                }
                
                if (idx != -1) {
                    String separator = colq.substring(idx + 1);
                    sepMap.put(fieldName, separator);
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey().toString());
                }
            } else {
                log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey().toString());
            }
        }
        
        return Collections.unmodifiableMap(sepMap);
    }
    
    public TypeMetadata getTypeMetadata() throws TableNotFoundException {
        return this.typeMetadataHelper.getTypeMetadata(null);
    }
    
    public TypeMetadata getTypeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.typeMetadataHelper.getTypeMetadata(ingestTypeFilter);
    }
    
    public CompositeMetadata getCompositeMetadata() throws TableNotFoundException {
        return this.compositeMetadataHelper.getCompositeMetadata(null);
    }
    
    public CompositeMetadata getCompositeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.compositeMetadataHelper.getCompositeMetadata(ingestTypeFilter);
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
    @Cacheable(value = "getFieldsToDatatypes", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Multimap<String,Type<?>> getFieldsToDatatypes(Set<String> ingestTypeFilter)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getFieldsToDatatypes(" + this.auths + "," + this.metadataTableName + "," + ingestTypeFilter + ")");
        TypeMetadata typeMetadata = this.typeMetadataHelper.getTypeMetadata(ingestTypeFilter);
        Multimap<String,Type<?>> typeMap = HashMultimap.create();
        for (Entry<String,String> entry : typeMetadata.fold().entries()) {
            String value = entry.getValue();
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Type<?>> clazz = (Class<? extends Type<?>>) Class.forName(value);
                typeMap.put(entry.getKey(), getDatatypeFromClass(clazz));
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                log.error("Unable to find datatype on class path: " + value, e);
            }
            
        }
        return typeMap;
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
    @Cacheable(value = "getFieldsForDatatype", key = "{#root.target.auths,#root.target.metadataTableName,#datawaveType}",
                    cacheManager = "metadataHelperCacheManager")
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getFieldsForDatatype(" + datawaveType + ")");
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
    @Cacheable(value = "getFieldsForDatatype", key = "{#root.target.auths,#root.target.metadataTableName,#datawaveType,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType, Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getFieldsForDatatype(" + datawaveType + "," + ingestTypeFilter + ")");
        TypeMetadata typeMetadata = this.typeMetadataHelper.getTypeMetadata(ingestTypeFilter);
        String datawaveTypeClassName = datawaveType.getName();
        
        // datatype class name to field name <--field name to datatype class name
        Multimap<String,String> inverted = Multimaps.invertFrom(typeMetadata.fold(ingestTypeFilter), HashMultimap.<String,String> create());
        
        return Sets.newHashSet(inverted.get(datawaveTypeClassName));
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
    protected Type<?> getDatatypeFromClass(Class<? extends Type<?>> datatypeClass) throws InstantiationException, IllegalAccessException {
        return datatypeClass.newInstance();
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}.
     * 
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    public Set<String> getTermFrequencyFields(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException {
        
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
     * Get expansion fields using the data type filter.
     * 
     * @param ingestTypeFilter
     * @return
     * @throws TableNotFoundException
     */
    public Set<String> getExpansionFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> expansionFields = loadExpansionFields();
        
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
        
        Multimap<String,String> contentFields = loadContentFields();
        
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
    
    public Set<String> getDatatypes(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Set<String> datatypes = loadDatatypes();
        if (ingestTypeFilter != null && !ingestTypeFilter.isEmpty()) {
            datatypes = Sets.newHashSet(Sets.intersection(datatypes, ingestTypeFilter));
        }
        
        return Collections.unmodifiableSet(datatypes);
    }
    
    protected HashMap<String,Long> getCountsByFieldInDayWithTypes(Entry<String,String> identifier) throws TableNotFoundException, IOException {
        String fieldName = identifier.getKey();
        String date = identifier.getValue();
        
        Scanner scanner = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
        scanner.setRange(Range.exact(fieldName));
        
        IteratorSetting cqRegex = new IteratorSetting(50, RegExFilter.class);
        RegExFilter.setRegexs(cqRegex, null, null, ".*\u0000" + date, null, false);
        scanner.addScanIterator(cqRegex);
        
        final Text holder = new Text();
        final HashMap<String,Long> datatypeToCounts = Maps.newHashMap();
        for (Entry<Key,Value> countEntry : scanner) {
            ByteArrayInputStream bais = new ByteArrayInputStream(countEntry.getValue().get());
            DataInputStream inputStream = new DataInputStream(bais);
            
            Long sum = WritableUtils.readVLong(inputStream);
            
            countEntry.getKey().getColumnQualifier(holder);
            int offset = holder.find(NULL_BYTE);
            
            Preconditions.checkArgument(-1 != offset, "Could not find nullbyte separator in column qualifier for: " + countEntry.getKey());
            
            String datatype = Text.decode(holder.getBytes(), 0, offset);
            
            datatypeToCounts.put(datatype, sum);
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
     * Fetches the first entry from each row in the {@link #metadataTableName} table. This equates to the set of all fields that have occurred in the database.
     * Returns a multimap of datatype to field
     * 
     * @throws TableNotFoundException
     */
    @Cacheable(value = "loadAllFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadAllFields() throws TableNotFoundException {
        log.debug("cache fault for loadAllFields(" + this.auths + "," + this.metadataTableName + ")");
        if (log.isTraceEnabled()) {
            log.trace("Using these minimized auths:" + AuthorizationsMinimizer.minimize(this.auths).iterator().next());
        }
        Multimap<String,String> fields = HashMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
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
     * Fetches results from {@link #metadataTableName} and calculates the set of fieldNames which are indexed but do not appear as an attribute on the Event
     * Returns a multimap of datatype to field
     * 
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getIndexOnlyFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> getIndexOnlyFields() throws TableNotFoundException {
        log.debug("cache fault for getIndexOnlyFields(" + this.auths + "," + this.metadataTableName + ")");
        Multimap<String,String> fields = HashMultimap.create();
        
        final Map<String,Multimap<Text,Text>> metadata = new HashMap<>();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        if (log.isTraceEnabled())
            log.trace("loadIndexOnlyFields from table: " + metadataTableName);
        
        // Fetch the 'e' and 'i' columns
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CI);
        
        // For all keys in the DatawaveMetadata table
        bs.setRange(new Range());
        
        Iterator<Entry<Key,Value>> iterator = bs.iterator();
        Set<String> compositeFields = Sets.newHashSet();
        // Collect the results and put them into a Multimap
        while (iterator.hasNext()) {
            Entry<Key,Value> entry = iterator.next();
            Key k = entry.getKey();
            Text fieldName = k.getRow();
            Text fieldType = k.getColumnFamily();
            String dataType = getDatatype(k);
            if (fieldType.equals(ColumnFamilyConstants.COLF_CI)) {
                compositeFields.add(getCompositeFieldName(k));
            }
            
            Multimap<Text,Text> md = metadata.get(dataType);
            if (md == null) {
                md = HashMultimap.create();
                metadata.put(dataType, md);
                
            }
            md.put(fieldName, fieldType);
        }
        
        // Find all of the fields which only have the 'i' column
        for (String dataType : metadata.keySet()) {
            for (Text fieldName : metadata.get(dataType).keySet()) {
                Collection<Text> columns = metadata.get(dataType).get(fieldName);
                
                if (1 == columns.size()) {
                    Text c = columns.iterator().next();
                    
                    if (c.equals(ColumnFamilyConstants.COLF_I)) {
                        if (compositeFields.contains(fieldName.toString()) == false) {
                            fields.put(dataType, fieldName.toString());
                        }
                    }
                }
            }
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}. Returns a multimap of datatype to
     * field
     * 
     * @return
     * @throws TableNotFoundException
     */
    @Cacheable(value = "loadTermFrequencyFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadTermFrequencyFields() throws TableNotFoundException {
        log.debug("cache fault for loadTermFrequencyFields(" + this.auths + "," + this.metadataTableName + ")");
        Multimap<String,String> fields = HashMultimap.create();
        if (log.isTraceEnabled())
            log.trace("loadTermFrequencyFields from table: " + metadataTableName);
        // Scanner to the provided metadata table
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        bs.setRange(new Range());
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_TF);
        
        for (Entry<Key,Value> entry : bs) {
            fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as being indexed, {@link ColumnFamilyConstants#COLF_I}. Returns a multimap of datatype to field
     * 
     * @return
     * @throws TableNotFoundException
     */
    @Cacheable(value = "loadIndexedFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadIndexedFields() throws TableNotFoundException {
        log.debug("cache fault for loadIndexedFields(" + this.auths + "," + this.metadataTableName + ")");
        Multimap<String,String> fields = HashMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        bs.setRange(new Range());
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
        
        if (log.isTraceEnabled())
            log.trace("loadIndexedFields from table: " + metadataTableName);
        
        for (Entry<Key,Value> entry : bs) {
            fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as being reverse indexed, {@link ColumnFamilyConstants#COLF_RI}. Returns a multimap of datatype to field
     *
     * @return
     * @throws TableNotFoundException
     */
    @Cacheable(value = "loadReverseIndexedFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadReverseIndexedFields() throws TableNotFoundException {
        log.debug("cache fault for loadReverseIndexedFields(" + this.auths + "," + this.metadataTableName + ")");
        Multimap<String,String> fields = HashMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        bs.setRange(new Range());
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_RI);
        
        if (log.isTraceEnabled())
            log.trace("loadReverseIndexedFields from table: " + metadataTableName);
        
        for (Entry<Key,Value> entry : bs) {
            fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as being indexed, {@link ColumnFamilyConstants#COLF_I}. Returns a multimap of datatype to field
     * 
     * @return
     * @throws TableNotFoundException
     */
    @Cacheable(value = "loadIndexedFields", key = "{#root.target.fullUserAuths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadAllIndexedFields() throws TableNotFoundException {
        log.debug("cache fault for loadIndexedFields(" + this.auths + "," + this.metadataTableName + ")");
        Multimap<String,String> fields = HashMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, fullUserAuths);
        
        bs.setRange(new Range());
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
        
        if (log.isTraceEnabled())
            log.trace("loadIndexedFields from table: " + metadataTableName);
        
        for (Entry<Key,Value> entry : bs) {
            fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as being expansion fields, {@link ColumnFamilyConstants#COLF_EXP}. Returns a multimap of datatype to field
     * 
     * @return
     * @throws TableNotFoundException
     */
    @Cacheable(value = "loadExpansionFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadExpansionFields() throws TableNotFoundException {
        log.debug("cache fault for loadExpansionFields(" + this.auths + "," + this.metadataTableName + ")");
        Multimap<String,String> fields = HashMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        bs.setRange(new Range());
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_EXP);
        
        if (log.isTraceEnabled())
            log.trace("loadExpansionFields from table: " + metadataTableName);
        
        for (Entry<Key,Value> entry : bs) {
            fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the set of all fields marked as being content fields, {@link ColumnFamilyConstants#COLF_CONTENT}. Returns a multimap of datatype to field
     * 
     * @return
     * @throws TableNotFoundException
     */
    @Cacheable(value = "loadContentFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadContentFields() throws TableNotFoundException {
        log.debug("cache fault for loadContentFields(" + this.auths + "," + this.metadataTableName + ")");
        Multimap<String,String> fields = HashMultimap.create();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        bs.setRange(new Range());
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CONTENT);
        
        if (log.isTraceEnabled())
            log.trace("loadContentFields from table: " + metadataTableName);
        
        for (Entry<Key,Value> entry : bs) {
            fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all datatypes that appear in the DatawaveMetadata table.
     * 
     * By scanning for all {@link ColumnFamilyConstants#COLF_E}, we will find all of the datatypes currently ingested by virtue that a datatype must have at
     * least one field that appears in an event.
     * 
     * @throws TableNotFoundException
     */
    @Cacheable(value = "loadDatatypes", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Set<String> loadDatatypes() throws TableNotFoundException {
        log.debug("cache fault for loadDatatypes(" + this.auths + "," + this.metadataTableName + ")");
        if (log.isTraceEnabled())
            log.trace("loadDatatypes from table: " + metadataTableName);
        HashSet<String> datatypes = new HashSet<>();
        final Text holder = new Text();
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        bs.setRange(new Range());
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
        
        for (Entry<Key,Value> entry : bs) {
            entry.getKey().getColumnQualifier(holder);
            
            datatypes.add(holder.toString());
        }
        
        return Collections.unmodifiableSet(datatypes);
    }
    
    /**
     * Fetches results from {@link #metadataTableName} and calculates the set of field index holes that exists for all indexed entries.
     * 
     * @return a map of field names and datatype pairs to field index holes
     */
    @Cacheable(value = "getFieldIndexHoles", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Map<Pair<String,String>,FieldIndexHole> getFieldIndexHoles() throws TableNotFoundException, CharacterCodingException {
        return getFieldIndexHoles(ColumnFamilyConstants.COLF_I);
    }
    
    /**
     * Fetches results from {@link #metadataTableName} and calculates the set of field index holes that exists for all reversed indexed entries.
     * 
     * @return a map of field names and datatype pairs to field index holes
     */
    @Cacheable(value = "getReversedFieldIndexHoles", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Map<Pair<String,String>,FieldIndexHole> getReversedFieldIndexHoles() throws TableNotFoundException, CharacterCodingException {
        return getFieldIndexHoles(ColumnFamilyConstants.COLF_RI);
    }
    
    /**
     * Supplies field index hole for {@link #getFieldIndexHoles()} and {@link #getReversedFieldIndexHoles()}.
     */
    private Map<Pair<String,String>,FieldIndexHole> getFieldIndexHoles(Text indexColumnFamily) throws TableNotFoundException {
        log.debug("cache fault for getFieldIndexHoles(" + this.auths + "," + this.metadataTableName + ")");
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        // Fetch the frequency column and the specified index column.
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
        bs.fetchColumnFamily(indexColumnFamily);
        
        // For all keys in the DatawaveMetadata table.
        bs.setRange(new Range());
        
        // We must first scan over all fieldName-datatype combinations and extract the date ranges in which we've seen them. Each date range represents a span
        // of time when we saw an event for each day in that date range, from the start to end (inclusive).
        Map<Pair<String,String>,SortedSet<Pair<Date,Date>>> frequencyMap = new HashMap<>();
        Map<Pair<String,String>,SortedSet<Pair<Date,Date>>> indexMap = new HashMap<>();
        Calendar calendar = Calendar.getInstance();
        
        String prevFieldName = null;
        String prevDatatype = null;
        Date prevDate = null;
        Date startDate = null;
        Text prevColumnFamily = null;
        // Points to the target map object that we add date ranges to. This changes when we see a different column family compared to the previous row.
        Map<Pair<String,String>,SortedSet<Pair<Date,Date>>> dateMap = frequencyMap;
        
        // Scan each row and extract the date ranges.
        for (Entry<Key,Value> entry : bs) {
            Key key = entry.getKey();
            String fieldName = key.getRow().toString();
            Text columnFamily = key.getColumnFamily();
            
            // Parse the data type and event date from the column qualifier.
            String cq = key.getColumnQualifier().toString();
            int offset = cq.indexOf(NULL_BYTE);
            String datatype = cq.substring(0, offset);
            Date date = DateHelper.parse(cq.substring((offset + 1)));
            
            // If this is the very first entry we've seen, update the tracking variables and continue to the next entry.
            if (prevFieldName == null) {
                prevFieldName = fieldName;
                prevDatatype = datatype;
                prevDate = date;
                startDate = date;
                prevColumnFamily = columnFamily;
                continue;
            }
            
            // If the column family is different, record the last date range, and begin collecting date ranges for the next batch of related rows.
            if (!prevColumnFamily.equals(columnFamily)) {
                // We've encountered a new fieldName-datatype combination. Add the latest date range seen for the previous fieldName-datatype combination.
                Pair<Date,Date> dateRange = Pair.of(startDate, prevDate);
                SortedSet<Pair<Date,Date>> dates = dateMap.computeIfAbsent(Pair.of(prevFieldName, prevDatatype), (k) -> new TreeSet<>());
                dates.add(dateRange);
                
                // Update our tracking variables.
                prevFieldName = fieldName;
                prevDatatype = datatype;
                startDate = date;
                prevDate = date;
                
                // Change which map dateMap points to based on the column family.
                if (key.getColumnFamily().equals(ColumnFamilyConstants.COLF_F)) {
                    dateMap = frequencyMap;
                } else {
                    dateMap = indexMap;
                }
            } else {
                // We're on the same fieldName-datatype combination as the previous entry. Compare the dates and determine if we need to start a new date range.
                if (fieldName.equals(prevFieldName) && datatype.equals(prevDatatype)) {
                    calendar.setTime(prevDate);
                    calendar.add(Calendar.DATE, 1);
                    // If the current date is one day after the previous date, it falls within the current date range. Update our tracking variables and
                    // continue.
                    if (!calendar.getTime().equals(date)) {
                        // The current date should not be included in the current date range. Add the current date range, and start a new one.
                        Pair<Date,Date> dateRange = Pair.of(startDate, prevDate);
                        SortedSet<Pair<Date,Date>> dates = dateMap.computeIfAbsent(Pair.of(prevFieldName, prevDatatype), (k) -> new TreeSet<>());
                        dates.add(dateRange);
                        
                        // Update our tracking variables.
                        startDate = date;
                    }
                } else {
                    // We've encountered a new fieldName-datatype combination. Add the latest date range seen for the previous fieldName-datatype combination.
                    Pair<Date,Date> dateRange = Pair.of(startDate, prevDate);
                    SortedSet<Pair<Date,Date>> dates = dateMap.computeIfAbsent(Pair.of(prevFieldName, prevDatatype), (k) -> new TreeSet<>());
                    dates.add(dateRange);
                    
                    // Update our tracking variables.
                    prevFieldName = fieldName;
                    prevDatatype = datatype;
                    startDate = date;
                }
                prevDate = date;
            }
            prevColumnFamily = columnFamily;
        }
        
        // After there are no more rows, ensure that we record the last date range for the last fieldName-datatype combination that we saw.
        Pair<Date,Date> dateRange = Pair.of(startDate, prevDate);
        SortedSet<Pair<Date,Date>> dates = dateMap.computeIfAbsent(Pair.of(prevFieldName, prevDatatype), (k) -> new TreeSet<>());
        dates.add(dateRange);
        
        // New tracking variables.
        Pair<String,String> prevFieldNameAndDataType = null;
        Pair<Date,Date> prevFrequencyDateRange = null;
        Date holeStartDate = null;
        Map<Pair<String,String>,FieldIndexHole> fieldIndexHoles = new HashMap<>();
        
        // Now that we have the date ranges for the frequency and index rows, compare the date ranges for each fieldName-datatype combination to identify any
        // and all field index holes. Evaluate the date ranges for each fieldName-datatype.
        for (Pair<String,String> fieldNameAndDatatype : frequencyMap.keySet()) {
            
            // If hole start date is not null, we have a hole left over from the previous fieldName-datatype combination. The index hole spans from the hole
            // start date to the end of the last frequency date range.
            if (holeStartDate != null) {
                FieldIndexHole indexHole = fieldIndexHoles.computeIfAbsent(prevFieldNameAndDataType, (k) -> new FieldIndexHole(k.getLeft(), k.getRight()));
                indexHole.addDateRange(Pair.of(holeStartDate, prevFrequencyDateRange.getRight()));
                holeStartDate = null;
            }
            
            // At least one corresponding index row was seen. Compare the date ranges to identify any index holes.
            if (indexMap.containsKey(fieldNameAndDatatype)) {
                SortedSet<Pair<Date,Date>> frequencyDates = frequencyMap.get(fieldNameAndDatatype);
                
                Iterator<Pair<Date,Date>> indexDatesIterator = indexMap.get(fieldNameAndDatatype).iterator();
                Pair<Date,Date> prevIndexDateRange = null;
                boolean comparePrevIndexDateRange = false;
                // Evaluate each date range we saw for frequency rows for the current fieldName-datatype.
                for (Pair<Date,Date> frequencyDateRange : frequencyDates) {
                    Date frequencyStartDate = frequencyDateRange.getLeft();
                    Date frequencyEndDate = frequencyDateRange.getRight();
                    
                    // If it's been flagged that we need to compare the previous index date range to the current frequency date range, do so. This is done when
                    // we potentially have an index hole that spans over the end of the previous frequency date range and the start of the next frequency date
                    // range.
                    if (comparePrevIndexDateRange) {
                        Date indexStartDate = prevIndexDateRange.getLeft();
                        Date indexEndDate = prevIndexDateRange.getRight();
                        
                        // If holeStartDate is not null, we have an index hole left over from the previous frequency date range. The index hole spans from the
                        // hole start date to the end of the last frequency date range.
                        if (holeStartDate != null) {
                            FieldIndexHole indexHole = fieldIndexHoles.computeIfAbsent(fieldNameAndDatatype,
                                            (k) -> new FieldIndexHole(k.getLeft(), k.getRight()));
                            indexHole.addDateRange(Pair.of(holeStartDate, prevFrequencyDateRange.getRight()));
                            holeStartDate = null;
                        }
                        
                        // The index start date is equal to the frequency start date. Check for a hole.
                        if (indexStartDate.equals(frequencyStartDate)) {
                            if (!indexEndDate.equals(frequencyEndDate)) {
                                // There is an index hole starting the day after the index end date. We must evaluate the next index date range to determine the
                                // end date of the index hole.
                                calendar.setTime(indexEndDate);
                                calendar.add(Calendar.DATE, 1);
                                holeStartDate = calendar.getTime();
                            }
                            // Otherwise there is no index hole here.
                        } else {
                            // The index start date is after the frequency start date. Check if we have a hole that partially covers the frequency date range,
                            // or all of it.
                            FieldIndexHole indexHole = fieldIndexHoles.computeIfAbsent(fieldNameAndDatatype,
                                            (k) -> new FieldIndexHole(k.getLeft(), k.getRight()));
                            if (indexStartDate.before(frequencyEndDate)) {
                                // There is an index hole starting on the frequency start date, and ending the day before the index start date.
                                calendar.setTime(indexStartDate);
                                calendar.add(Calendar.DATE, -1);
                                indexHole.addDateRange(Pair.of(frequencyStartDate, calendar.getTime()));
                                
                                if (indexEndDate.before(frequencyEndDate)) {
                                    // There is an index hole starting the day after the index end date. We must evaluate the next index date range to determine
                                    // the end date of the index hole.
                                    calendar.setTime(indexEndDate);
                                    calendar.add(Calendar.DATE, 1);
                                    holeStartDate = calendar.getTime();
                                }
                            } else {
                                // The entire frequency date range is an index hole. Add it as such, and continue to the next frequency date range. We want to
                                // compare the current index date range to the next frequency date range as well.
                                indexHole.addDateRange(frequencyDateRange);
                                continue;
                            }
                        }
                        comparePrevIndexDateRange = false;
                    }
                    
                    // Evaluate each index date range against the current frequency date range. If we see an index date range that begins after the current
                    // frequency date range, we will skip to the next frequency date range.
                    while (indexDatesIterator.hasNext()) {
                        Pair<Date,Date> indexDateRange = indexDatesIterator.next();
                        Date indexStartDate = indexDateRange.getLeft();
                        Date indexEndDate = indexDateRange.getRight();
                        
                        if (indexStartDate.equals(frequencyStartDate)) {
                            if (indexEndDate.equals(frequencyEndDate)) {
                                // The current index date range is equal to the current frequency date rang, and there is no index hole for the current
                                // frequency date range. Break out of the loop and continue to the next frequency date range.
                                prevIndexDateRange = indexDateRange;
                                break;
                            } else {
                                // There is an index hole starting the day after the index end date. Mark the start date, and continue to the next index date
                                // range to determine the end date.
                                calendar.setTime(indexEndDate);
                                calendar.add(Calendar.DATE, 1);
                                holeStartDate = calendar.getTime();
                            }
                        } else if (indexStartDate.before(frequencyEndDate)) {
                            FieldIndexHole indexHole;
                            calendar.setTime(indexStartDate);
                            calendar.add(Calendar.DATE, -1);
                            if (holeStartDate != null) {
                                // If holeStartDate is not null, we've previously identified the start of an index hole that is not the start of the frequency
                                // date range. There is an index hole from holeStartDate to the day before the index start date.
                                indexHole = fieldIndexHoles.computeIfAbsent(fieldNameAndDatatype, (k) -> new FieldIndexHole(k.getLeft(), k.getRight()));
                                indexHole.addDateRange(Pair.of(holeStartDate, calendar.getTime()));
                                holeStartDate = null;
                            } else {
                                // There is an index hole from the frequency start date to the day before the index start date.
                                indexHole = fieldIndexHoles.computeIfAbsent(fieldNameAndDatatype, (k) -> new FieldIndexHole(k.getLeft(), k.getRight()));
                                indexHole.addDateRange(Pair.of(frequencyStartDate, calendar.getTime()));
                            }
                            
                            // It's possible for the current index date range to end before the current frequency date range. If so, this indicates a new index
                            // hole.
                            if (indexEndDate.before(frequencyEndDate)) {
                                // There is an index hole starting the day after the index end date. We need to evaluate the next index date range to determine
                                // the end of the index hole. Mark the start of this new index hole.
                                calendar.setTime(indexEndDate);
                                calendar.add(Calendar.DATE, 1);
                                holeStartDate = calendar.getTime();
                            }
                        } else {
                            // The start of the current index date range occurs after the current frequency date range. There is a hole in the current frequency
                            // date range.
                            FieldIndexHole indexHole = fieldIndexHoles.computeIfAbsent(fieldNameAndDatatype,
                                            (k) -> new FieldIndexHole(k.getLeft(), k.getRight()));
                            if (holeStartDate == null) {
                                // The entire current frequency date range is an index hole. Add it as such and break out to continue to the next frequency
                                // date range.
                                indexHole.addDateRange(frequencyDateRange);
                                break;
                            } else {
                                // There is an index hole from the recorded hole start date to the end of the frequency date range. Add it as such and break
                                // out to continue to the next frequency date range.
                                indexHole.addDateRange(Pair.of(holeStartDate, frequencyEndDate));
                                holeStartDate = null;
                                // The current index date range is entirely after the current frequency date range. As such, we need to compare the current
                                // index date range to the next frequency date range.
                                comparePrevIndexDateRange = true;
                            }
                        }
                        // Update the prev index date range.
                        prevIndexDateRange = indexDateRange;
                    }
                    // Update the prev frequency date range.
                    prevFrequencyDateRange = frequencyDateRange;
                }
                
            } else {
                // No corresponding index rows were seen for any of the frequency rows. Each date range represents an index hole.
                FieldIndexHole indexHole = fieldIndexHoles.computeIfAbsent(fieldNameAndDatatype, (k) -> new FieldIndexHole(k.getLeft(), k.getRight()));
                indexHole.addDateRanges(frequencyMap.get(fieldNameAndDatatype));
            }
            // Update the prev fieldName-datatype.
            prevFieldNameAndDataType = fieldNameAndDatatype;
        }
        
        // If we have a non-null hole start date after processing all the date ranges, we have an index hole that ends at the last frequency date range seen
        // for the last fieldName-datatype combination.
        if (holeStartDate != null) {
            FieldIndexHole indexHole = fieldIndexHoles.computeIfAbsent(prevFieldNameAndDataType, (k) -> new FieldIndexHole(k.getLeft(), k.getRight()));
            indexHole.addDateRange(Pair.of(holeStartDate, prevFrequencyDateRange.getRight()));
        }
        
        return fieldIndexHoles;
    }
    
    private static String getKey(String instanceID, String metadataTableName) {
        StringBuilder builder = new StringBuilder();
        builder.append(instanceID).append('\0');
        builder.append(metadataTableName).append('\0');
        return builder.toString();
    }
    
    private static String getKey(AllFieldMetadataHelper helper) {
        return getKey(helper.accumuloClient.instanceOperations().getInstanceID(), helper.metadataTableName);
    }
    
    @Override
    public String toString() {
        return getKey(this);
    }
    
}
