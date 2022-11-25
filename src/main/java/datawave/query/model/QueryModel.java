package datawave.query.model;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.util.StringUtils;

import static java.nio.charset.StandardCharsets.UTF_8;

public class QueryModel implements Serializable {
    private static final long serialVersionUID = -7618411736250884135L;
    
    private static final String EMPTY_STR = "";
    public static final char PARAM_VALUE_SEP = ',';
    public static final String PARAM_VALUE_SEP_STR = new String(new char[] {PARAM_VALUE_SEP});
    public static final String LIMIT_FIELDS_ORIGINAL_COUNT_SUFFIX = "ORIGINAL_COUNT";
    
    // forward mappings map a field name to a list of database fields
    protected final Multimap<String,String> forwardQueryMapping;
    // reverse mappings map a database field to a (hopefully) user understandable field name
    protected final Map<String,String> reverseQueryMapping;
    // lenient forward mappings are those that are best effort in that if the underlying
    // database field cannot be found in the index, then it can be dropped.
    protected final Set<String> lenientForwardMappings;
    
    public QueryModel() {
        this.forwardQueryMapping = HashMultimap.create();
        this.lenientForwardMappings = new HashSet();
        this.reverseQueryMapping = Maps.newHashMap();
    }
    
    public QueryModel(QueryModel other) {
        this.forwardQueryMapping = HashMultimap.create(other.getForwardQueryMapping());
        this.lenientForwardMappings = new HashSet(other.lenientForwardMappings);
        this.reverseQueryMapping = Maps.newHashMap(other.getReverseQueryMapping());
    }
    
    public Multimap<String,String> getForwardQueryMapping() {
        return this.forwardQueryMapping;
    }
    
    public Map<String,String> getReverseQueryMapping() {
        return this.reverseQueryMapping;
    }
    
    public void addTermToModel(String alias, String nameOnDisk) {
        forwardQueryMapping.put(alias, nameOnDisk);
    }
    
    public void addTermToReverseModel(String nameOnDisk, String alias) {
        reverseQueryMapping.put(nameOnDisk, alias);
    }
    
    public Collection<String> getMappingsForAlias(String field) {
        return forwardQueryMapping.get(field);
    }
    
    public String getReverseAliasForField(String field) {
        return reverseQueryMapping.get(field);
    }
    
    public Set<String> getLenientForwardMappings() {
        return lenientForwardMappings;
    }
    
    public void setLenientForwardMappings(Set<String> fields) {
        lenientForwardMappings.clear();
        if (fields != null) {
            lenientForwardMappings.addAll(fields);
        }
    }
    
    public void addLenientForwardMappings(String field) {
        lenientForwardMappings.add(field);
    }
    
    public boolean isLenientForwardMapping(String field) {
        return lenientForwardMappings.contains(field);
    }
    
    /**
     * Remap projection fields
     * 
     * @param projectFields
     * @param model
     * @return
     * 
     */
    public String remapParameter(String projectFields, Multimap<String,String> model) {
        Set<String> projectFieldsList = new HashSet<>(Arrays.asList(StringUtils.split(projectFields, PARAM_VALUE_SEP)));
        
        Collection<String> newMappings = remapParameter(projectFieldsList, model);
        
        return org.apache.commons.lang.StringUtils.join(newMappings, PARAM_VALUE_SEP);
        
    }
    
    public Collection<String> remapParameter(Collection<String> projectFields, Multimap<String,String> model) {
        // Don't be destructive, always preserve what was passed in.
        Set<String> newMappings = Sets.newHashSet(projectFields);
        
        // We could generate a Set to eliminate duplicates but it would require yet another iteration
        // just something to consider for future.
        for (String field : projectFields) {
            field = field.toUpperCase();
            if (model.containsKey(field)) {
                newMappings.addAll(model.get(field));
            }
        }
        
        return newMappings;
    }
    
    public Collection<String> remapParameterEquation(Collection<String> projectFields, Multimap<String,String> model) {
        // Don't be destructive, always preserve what was passed in.
        Set<String> newMappings = Sets.newHashSet(projectFields);
        
        // We could generate a Set to eliminate duplicates but it would require yet another iteration
        // just something to consider for future.
        for (String field : projectFields) {
            field = field.toUpperCase();
            String leftSide = field;
            String rightSide = "";
            if (field.indexOf('=') != -1) {
                leftSide = field.substring(0, field.indexOf('=')).trim();
                rightSide = field.substring(field.indexOf('='));
            }
            if (model.containsKey(leftSide)) {
                for (String projection : model.get(leftSide)) {
                    newMappings.add(projection + rightSide);
                }
            }
        }
        
        return newMappings;
    }
    
    /***
     * Take Current FieldName and remap it using the reverse QueryModel. Note here, If we do not find a hit in the queryModel then no aliasing is performed and
     * the default fieldname is returned. This is different behavior than in the forward case.
     * 
     * 
     * @param fieldName
     * @return alias
     */
    public String aliasFieldNameReverseModel(String fieldName) {
        String fName = fieldName;
        
        int idx = fName.indexOf(".");
        
        if (idx > -1) {
            fName = fName.substring(0, idx);
        }
        
        if (reverseQueryMapping.containsKey(fName)) {
            String term = reverseQueryMapping.get(fName);
            if (idx > -1) { // if there was a dot, include the grouping we stripped off earlier
                return term + fieldName.substring(idx);
            } else {
                return term;
            }
        } else if (fName != null && fName.endsWith(LIMIT_FIELDS_ORIGINAL_COUNT_SUFFIX)) {
            String atomPart = fName.substring(0, fName.indexOf(LIMIT_FIELDS_ORIGINAL_COUNT_SUFFIX));
            String alias = aliasFieldNameReverseModel(atomPart);
            if (alias.equals(atomPart)) {
                return alias + "." + LIMIT_FIELDS_ORIGINAL_COUNT_SUFFIX;
            } else {
                return alias + "." + atomPart.replaceAll("_", ".") + "." + LIMIT_FIELDS_ORIGINAL_COUNT_SUFFIX;
                
            }
        }
        
        return fieldName;
    }
    
    /**
     * Print the forward mapping of the model to the provided PrintStream in a form capable of being reloaded via WriteModelToAccumulo
     * 
     * @param out
     */
    public void dumpForward(PrintStream out) {
        out.println("# Query Model Forward Mapping - " + System.currentTimeMillis());
        for (Entry<String,String> mapping : this.forwardQueryMapping.entries()) {
            out.print(mapping.getKey() + ":" + mapping.getValue());
            out.println();
        }
    }
    
    /**
     * Print the reverse mapping of the model to the provided PrintStream in a form capable of being reloaded via WriteModelToAccumulo
     * 
     * @param out
     */
    public void dumpReverse(PrintStream out) {
        out.println("# Query Model Reverse Mapping - " + System.currentTimeMillis());
        for (Entry<String,String> mapping : this.reverseQueryMapping.entrySet()) {
            out.println(mapping.getKey() + ":" + mapping.getValue());
        }
    }
    
    @Override
    public String toString() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            PrintStream stream = new PrintStream(new BufferedOutputStream(bytes), false, UTF_8.name());
            stream.println(super.toString());
            dumpForward(stream);
            dumpReverse(stream);
            stream.flush();
            return bytes.toString(UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
}
