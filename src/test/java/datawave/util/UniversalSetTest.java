package datawave.util;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class UniversalSetTest {
    
    Collection<String> universalSet = UniversalSet.instance();
    
    @Test
    public void testExpectedBehavior() {
        
        Assertions.assertTrue(universalSet.isEmpty());
        
        Assertions.assertTrue(universalSet.contains(new Object()));
        
        Assertions.assertTrue(universalSet.containsAll(Sets.newHashSet("foo", "bar", "baz")));
        
        int originalSize = universalSet.size();
        
        Assertions.assertFalse(universalSet.remove(this));
        Assertions.assertEquals(universalSet.size(), originalSize);
        
        Assertions.assertFalse(universalSet.removeAll(Sets.newHashSet(this)));
        Assertions.assertEquals(universalSet.size(), originalSize);
        
        Assertions.assertFalse(universalSet.add("trouble-maker"));
        Assertions.assertEquals(universalSet.size(), originalSize);
        
        Assertions.assertFalse(universalSet.addAll(Sets.newHashSet("foo", "bar", "baz")));
        Assertions.assertEquals(universalSet.size(), originalSize);
        
        Assertions.assertFalse(universalSet.retainAll(Sets.newHashSet("foo", "bar", "baz")));
        Assertions.assertEquals(universalSet.size(), originalSize);
        
        Assertions.assertFalse(universalSet.removeIf(x -> true));
        Assertions.assertEquals(universalSet.size(), originalSize);
        
        Iterator<String> iterator = universalSet.iterator();
        Assertions.assertFalse(iterator.hasNext());
        
        Assertions.assertArrayEquals(universalSet.toArray(), new String[0]);
        
        Assertions.assertArrayEquals(universalSet.toArray(new String[0]), new String[0]);
        
        Assertions.assertEquals(universalSet.size(), 0);
        
        Assertions.assertTrue(universalSet instanceof Set);
    }
}
