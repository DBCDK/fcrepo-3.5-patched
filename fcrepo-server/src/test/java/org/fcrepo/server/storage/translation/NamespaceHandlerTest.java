/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also 
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.server.storage.translation;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kasper
 */
public class NamespaceHandlerTest {
    
    public NamespaceHandlerTest() {
    }

    @Test
    public void testGetNamespaceOrder() {
        NamespaceHandler nsh = new NamespaceHandler();
        String namespace1 = "firstnamespace";
        String namespace2 = "secondnamespace";
        
        nsh.addNamespace("prefix", namespace1);
        assertEquals("first namespace is as expected", namespace1, nsh.getNamespace("prefix"));
        
        nsh.addNamespace("prefix", namespace2);
        assertEquals("second namespace is as expected", namespace2, nsh.getNamespace("prefix"));
        
        nsh.removeNamespace("prefix");
        assertEquals("second namespace is removed", namespace1, nsh.getNamespace("prefix"));
    }
    
    // Supports legacy test runners
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(NamespaceHandlerTest.class);
    }
    
}
