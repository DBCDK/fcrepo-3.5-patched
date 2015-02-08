/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.management;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;


/**
 * @author Edwin Shin
 */
public abstract class TestPIDGenerator
        extends TestCase {

    private PIDGenerator testPIDGenerator;

    private Set<String> namespaces;

    protected abstract PIDGenerator getTestPIDGenerator();

    protected abstract Set<String> getNamespaces();

    @Override
    protected void setUp() {
        testPIDGenerator = getTestPIDGenerator();
        if (testPIDGenerator == null) {
            fail("getTestPIDGenerator() returned null");
        }
        namespaces = getNamespaces();
        if (namespaces.size() == 0) {
            fail("must provide at least one namespace");
        }
    }

    public void testGeneratePID() {
        Iterator<String> it = namespaces.iterator();
        try {
            while (it.hasNext()) {
                testPIDGenerator.generatePID(it.next());
            }
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public void testgetLastPID() {
        try {
            testPIDGenerator.getLastPID();
        } catch (UnsupportedOperationException e) {
            // optional
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public void testNeverGeneratePID() {
        // TODO
    }
}
