/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.test.api;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.ws.soap.SOAPFaultException;

import junit.framework.JUnit4TestAdapter;

import org.custommonkey.xmlunit.NamespaceContext;
import org.custommonkey.xmlunit.SimpleNamespaceContext;
import org.custommonkey.xmlunit.XMLUnit;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.Constants;
import org.fcrepo.common.Models;
import org.fcrepo.common.PID;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.server.types.gen.RelationshipTuple;
import org.fcrepo.server.utilities.TypeUtility;
import org.fcrepo.test.FedoraServerTestCase;
import org.fcrepo.test.ManagedContentTranslator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.trippi.TripleIterator;
import org.trippi.io.RIOTripleIterator;

/**
 * Tests for the various relationship API-M methods. Tests assume a running
 * instance of the Fedora server with Resource Index enabled.
 *
 * @author Edwin Shin
 */
public class TestRelationships
        extends FedoraServerTestCase
        implements Constants {

    private static FedoraClient s_client;
    
    private FedoraAPIMMTOM apim;
// probably 1 thread would be fine...
    private static ExecutorService exec;

    private static final String RISEARCH_QUERY =
            "/risearch?type=triples&lang=spo&format=NTriples&stream=on&"
                    + "flush=true&query=";

    private static byte[] DEMO_888_FOXML;

    private static byte[] DEMO_777_FOXML;

    private static String MULTIBYTE_UTF8;

    // FIXME: once the raw pid form of subject in the relationship methods is no longer in use, remove 0 and 4 below
    // demo:777 contains no rels-ext/rels-int datastream, demo:888 contains both
    // subject identifiers for the following scenarios (add/purge mostly)
    // 0: demo:777, subject is the digital object, as a pid
    // 1: demo:777, subject is the digital object, as a uri
    // 2: demo:777, subject is Datastream DS1, as a uri
    // 3: demo:777, subject is Datastream DS2, as a uri
    // 4: demo:888, subject is the digital object, as a pid
    // 5: demo:888, subject is the digital object, as a uri
    // 6: demo:888, subject is Datastream DS1, as a uri
    // 7: demo:888, subject is Datastream DS2, as a uri

    // test subject identifiers for the above
    private final String subject[] = {
            "demo:777", // deprecated
            "info:fedora/demo:777",
            "info:fedora/demo:777/DS1",
            "info:fedora/demo:777/DS2",
            "demo:888", // deprecated
            "info:fedora/demo:888",
            "info:fedora/demo:888/DS1",
            "info:fedora/demo:888/DS2",
            // objects with managed content RELS-EXT/RELS-INT
            "demo:777m", // deprecated
            "info:fedora/demo:777m", "info:fedora/demo:777m/DS1",
            "info:fedora/demo:777m/DS2",
            "demo:888m", // deprecated
            "info:fedora/demo:888m", "info:fedora/demo:888m/DS1",
            "info:fedora/demo:888m/DS2"};

    // test objects.  Note that setup also creates clones of these with managed content RELS-EXT/RELS-INT (and DC)
    static {
        // Test FOXML object with RELS-EXT and RELS-INT datastream
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        sb.append("<foxml:digitalObject VERSION=\"1.1\" PID=\"demo:888\" xmlns:foxml=\"info:fedora/fedora-system:def/foxml#\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"info:fedora/fedora-system:def/foxml# http://www.fedora.info/definitions/1/0/foxml1-1.xsd\">");
        sb.append("  <foxml:objectProperties>");
        sb.append("    <foxml:property NAME=\"info:fedora/fedora-system:def/model#state\" VALUE=\"A\"/>");
        sb.append("  </foxml:objectProperties>");
        sb.append("  <foxml:datastream ID=\"RELS-EXT\" CONTROL_GROUP=\"M\" STATE=\"A\">");
        sb.append("    <foxml:datastreamVersion FORMAT_URI=\"info:fedora/fedora-system:FedoraRELSExt-1.0\" ID=\"RELS-EXT.0\" MIMETYPE=\"application/rdf+xml\" LABEL=\"RDF Statements about this object\">");
        sb.append("      <foxml:xmlContent>");
        sb.append("        <rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\""
                + "                 xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\">");
        sb.append("          <rdf:Description rdf:about=\"info:fedora/demo:888\">");
        sb.append("            <fedora-model:hasModel rdf:resource=\"info:fedora/demo:UVA_STD_IMAGE_1\"/>");
        sb.append("            <fedora-model:hasModel rdf:resource=\""
                + Models.FEDORA_OBJECT_CURRENT.uri + "\"/>");
        sb.append("          </rdf:Description>");
        sb.append("        </rdf:RDF>");
        sb.append("      </foxml:xmlContent>");
        sb.append("    </foxml:datastreamVersion>");
        sb.append("  </foxml:datastream>");
        sb.append("  <foxml:datastream ID=\"RELS-INT\" CONTROL_GROUP=\"M\" STATE=\"A\">");
        sb.append("    <foxml:datastreamVersion FORMAT_URI=\"info:fedora/fedora-system:FedoraRELSInt-1.0\" ID=\"RELS-INT.0\" MIMETYPE=\"application/rdf+xml\" LABEL=\"RDF Statements about datastreams in this object\">");
        sb.append("      <foxml:xmlContent>");
        sb.append("        <rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\""
                + "                 xmlns:myns=\"http://www.example.org/testns#\">");
        sb.append("          <rdf:Description rdf:about=\"info:fedora/demo:888/DS1\">");
        sb.append("            <myns:test1 rdf:resource=\"info:fedora/demo:UVA_STD_IMAGE_1\"/>");
        sb.append("          </rdf:Description>");
        sb.append("          <rdf:Description rdf:about=\"info:fedora/demo:888/DS3\">");
        sb.append("            <myns:test2 rdf:resource=\"info:fedora/demo:11223344\"/>");
        sb.append("          </rdf:Description>");
        sb.append("        </rdf:RDF>");
        sb.append("      </foxml:xmlContent>");
        sb.append("    </foxml:datastreamVersion>");
        sb.append("  </foxml:datastream>");
        sb.append("</foxml:digitalObject>");

        try {
            DEMO_888_FOXML = sb.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException uee) {
        }

        // Test FOXML object with no RELS-EXT (or RELS-INT) datastream
        sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        sb.append("<foxml:digitalObject VERSION=\"1.1\" PID=\"demo:777\" xmlns:foxml=\"info:fedora/fedora-system:def/foxml#\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"info:fedora/fedora-system:def/foxml# http://www.fedora.info/definitions/1/0/foxml1-1.xsd\">");
        sb.append("  <foxml:objectProperties>");
        sb.append("    <foxml:property NAME=\"info:fedora/fedora-system:def/model#state\" VALUE=\"A\"/>");
        sb.append("  </foxml:objectProperties>");
        sb.append("</foxml:digitalObject>");

        try {
            DEMO_777_FOXML = sb.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException uee) {
        }

        try {
            // UTF-8 string with multibyte characters (for literal object tests)
            // construct explicitly from bytes to avoid any encoding issues with this source file (which *should* be utf-8)
            // (or could use: MULTIBYTE_UTF8 = "“Α ¿”";
            MULTIBYTE_UTF8 =
                    new String(new byte[] {(byte) 0xE2, (byte) 0x80,
                            (byte) 0x9C, // left double quotes “
                            (byte) 0xCE, (byte) 0x91, // capital alpha Α
                            (byte) 0x20, // space
                            (byte) 0xC2, (byte) 0xBF, // inverted question mark ¿
                            (byte) 0xE2, (byte) 0x80, (byte) 0x9D // right double quotes ”
                               },
                               "UTF-8");
        } catch (UnsupportedEncodingException uee) {
        }

    }

    @BeforeClass
    public static void bootStrap() throws Exception {
        s_client = getFedoraClient();
        exec = Executors.newFixedThreadPool(4);
    }
    
    @AfterClass
    public static void cleanUp() {
        exec.shutdown();
        s_client.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        apim = s_client.getAPIMMTOM();
        Map<String, String> nsMap = new HashMap<String, String>();
        nsMap.put("oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
        nsMap.put("dc", "http://purl.org/dc/elements/1.1/");
        nsMap.put("foxml", "info:fedora/fedora-system:def/foxml#");
        NamespaceContext ctx = new SimpleNamespaceContext(nsMap);
        XMLUnit.setXpathNamespaceContext(ctx);

        apim.ingest(TypeUtility.convertBytesToDataHandler(DEMO_888_FOXML),
                    FOXML1_1.uri,
                    "ingesting new foxml object");
        apim.ingest(TypeUtility.convertBytesToDataHandler(DEMO_777_FOXML),
                    FOXML1_1.uri,
                    "ingesting new foxml object");

        // managed content versions of above (reserved datastreams translated from X to M)
        ManagedContentTranslator.createManagedClone(apim,
                                                    "demo:888",
                                                    "demo:888m");
        ManagedContentTranslator.createManagedClone(apim,
                                                    "demo:777",
                                                    "demo:777m");
    }

    @After
    public void tearDown() throws Exception {
        apim.purgeObject("demo:777", "", false);
        apim.purgeObject("demo:888", "", false);
        apim.purgeObject("demo:777m", "", false);
        apim.purgeObject("demo:888m", "", false);
        XMLUnit.setXpathNamespaceContext(SimpleNamespaceContext.EMPTY_CONTEXT);
    }

    @Test
    public void testAddRelationship() throws Exception {
        String p, o;
        int relNum = 0; // used to form unique relationships... addRelationship needs unique predicate x object combinatinos

        for (String s : subject) {
            p = "urn:bar" + relNum++;
            o = "urn:baz";
            addRelationship(s, p, o, false, null);

            // plain literal
            o = "quux";
            addRelationship(s, p, o, true, null);

            // datatyped literal
            o = "1970-01-01T00:00:00Z";
            addRelationship(s, p, o, true, Constants.RDF_XSD.DATE_TIME.uri);

            // utf-8 literal with multibyte sequences
            o = MULTIBYTE_UTF8;
            addRelationship(s, p, o, true, null);
        }
    }

    @Test
    public void testValidation() {
        String p, o;
        int relNum = 0; // used to form unique relationships or objects/object literals... addRelationship needs unique predicate x object combinations

        for (String s : subject) {

            p = "http://purl.org/dc/elements/1.1/title";
            o = "A Dictionary of Maqiao" + relNum++;

            // DC rels only invalid for RELS-EXT...
            if (!s.endsWith("DS1") && !s.endsWith("DS2")) {
                try {
                    apim.addRelationship(s, p, o, true, null);
                    fail("Adding Dublin Core relationship should have failed - "
                        + s);
                } catch (SOAPFaultException se) {
                    assertTrue(se.getMessage(),
                               se.getMessage().contains("improper relationship assertion"));
                }
            }

            p = "info:fedora/fedora-system:def/model#foo";
            try {
                apim.addRelationship(s, p, o, true, null);
                fail("Adding Fedora Model relationship should have failed - " + s);
            } catch (SOAPFaultException se) {
                assertTrue(se.getMessage(),
                           se.getMessage().contains("Disallowed predicate"));
            }

            p = "urn:bar" + relNum;
            // invalid dateTime literal
            o = "2009-10-05T16:02:26+0100";
            try {
                apim.addRelationship(s, p, o, true, Constants.RDF_XSD.DATE_TIME.uri);
                fail("Adding invalid date/time literal in relationship should have failed - "
                    + s);
            } catch (SOAPFaultException se) {
                assertTrue(se.getMessage(),
                           se.getMessage().contains("is not a valid 'dateTime' value"));
            }
        }
    }

    @Test
    public void testBadSubjectURI() {
        String s, p, o;

        // subject is a valid info:fedora/ uri for an object, but object does not exist
        s = "info:fedora/does:notexist";
        p = "urn:foo";
        o = "urn:bar";
        try {
            apim.addRelationship(s, p, o, false, null);
            fail("Adding relationship with subject as a Fedora DO that does not exist should have failed");
        } catch (SOAPFaultException se) {}

        // subject is a valid info:fedora/ uri for a datastream, but object does not exist
        s = "info:fedora/does:notexist/DS1";
        p = "urn:foo";
        o = "urn:baz";
        try {
            apim.addRelationship(s, p, o, false, null);
            fail("Adding relationship with subject as a Fedora object datastream where DO does not exist should have failed");
        } catch (SOAPFaultException se) {}

        // subject is a valid uri, but not in info:fedora/ scheme
        s = "http://www.example.org/test";
        p = "urn:foo";
        o = "urn:quux";
        try {
            apim.addRelationship(s, p, o, false, null);
            fail("Adding relationship with subject uri not in the info:fedora scheme should have failed");
        } catch (SOAPFaultException se) {}

        // Valid PID & datastream ID, but invalid subject URI
        // should be: info:fedora/demo:888/DS1
        s = "demo:888/DS1";
        p = "urn:foo";
        o = "urn:quux";
        try {
            apim.addRelationship(s, p, o, false, null);
            fail("Adding relationship with invalid short URI should have failed");
        } catch (SOAPFaultException se) {}
    }

    @Test
    public void testGetRelationships() throws Exception {
        String p, o;
        int relNum = 0; // used to form unique relationships or objects/object literals... addRelationship needs unique predicate x object combinations

        for (String s : subject) {

            p = "urn:bar" + relNum++;
            o = "urn:baz";
            // test object relationship
            getRelationship(s, p, o, false, null);

            p = "urn:title" + relNum;
            o = "asdf";
            // test untyped literal relationship
            getRelationship(s, p, o, true, null);

            p = "urn:temperature" + relNum;
            o = "98.6";
            // test typed literal relationship
            getRelationship(s, p, o, true, Constants.RDF_XSD.FLOAT.uri);

            // test untyped utf-8 literal with multibyte sequences
            p = "urn:utf8literal" + relNum;
            o = MULTIBYTE_UTF8;
            getRelationship(s, p, o, true, null);
        }
    }

    @Test
    public void testGetAllRelationships() throws Exception {
        // subject uri and pid
        List<RelationshipTuple> tuples = apim.getRelationships("demo:777", null);
        assertEquals(1, tuples.size());
    }

    @Test
    public void testBasicCModelRelationships() throws Exception {
        // just the uri form for subject, pid form has got a hammering above
        for (String pid : new String[] {"info:fedora/demo:777",
                "info:fedora/demo:888"}) {
            checkExistsViaGetRelationships(pid,
                                           Constants.MODEL.HAS_MODEL.uri,
                                           Models.FEDORA_OBJECT_CURRENT.uri);
        }
    }

    @Test
    public void testPurgeRelationships() throws Exception {
        String p, o;
        int relNum = 0; // used to form unique relationships or objects/object literals... addRelationship needs unique predicate x object combinations

        for (String s : subject) {

            p = "urn:p" + relNum++;
            o = "urn:o";
            purgeRelationship(s, p, o, false, null);

            p = "urn:title" + relNum;
            o = "asdf";//"三国演义"; // test unicode
            purgeRelationship(s, p, o, true, null);

            p = "urn:temperature" + relNum;
            o = "98.6";
            purgeRelationship(s, p, o, true, Constants.RDF_XSD.FLOAT.uri);

            // utf-8 literal with multibyte sequences
            p = "urn:utf8literal" + relNum;
            o = MULTIBYTE_UTF8;
            purgeRelationship(s, p, o, true, null);

            assertFalse("Purging non-existant relation should have failed",
                        apim.purgeRelationship(s,
                                               "urn:asdf",
                                               "867-5309",
                                               true,
                                               null));
            
            addRelationship(s, p, o, true, null);
            addRelationship(s, p, "foo", true, null);
            
            List<RelationshipTuple> tuples = apim.getRelationships(s, p);
            assertNotNull(tuples);
            assertEquals(2, tuples.size());
            assertTrue("Purging relationship with null object should delete all subject/predicate matches",
                    apim.purgeRelationship(s, p, null, true, null));
            tuples = apim.getRelationships(s, p);
            assertNotNull(tuples);
            assertEquals(0, tuples.size());
        }
    }

    private void checkExistsViaGetRelationships(String subject,
                                                String predicate,
                                                String object) throws Exception {
        boolean found = false;

        for (RelationshipTuple tuple : apim
                .getRelationships(subject, predicate)) {
            if (tuple.getSubject().equals(subjectAsURI(subject))
                    && tuple.getPredicate().equals(predicate)
                    && tuple.getObject().equals(object)) {
                found = true;
            }
        }
        assertTrue("Relationship not found via getRelationships (subject="
                           + subject + ", predicate=" + predicate + ", object="
                           + object,
                   found);
    }

    // note: queries resource index by predicate and object, and then checks subject is ok
    // so make sure if testing across multiple objects that predicate x object combinations are unique

    private void addRelationship(String subject,
                                 String predicate,
                                 String object,
                                 boolean isLiteral,
                                 String datatype) throws Exception {
        assertTrue(apim.addRelationship(subject,
                                        predicate,
                                        object,
                                        isLiteral,
                                        datatype));
        assertFalse("Adding duplicate relationship should return false",
                    apim.addRelationship(subject,
                                         predicate,
                                         object,
                                         isLiteral,
                                         datatype));

        // check resource index
        String query = "";
        if (isLiteral) {
            if (datatype != null) {
                query =
                        String.format("* <%s> '%s'^^%s",
                                      predicate,
                                      object,
                                      datatype);
            } else {
                query = String.format("* <%s> '%s'", predicate, object);
            }
        } else {
            query = String.format("* <%s> <%s>", predicate, object);
        }

        TripleIterator triples = queryRI(query);
        try {
            assertTrue("Relationship not found in RI (query = " + query + ")",
                       triples.hasNext());
            while (triples.hasNext()) {
                assertEquals(triples.next().getSubject().stringValue(),
                             subjectAsURI(subject));
            }
        } finally {
            triples.close();
        }
    }

    // FIXME: remove once pid no longer allowed as subject in relationships methods
    // check if subject is a uri or just a pid, if a pid then return the uri form

    private String subjectAsURI(String subj) {
        // already a uri?
        if (subj.startsWith(Constants.FEDORA.uri)) {
            return subj;
        }
        // no, convert to uri
        return PID.toURI(subj);

    }

    private void getRelationship(String subject,
                                 String predicate,
                                 String object,
                                 boolean isLiteral,
                                 String datatype) throws Exception {
        addRelationship(subject, predicate, object, isLiteral, datatype);
        List<RelationshipTuple> tuples = apim.getRelationships(subject, predicate);
        assertNotNull(tuples);
        assertEquals(1, tuples.size());
        assertEquals(subjectAsURI(subject), tuples.get(0).getSubject());
        assertEquals(predicate, tuples.get(0).getPredicate());
        assertEquals(object, tuples.get(0).getObject());
        assertEquals(isLiteral, tuples.get(0).isIsLiteral());
        assertEquals(datatype, tuples.get(0).getDatatype());
    }

    private void purgeRelationship(String subject,
                                   String predicate,
                                   String object,
                                   boolean isLiteral,
                                   String datatype) throws Exception {
        addRelationship(subject, predicate, object, isLiteral, datatype);
        assertTrue(apim.purgeRelationship(subject,
                                          predicate,
                                          object,
                                          isLiteral,
                                          datatype));
    }

    private TripleIterator queryRI(String query) throws Exception {
        FedoraClient client = getFedoraClient();
        InputStream results =
                client.get(RISEARCH_QUERY + URLEncoder.encode(query, "UTF-8"),
                           true);
        return new RIOTripleIterator(results,
                                     new NTriplesParser(),
                                     "info/fedora",
                                     exec);

    }

    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TestRelationships.class);
    }

    public static void main(String[] args) {
        JUnitCore.runClasses(TestRelationships.class);
    }

}
