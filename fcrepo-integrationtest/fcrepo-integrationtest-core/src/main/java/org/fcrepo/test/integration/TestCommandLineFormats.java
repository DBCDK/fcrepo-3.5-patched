/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.test.integration;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.custommonkey.xmlunit.XMLAssert.assertXpathEvaluatesTo;
import static org.custommonkey.xmlunit.XMLAssert.assertXpathExists; 
import static org.custommonkey.xmlunit.XMLAssert.assertXpathNotExists; 

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.abdera.Abdera;
import org.apache.abdera.model.Document;
import org.apache.abdera.model.Feed;
import org.apache.abdera.parser.Parser;
import org.custommonkey.xmlunit.NamespaceContext;
import org.custommonkey.xmlunit.SimpleNamespaceContext;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.JUnit4TestAdapter;

import org.fcrepo.client.FedoraClient;
import org.fcrepo.client.utility.export.Export;
import org.fcrepo.client.utility.ingest.Ingest;
import org.fcrepo.common.PID;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.server.utilities.TypeUtility;
import org.fcrepo.test.FedoraTestCase;
import org.fcrepo.test.api.TestAPIM;
import org.fcrepo.utilities.FileUtils;


/**
 * Tests the command-line Ingest and Export interfaces with varied formats.
 *
 * @author Bill Branan
 */
public class TestCommandLineFormats
        extends FedoraTestCase {

    private static FedoraAPIMMTOM apim;
    
    private static FedoraClient s_client;

    @BeforeClass
    public static void bootStrap() throws Exception {
        s_client = getFedoraClient(getBaseURL(), getUsername(), getPassword());
        apim = s_client.getAPIMMTOM();
    }
    
    @AfterClass
    public static void cleanUp() {
        s_client.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        Map<String, String> nsMap = new HashMap<String, String>();
        nsMap.put("foxml", "info:fedora/fedora-system:def/foxml#");
        nsMap.put("METS", "http://www.loc.gov/METS/");
        nsMap.put("", "http://www.w3.org/2005/Atom");
        NamespaceContext ctx = new SimpleNamespaceContext(nsMap);
        XMLUnit.setXpathNamespaceContext(ctx);
    }

    @After
    public void tearDown() {
        XMLUnit.setXpathNamespaceContext(SimpleNamespaceContext.EMPTY_CONTEXT);
    }

    @Test
    public void testIngestFOXML10() throws Exception {
        System.out.println("Testing Ingest with FOXML 1.0 format");
        File foxml10 = File.createTempFile("demo_997", ".xml");
        FileOutputStream fileWriter = new FileOutputStream(foxml10);
        fileWriter.write(TestAPIM.demo997FOXML10ObjectXML);
        fileWriter.close();

        String[] parameters = {"f ", foxml10.getAbsolutePath(),
                               FOXML1_0.uri, getHost() + ":" + getPort(),
                               getUsername(), getPassword(), getProtocol(),
                               "\"Ingest\"", getFedoraAppServerContext()};

        Ingest.main(parameters);
        foxml10.delete();

        try {
            byte[] objectXML = TypeUtility.convertDataHandlerToBytes(apim.getObjectXML("demo:997"));
            assertTrue(objectXML.length > 0);
            String xmlIn = new String(objectXML, "UTF-8");
            assertXpathExists("foxml:digitalObject[@PID='demo:997']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            assertXpathEvaluatesTo("5", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
        } finally {
            apim.purgeObject("demo:997", "", false);
        }
    }

    @Test
    public void testIngestFOXML11() throws Exception {
        System.out.println("Testing Ingest with FOXML 1.1 format");
        File foxml11 = File.createTempFile("demo_998", ".xml");
        FileOutputStream fileWriter = new FileOutputStream(foxml11);
        fileWriter.write(TestAPIM.demo998FOXMLObjectXML);
        fileWriter.close();

        String[] parameters = {"f ", foxml11.getAbsolutePath(),
                               FOXML1_1.uri, getHost() + ":" + getPort(),
                               getUsername(), getPassword(), getProtocol(),
                               "\"Ingest\"", getFedoraAppServerContext()};

        Ingest.main(parameters);
        foxml11.delete();

        try {
            byte[] objectXML = TypeUtility.convertDataHandlerToBytes(apim.getObjectXML("demo:998"));
            assertTrue(objectXML.length > 0);
            String xmlIn = new String(objectXML, "UTF-8");
            assertXpathExists("foxml:digitalObject[@PID='demo:998']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            assertXpathEvaluatesTo("5", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
        } finally {
            apim.purgeObject("demo:998", "", false);
        }
    }

    @Test
    public void testIngestMETS11() throws Exception {
        System.out.println("Testing Ingest with METS 1.1 format");
        File mets = File.createTempFile("demo_999", ".xml");
        FileOutputStream fileWriter = new FileOutputStream(mets);
        fileWriter.write(TestAPIM.demo999METSObjectXML);
        fileWriter.close();

        String[] parameters = {"f ", mets.getAbsolutePath(),
                               METS_EXT1_1.uri, getHost() + ":" + getPort(),
                               getUsername(), getPassword(), getProtocol(),
                               "\"Ingest\"", getFedoraAppServerContext()};

        Ingest.main(parameters);
        mets.delete();

        try {
            byte[] objectXML = TypeUtility.convertDataHandlerToBytes(apim.getObjectXML("demo:999"));
            assertTrue(objectXML.length > 0);
            String xmlIn = new String(objectXML, "UTF-8");
            assertXpathExists("foxml:digitalObject[@PID='demo:999']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            assertXpathEvaluatesTo("5", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
        } finally {
            apim.purgeObject("demo:999", "", false);
        }
    }

    @Test
    public void testIngestMETS10() throws Exception {
        System.out.println("Testing Ingest with METS 1.0 format");
        File mets = File.createTempFile("demo_999b", ".xml");
        FileOutputStream fileWriter = new FileOutputStream(mets);
        fileWriter.write(TestAPIM.demo999bMETS10ObjectXML);
        fileWriter.close();

        String[] parameters = {"f ", mets.getAbsolutePath(),
                               METS_EXT1_0.uri, getHost() + ":" + getPort(),
                               getUsername(), getPassword(), getProtocol(),
                               "\"Ingest\"", getFedoraAppServerContext()};

        Ingest.main(parameters);
        mets.delete();

        try {
            byte[] objectXML = TypeUtility.convertDataHandlerToBytes(apim.getObjectXML("demo:999b"));
            assertTrue(objectXML.length > 0);
            String xmlIn = new String(objectXML, "UTF-8");
            assertXpathExists("foxml:digitalObject[@PID='demo:999b']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            assertXpathEvaluatesTo("5", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
        } finally {
            apim.purgeObject("demo:999b", "", false);
        }
    }

    @Test
    public void testIngestATOM() throws Exception {
        System.out.println("Testing Ingest with ATOM format");
        File atom = File.createTempFile("demo_1000", ".xml");
        FileOutputStream fileWriter = new FileOutputStream(atom);
        fileWriter.write(TestAPIM.demo1000ATOMObjectXML);
        fileWriter.close();

        String[] parameters = {"f ", atom.getAbsolutePath(),
                               ATOM1_1.uri, getHost() + ":" + getPort(),
                               getUsername(), getPassword(), getProtocol(),
                               "\"Ingest\"", getFedoraAppServerContext()};

        Ingest.main(parameters);
        atom.delete();

        try {
            byte[] objectXML = TypeUtility.convertDataHandlerToBytes(apim.getObjectXML("demo:1000"));
            assertTrue(objectXML.length > 0);
            String xmlIn = new String(objectXML, "UTF-8");
            assertXpathExists("foxml:digitalObject[@PID='demo:1000']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            assertXpathEvaluatesTo("6", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
        } finally {
            apim.purgeObject("demo:1000", "", false);
        }
    }

    @Test
    public void testIngestATOM_ZIP() throws Exception {
        System.out.println("Testing Ingest with ATOM_ZIP format");
        File atom = File.createTempFile("demo_1001", ".zip");
        FileOutputStream fileWriter = new FileOutputStream(atom);
        fileWriter.write(TestAPIM.demo1001ATOMZip);
        fileWriter.close();

        String[] parameters = {"f ", atom.getAbsolutePath(),
                               ATOM_ZIP1_1.uri, getHost() + ":" + getPort(),
                               getUsername(), getPassword(), getProtocol(),
                               "\"Ingest\"", getFedoraAppServerContext()};

        Ingest.main(parameters);
        atom.delete();

        try {
            byte[] objectXML = TypeUtility.convertDataHandlerToBytes(apim.getObjectXML("demo:1001"));
            assertTrue(objectXML.length > 0);
            String xmlIn = new String(objectXML, "UTF-8");
            assertXpathExists("foxml:digitalObject[@PID='demo:1001']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            assertXpathEvaluatesTo("3", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
        } finally {
            apim.purgeObject("demo:1001", "", false);
        }
    }

    @Test
    public void testExportFOXML10() throws Exception {
        System.out.println("Testing Export in FOXML 1.0 format");
        apim.ingest(TypeUtility.convertBytesToDataHandler(TestAPIM.demo998FOXMLObjectXML), FOXML1_1.uri, "Ingest for test");

        try {
            File temp = File.createTempFile("temp", "");
            String[] parameters = {getHost() + ":" + getPort(),
                                   getUsername(), getPassword(), "demo:998", FOXML1_0.uri,
                                   "public", temp.getParent(), "http", getFedoraAppServerContext()};

            Export.main(parameters);
            File foxml10 = new File(temp.getParent() + "/demo_998.xml");
            FileInputStream fileReader = new FileInputStream(foxml10);
            byte[] objectXML = new byte[fileReader.available()];
            fileReader.read(objectXML);
            fileReader.close();
            String xmlIn = new String(objectXML, "UTF-8");
            assertXpathExists("foxml:digitalObject[@PID='demo:998']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            assertXpathEvaluatesTo("5", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
            assertXpathNotExists("foxml:digitalObject[@VERSION='1.1']", xmlIn);

            temp.delete();
            foxml10.delete();
        } finally {
            apim.purgeObject("demo:998", "Purge test object", false);
        }
    }

    @Test
    public void testExportFOXML11() throws Exception {
        System.out.println("Testing Export in FOXML 1.1 format");
        apim.ingest(TypeUtility.convertBytesToDataHandler(TestAPIM.demo998FOXMLObjectXML), FOXML1_1.uri, "Ingest for test");

        try {
            File temp = File.createTempFile("temp", "");
            String[] parameters = {getHost() + ":" + getPort(),
                                   getUsername(), getPassword(), "demo:998", FOXML1_1.uri,
                                   "public", temp.getParent(), "http", getFedoraAppServerContext()};

            Export.main(parameters);
            File foxml11 = new File(temp.getParent() + "/demo_998.xml");
            String xmlIn = fileAsUTFString(foxml11);
            assertXpathExists("foxml:digitalObject[@PID='demo:998']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            // Audit stream disabled in code
            //assertXpathEvaluatesTo("6", "count(//foxml:datastream)", xmlIn);
            assertXpathEvaluatesTo("5", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
            assertXpathExists("foxml:digitalObject[@VERSION='1.1']", xmlIn);

            temp.delete();
            foxml11.delete();
        } finally {
            apim.purgeObject("demo:998", "Purge test object", false);
        }
    }

    @Test
    public void testBulkExportFOXML11Syntax() throws Exception {
        System.out.println("Testing Export in FOXML 1.1 format");
        apim.ingest(TypeUtility.convertBytesToDataHandler(TestAPIM.demo998FOXMLObjectXML), FOXML1_1.uri, "Ingest for test");

        try {
            File temp = File.createTempFile("temp", "");
            String[] parameters = {getHost() + ":" + getPort(),
                                   getUsername(), getPassword(), "FTYPS", "default",
                                   "default", temp.getParent(), "http", getFedoraAppServerContext()};

            Export.main(parameters);
            File foxml11 = new File(temp.getParent() + "/demo_998.xml");
            assertTrue(
            		"Expected export file " + foxml11.getAbsolutePath() +
            		" does not exist!", foxml11.exists());
            String xmlIn = fileAsUTFString(foxml11);
            assertXpathExists("foxml:digitalObject[@PID='demo:998']", xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#state' and @VALUE='Active']",
                    xmlIn);
            assertXpathExists(
                    "//foxml:objectProperties/foxml:property[@NAME='info:fedora/fedora-system:def/model#label' and @VALUE='Data Object (Coliseum) for Local Simple Image Demo']",
                    xmlIn);
            assertXpathEvaluatesTo("5", "count(//foxml:datastream)", xmlIn);
            assertXpathNotExists("//foxml:disseminator", xmlIn);
            assertXpathExists("foxml:digitalObject[@VERSION='1.1']", xmlIn);

            temp.delete();
            foxml11.delete();
        } finally {
            apim.purgeObject("demo:998", "Purge test object", false);
        }
    }

    @Test
    public void testExportMETS11() throws Exception {
        System.out.println("Testing Export in METS 1.1 format");
        apim.ingest(TypeUtility.convertBytesToDataHandler(TestAPIM.demo998FOXMLObjectXML), FOXML1_1.uri, "Ingest for test");

        try {
            File temp = File.createTempFile("temp", "");
            String[] parameters = {getHost() + ":" + getPort(),
                                   getUsername(), getPassword(), "demo:998", METS_EXT1_1.uri,
                                   "public", temp.getParent(), "http", getFedoraAppServerContext()};

            Export.main(parameters);
            File mets = new File(temp.getParent() + "/demo_998.xml");
            String xmlIn = fileAsUTFString(mets);
            assertXpathExists("METS:mets[@OBJID='demo:998']", xmlIn);
            assertXpathExists("METS:mets[@LABEL='Data Object (Coliseum) for Local Simple Image Demo']", xmlIn);
            assertXpathExists("METS:mets[@EXT_VERSION='1.1']", xmlIn);
            assertXpathEvaluatesTo("4", "count(//METS:fileGrp[@STATUS='A'])", xmlIn);

            temp.delete();
            mets.delete();
        } finally {
            apim.purgeObject("demo:998", "Purge test object", false);
        }
    }

    @Test
    public void testExportMETS10() throws Exception {
        System.out.println("Testing Export in METS 1.0 format");
        apim.ingest(TypeUtility.convertBytesToDataHandler(TestAPIM.demo998FOXMLObjectXML), FOXML1_1.uri, "Ingest for test");

        try {
            File temp = File.createTempFile("temp", "");
            String[] parameters = {getHost() + ":" + getPort(),
                                   getUsername(), getPassword(), "demo:998", METS_EXT1_0.uri,
                                   "public", temp.getParent(), "http", getFedoraAppServerContext()};

            Export.main(parameters);
            File mets = new File(temp.getParent() + "/demo_998.xml");
            String xmlIn = fileAsUTFString(mets);
            assertXpathExists("METS:mets[@OBJID='demo:998']", xmlIn);
            assertXpathExists("METS:mets[@LABEL='Data Object (Coliseum) for Local Simple Image Demo']", xmlIn);
            assertXpathNotExists("METS:mets[@EXT_VERSION='1.1']", xmlIn);
            assertXpathEvaluatesTo("4", "count(//METS:fileGrp[@STATUS='A'])", xmlIn);

            temp.delete();
            mets.delete();
        } finally {
            apim.purgeObject("demo:998", "Purge test object", false);
        }
    }

    @Test
    public void testExportATOM() throws Exception {
        System.out.println("Testing Export in ATOM format");
        apim.ingest(TypeUtility.convertBytesToDataHandler(TestAPIM.demo998FOXMLObjectXML), FOXML1_1.uri, "Ingest for test");

        try {
            File temp = File.createTempFile("temp", "");
            String[] parameters = {getHost() + ":" + getPort(),
                                   getUsername(), getPassword(), "demo:998", ATOM1_1.uri,
                                   "public", temp.getParent(), "http", getFedoraAppServerContext()};

            Export.main(parameters);
            File atom = new File(temp.getParent() + "/demo_998.xml");
            String xmlIn = fileAsUTFString(atom);
            // FIXME: Determine how to perform xpath tests with default namespace
            assertTrue(xmlIn.indexOf("<id>info:fedora/demo:998</id>") > -1);
            assertTrue(
                    xmlIn.indexOf("<title type=\"text\">Data Object (Coliseum) for Local Simple Image Demo</title>") >
                    -1);
            // assertXpathEvaluatesTo("info:fedora/demo:998", "feed/id", xmlIn);
            // assertXpathEvaluatesTo("Data Object (Coliseum) for Local Simple Image Demo", "feed/title[@type='text']", xmlIn);
            // assertXpathEvaluatesTo("6", "count(feed/entry)", xmlIn);

            temp.delete();
            atom.delete();
        } finally {
            apim.purgeObject("demo:998", "Purge test object", false);
        }
    }

    public void testExportATOM_ZIP() throws Exception {
        System.out.println("Testing Export in ATOM_ZIP format");
        apim.ingest(TypeUtility.convertBytesToDataHandler(TestAPIM.demo998FOXMLObjectXML), FOXML1_1.uri, "Ingest for test");

        try {
            File temp = File.createTempFile("temp", "");
            String[] parameters = {getHost() + ":" + getPort(),
                                   getUsername(), getPassword(), "demo:998", ATOM_ZIP1_1.uri,
                                   "archive", temp.getParent(), "http", getFedoraAppServerContext()};

            Export.main(parameters);
            File atom = new File(temp.getParent() + "/demo_998.zip");

            ZipInputStream zip = new ZipInputStream(new FileInputStream(atom));
            ZipEntry entry;
            int count = 0;
            while ((entry = zip.getNextEntry()) != null) {
                if (entry.getName().equals("atommanifest.xml")) {
                    count++;
                    ByteArrayOutputStream manifest = new ByteArrayOutputStream();
                    FileUtils.copy(zip, manifest);

                    Abdera abdera = Abdera.getInstance();
                    Parser parser = abdera.getParser();
                    Document<Feed> feedDoc = parser.parse(new StringReader(manifest.toString("UTF-8")));
                    Feed feed = feedDoc.getRoot();
                    assertEquals(PID.getInstance("demo:998").toURI(), feed.getId().toString());
                    // TODO other tests?
                }
            }
            assertEquals("Expected exactly 1 manifest file", 1, count);
            zip.close();

            temp.delete();
            atom.delete();
        } finally {
            apim.purgeObject("demo:998", "Purge test object", false);
        }
    }
    
    private static String fileAsUTFString(File input) throws IOException {
        FileInputStream fileReader = new FileInputStream(input);
        byte[] objectXML = new byte[fileReader.available()];
        fileReader.read(objectXML);
        fileReader.close();
        return new String(objectXML, "UTF-8");
    }

    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TestCommandLineFormats.class);
    }
}
