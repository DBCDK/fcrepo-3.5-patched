/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.test.api;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.security.MessageDigest;
import java.util.Date;

import javax.xml.ws.soap.SOAPFaultException;

import junit.framework.JUnit4TestAdapter;

import org.apache.abdera.Abdera;
import org.apache.abdera.ext.thread.ThreadHelper;
import org.apache.abdera.i18n.iri.IRI;
import org.apache.abdera.model.Entry;
import org.apache.abdera.model.Feed;
import org.apache.abdera.model.Link;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.PID;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.server.types.gen.ArrayOfString;
import org.fcrepo.server.utilities.StringUtility;
import org.fcrepo.server.utilities.TypeUtility;
import org.fcrepo.test.FedoraServerTestCase;
import org.fcrepo.utilities.Foxml11Document;
import org.fcrepo.utilities.Foxml11Document.ControlGroup;
import org.fcrepo.utilities.Foxml11Document.Property;
import org.fcrepo.utilities.Foxml11Document.State;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * @author Edwin Shin
 * @version $Id$
 * @since 3.0
 */
public class TestManagedDatastreams
        extends FedoraServerTestCase {

    private static final String BAD_URL = "Malformed URL:";

    private static final String NON_EXIST_UPLOAD = "does not match an existing file";

    private static FedoraClient s_client;
    
    private FedoraAPIMMTOM apim;

    private Abdera abdera;

    private final String[] copyTempFileLocations = {
            "copy:///tmp/foo.txt",
            "copy://tmp/foo.txt",
            "copy://../etc/passwd",
            "temp:///tmp/foo.txt",
            "temp://tmp/foo.txt",
            "temp://../etc/passwd"
    };

    private final String[] uploadedLocations = {
            "uploaded:///tmp/foo.txt",
            "uploaded://tmp/foo.txt"};

    @BeforeClass
    public static void bootStrap() throws Exception {
        s_client = getFedoraClient();
    }
    
    @AfterClass
    public static void cleanUp() {
        s_client.shutdown();
    }


    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        abdera = Abdera.getInstance();
        apim = s_client.getAPIMMTOM();
        System.setProperty("fedoraServerHost", "localhost");
        System.setProperty("fedoraServerPort", "8080");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testIngest() throws Exception {
        String pid = "demo:m_ds_test";

        for (String contentLocation : copyTempFileLocations) {
            try {
                apim.ingest(TypeUtility.convertBytesToDataHandler(getAtomObject(pid, contentLocation)), ATOM1_1.uri, null);
                fail("ingest should have failed with " + contentLocation);
            } catch (SOAPFaultException e) {
                assertTrue(e.getMessage(), e.getMessage().contains(BAD_URL));
            }
            try {
                apim.ingest(TypeUtility.convertBytesToDataHandler(getFoxmlObject(pid, contentLocation)), FOXML1_1.uri, null);
                fail("ingest should have failed with " + contentLocation);
            } catch (SOAPFaultException e) {
                assertTrue(e.getMessage(), e.getMessage().contains(BAD_URL));
            }
        }

        for (String contentLocation : uploadedLocations) {
            try {
                apim.ingest(TypeUtility.convertBytesToDataHandler(getAtomObject(pid, contentLocation)), ATOM1_1.uri, null);
                fail("ingest should have failed with " + contentLocation);
            } catch (SOAPFaultException e) {
                assertTrue(e.getMessage(), e.getMessage().contains(NON_EXIST_UPLOAD));
            }
            try {
                apim.ingest(TypeUtility.convertBytesToDataHandler(getFoxmlObject(pid, contentLocation)), FOXML1_1.uri, null);
                fail("ingest should have failed with " + contentLocation);
            } catch (SOAPFaultException e) {
                assertTrue(e.getMessage(), e.getMessage().contains(NON_EXIST_UPLOAD));
            }
        }
    }

    @Test
    public void testAddDatastream() throws Exception {
        String pid = "demo:m_ds_test_add";

        apim.ingest(TypeUtility.convertBytesToDataHandler(getAtomObject(pid, null)), ATOM1_1.uri, null);

        try {
            for (String contentLocation : copyTempFileLocations) {
                try {
                    addDatastream(pid, contentLocation);
                    fail("addDatastream should have failed with "
                         + contentLocation);
                } catch (SOAPFaultException e) {
                    assertTrue(e.getMessage(), e.getMessage().contains(BAD_URL));
                }
            }

            for (String contentLocation : uploadedLocations) {
                try {
                    addDatastream(pid, contentLocation);
                    fail("addDatastream should have failed with "
                         + contentLocation);
                } catch (SOAPFaultException e) {
                    assertTrue(e.getMessage(), e.getMessage().contains(NON_EXIST_UPLOAD));
                }
            }

        } finally {
            apim.purgeObject(pid, "test", false);
        }
    }

    @Test
    public void testModifyDatastreamByReference() throws Exception {
        String pid = "demo:m_ds_test_add";
        String dsLocation = getBaseURL() + "/get/fedora-system:ContentModel-3.0/DC";
        apim.ingest(TypeUtility.convertBytesToDataHandler(getAtomObject(pid, dsLocation)), ATOM1_1.uri, null);

        try {
            for (String contentLocation : copyTempFileLocations) {
                try {
                    modifyDatastreamByReference(pid, contentLocation);
                    fail("modifyDatastreamByReference should have failed with " + contentLocation);
                } catch (SOAPFaultException e) {
                    assertTrue(e.getMessage(), e.getMessage().contains(BAD_URL));
                }
            }

            for (String contentLocation : uploadedLocations) {
                try {
                    modifyDatastreamByReference(pid, contentLocation);
                    fail("modifyDatastreamByReference should have failed with " + contentLocation);
                } catch (SOAPFaultException e) {
                    assertTrue(e.getMessage(), e.getMessage().contains(NON_EXIST_UPLOAD));
                }
            }

            // A null contentLocation should cause the server to generate a
            // copy:// url
            modifyDatastreamByReference(pid, null);
        } finally {
            apim.purgeObject(pid, "test", false);
        }
    }

    @Test
    public void testAddDatastreamWithChecksum() throws Exception {
        String pid = "demo:m_ds_test_add";
        String checksumType = "MD5";
        apim.ingest(TypeUtility.convertBytesToDataHandler(getAtomObject(pid, null)), ATOM1_1.uri, null);
        File temp = null;

        try {
            temp = File.createTempFile("foo", "bar");
            String contentLocation = getFedoraClient().uploadFile(temp);
            String checksum = computeChecksum(checksumType, new FileInputStream(temp));
            String dsId = addDatastream(pid, contentLocation, checksumType, checksum);
            assertEquals("DS", dsId);

            // Now ensure that bogus checksums do indeed fail
            apim.purgeDatastream(pid, dsId, null, null, null, false);
            checksum = "bogus";
            try {
                addDatastream(pid, contentLocation, checksumType, checksum);
                fail("Adding datastream with bogus checksum should have failed.");
            } catch (SOAPFaultException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("Checksum Mismatch"));
            }
        } finally {
            apim.purgeObject(pid, "test", false);
            if (temp != null) {
                temp.delete();
            }
        }
    }

    @Test
    public void testModifyDatastreamByReferenceWithChecksum() throws Exception {
        String pid = "file:m_ds_test_add"; // file pid namespace should work as well as demo
        String checksumType = "MD5";
        apim.ingest(TypeUtility.convertBytesToDataHandler(getAtomObject(pid, null)), ATOM1_1.uri, null);
        File temp = null;
        try {
            temp = File.createTempFile("foo", "bar");
            String contentLocation = getFedoraClient().uploadFile(temp);
            String checksum = computeChecksum(checksumType, new FileInputStream(temp));
            String dsId = addDatastream(pid, contentLocation, checksumType, checksum);
            assertEquals("DS", dsId);

            FileOutputStream os = new FileOutputStream(temp);
            os.write("testModifyDatastreamByReferenceWithChecksum".getBytes());
            os.close();
            contentLocation = getFedoraClient().uploadFile(temp);
            checksum = computeChecksum(checksumType, new FileInputStream(temp));
            modifyDatastreamByReference(pid, contentLocation, checksumType, checksum);

            // Now ensure that bogus checksums do indeed fail
            checksum = "bogus";
            try {
                modifyDatastreamByReference(pid, contentLocation, checksumType, checksum);
                fail("Modifying datastream with bogus checksum should have failed.");
            } catch (SOAPFaultException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("Checksum Mismatch"));
            }
        } finally {
            apim.purgeObject(pid, "test", false);
            if (temp != null) {
                temp.delete();
            }
        }
    }

    private byte[] getAtomObject(String pid, String contentLocation) throws Exception {
        Feed feed = createAtomObject(pid, contentLocation);

        Writer sWriter = new StringWriter();
        feed.writeTo("prettyxml", sWriter);
        return sWriter.toString().getBytes("UTF-8");
    }

    private byte[] getFoxmlObject(String pid, String contentLocation) throws Exception {
        Foxml11Document doc = createFoxmlObject(pid, contentLocation);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        doc.serialize(out);
        return out.toByteArray();
    }

    private String addDatastream(String pid, String contentLocation)
            throws Exception {
        return addDatastream(pid, contentLocation, null, null);
    }

    private String addDatastream(String pid, String contentLocation, String checksumType, String checksum)
            throws Exception {
        return apim.addDatastream(pid,
                                  "DS",
                                  null,
                                  "testManagedDatastreams",
                                  true,
                                  "text/plain",
                                  "",
                                  contentLocation,
                                  "M",
                                  "A",
                                  checksumType,
                                  checksum,
                                  "testManagedDatastreams");
    }

    private String modifyDatastreamByReference(String pid,
                                               String contentLocation)
            throws Exception {
        return modifyDatastreamByReference(pid, contentLocation, null, null);
    }

    private String modifyDatastreamByReference(String pid,
                                               String contentLocation,
                                               String checksumType,
                                               String checksum)
            throws Exception {
        return apim.modifyDatastreamByReference(pid,
                                                "DS",
                                                new ArrayOfString(),
                                                "testManagedDatastreams",
                                                "text/plain",
                                                "",
                                                contentLocation,
                                                checksumType,
                                                checksum,
                                                "testManagedDatastreams",
                                                false);
    }

    private Feed createAtomObject(String spid, String contentLocation) throws Exception {
        PID pid = PID.getInstance(spid);
        Date date = new Date(1);
        String title = "title";
        String author = "org.fcrepo.test.api.TestManagedDatastreams";

        Feed feed = abdera.newFeed();
        feed.setId(pid.toURI());
        feed.setTitle(title);
        feed.setUpdated(date);
        feed.addAuthor(author);

        if (contentLocation != null && contentLocation.length() > 0) {
            addAtomManagedDatastream(feed, contentLocation);
        }

        return feed;
    }

    private void addAtomManagedDatastream(Feed feed, String contentLocation) throws Exception {
        String dsId = "DS";

        Entry dsEntry = feed.addEntry();
        dsEntry.setId(feed.getId().toString() + "/" + dsId);

        Entry dsvEntry = feed.addEntry();
        dsvEntry.setId(dsEntry.getId().toString() + "/" + feed.getUpdatedString());

        dsEntry.setTitle(feed.getTitle());
        dsEntry.setUpdated(feed.getUpdated());
        dsEntry.addLink(dsvEntry.getId().toString(), Link.REL_ALTERNATE);
        dsEntry.addCategory(MODEL.STATE.uri, "A", null);
        dsEntry.addCategory(MODEL.CONTROL_GROUP.uri, "M", null);
        dsEntry.addCategory(MODEL.VERSIONABLE.uri, "true", null);

        dsvEntry.setTitle(feed.getTitle());
        dsvEntry.setUpdated(feed.getUpdated());
        ThreadHelper.addInReplyTo(dsvEntry, dsEntry.getId());
        dsvEntry.setSummary("summary");
        dsvEntry.setContent(new IRI(contentLocation), "text/plain");
    }

    private Foxml11Document createFoxmlObject(String spid, String contentLocation) throws Exception {
        PID pid = PID.getInstance(spid);
        Date date = new Date(1);

        Foxml11Document doc = new Foxml11Document(pid.toString());
        doc.addObjectProperty(Property.STATE, "A");

        if (contentLocation != null && contentLocation.length() > 0) {
            String ds = "DS";
            String dsv = "DS1.0";
            doc.addDatastream(ds, State.A, ControlGroup.M, true);
            doc.addDatastreamVersion(ds, dsv, "text/plain", "label", 1, date);
            doc.setContentLocation(dsv, contentLocation, org.fcrepo.server.storage.types.Datastream.DS_LOCATION_TYPE_URL);
        }
        return doc;
    }

    private String computeChecksum(String csType, InputStream is) throws Exception {
        MessageDigest md = MessageDigest.getInstance(csType);
        if (is == null) {
            throw new IllegalArgumentException("InputStream cannot be null");
        }
        byte buffer[] = new byte[5000];
        int numread;
        while ((numread = is.read(buffer, 0, 5000)) > 0) {
            md.update(buffer, 0, numread);
        }
        return StringUtility.byteArraytoHexString(md.digest());
    }

    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TestManagedDatastreams.class);
    }
}
