
package org.fcrepo.test.integration;

import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.JUnit4TestAdapter;

import org.apache.commons.io.IOUtils;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.server.utilities.TypeUtility;
import org.fcrepo.test.FedoraServerTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestObjectLastModDate
        extends FedoraServerTestCase {

    private org.fcrepo.server.access.FedoraAPIAMTOM apia;

    private org.fcrepo.server.management.FedoraAPIMMTOM apim;

    private static final String FOXMLPATH = "fcrepo238.xml";

    private final DateFormat dateFormat =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static final String RESOURCEBASE =
            System.getProperty("fcrepo-integrationtest-core.classes") != null ? System
                    .getProperty("fcrepo-integrationtest-core.classes")
                    + "test-objects/foxml/cli-utils"
                    : "src/test/resources/test-objects/foxml/cli-utils";

    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TestObjectLastModDate.class);
    }

    @Before
    public void setUp() throws Exception {
        FedoraClient client = getFedoraClient();
        apia = client.getAPIAMTOM();
        apim = client.getAPIMMTOM();
        client.shutdown();
    }

    @Test
    public void testFCREPO238() throws Exception {
        String pid =
                apim.ingest(TypeUtility.convertBytesToDataHandler(IOUtils.toByteArray(new FileInputStream(RESOURCEBASE
                                    + "/" + FOXMLPATH))),
                            FOXML1_1.uri,
                            "testing fcrepo 238");
        org.fcrepo.server.types.gen.ObjectProfile profile = apia.getObjectProfile(pid, null);
        Date objDate = dateFormat.parse(profile.getObjLastModDate());
        for (org.fcrepo.server.types.gen.DatastreamDef dd : apia.listDatastreams(pid, null)) {
            org.fcrepo.server.types.gen.Datastream ds = apim.getDatastream(pid, dd.getID(), null);
            Date dsDate = dateFormat.parse(ds.getCreateDate());
            System.out.print("object:" + dateFormat.format(objDate) + ", ");
            System.out.println("datastream: " + dateFormat.format(dsDate));
            Assert.assertTrue("object last modificaton date is before datastream's create date. check FCREPO-238",
                              objDate.compareTo(dsDate) > -1);
        }
        apim.purgeObject(pid, "removing testobject", true);
    }
}
