/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.test.integration.cma;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;

import org.fcrepo.client.FedoraClient;
import org.fcrepo.client.utility.AutoPurger;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.test.FedoraServerTestCase;

import static org.fcrepo.test.integration.cma.Util.ingestTestObjects;
import static org.junit.Assert.assertTrue;


/**
 * Tests for reasonable/predictable behaviour when sDeps conflict.
 * <p>
 * Two Service Deployment objects (SDeps) deployed simultaneously for the same
 * service on the same content model creates a conflict. In general, this is a
 * problem - don't do it. However, there are a few legitimate reasons for
 * wanting to do so, primarily when replacing one SDep with another. Faced with
 * a choice of first removing the old SDep, then replacing it vs. first
 * ingesting/activating a new one, then subsequently removing the old one, we
 * notice that the former situation guarantees a period of time in which there
 * is no SDep for a given service - which may be problematic in situations where
 * downtime is unacceptable.
 * </p>
 * <p>
 * In order to support replacing sDeps without downtime, Fedora adopts the
 * following policy: If two or more SDep objects deploy the same service for the
 * same content model, then the object with the <em>earliest</em> modification
 * date will be used. Thus, a newly ingested or modified SDep will have a more
 * recent last modified date, and will be ignored until the existing SDep is
 * purged or modified.
 * </p>
 * These tests verify the above behavior.
 *
 * @author birkland
 */
public class ConflictingDeploymentTests {

    private static final String DEMO_OBJECT_BASE =
            "cma-examples/conflicting-deployments";

    private static final String PUBLIC_OBJECT_BASE =
            DEMO_OBJECT_BASE + "/public-objects";

    private static final String DEPLOYMENT_1_BASE =
            DEMO_OBJECT_BASE + "/sdeps/1";

    private static final String DEPLOYMENT_2_BASE =
            DEMO_OBJECT_BASE + "/sdeps/2";

    private final String SDEF_PID = "demo:conflicting-deployment.sdef";

    private final String OBJECT_PID = "demo:conflicting-deployment.object";

    private final String SDEP_1_PID = "demo:conflicting-deployment.sdep1";

    private final String SDEP_2_PID = "demo:conflicting-deployment.sdep2";

    private final String METHOD_NAME = "content";

    private static FedoraClient s_client;

    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(ConflictingDeploymentTests.class);
    }

    @BeforeClass
    public static void bootStrap() throws Exception {
        s_client =
                new FedoraClient(FedoraServerTestCase.getBaseURL(),
                                 FedoraServerTestCase.getUsername(),
                                 FedoraServerTestCase.getPassword());
        ingestTestObjects(s_client, PUBLIC_OBJECT_BASE);
    }
    
    @AfterClass
    public static void cleanUp() {
        s_client.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        int ingested = 0;
        ingested = ingestTestObjects(s_client, DEPLOYMENT_1_BASE);
        assertTrue("No objects were ingested from " + Util.resourcePath(DEPLOYMENT_1_BASE), ingested > 0);
        ingested = ingestTestObjects(s_client, DEPLOYMENT_2_BASE);
        assertTrue("No objects were ingested from " + Util.resourcePath(DEPLOYMENT_2_BASE), ingested > 0);
    }

    @After
    public void tearDown() throws Exception {
        FedoraAPIMMTOM apim = s_client.getAPIMMTOM();
        AutoPurger.purge(apim, SDEP_1_PID, null);
        AutoPurger.purge(apim, SDEP_2_PID, null);
    }

    /**
     * If two sDeps are ingested, the first one ingested should be the one to
     * drive the dissemination. Ingest in order 1,2
     */
    @Test
    public void testDeployFirstIngested12() throws Exception {

        String content = getDisseminatedContent();

        Assert.assertFalse("Wrong deployment used!" + content, content
                .contains("CONTENT_2"));
        Assert.assertTrue("Did not disseminate expected content", content
                .contains("CONTENT_1"));

    }

    /**
     * If two sDeps are ingested, the first one ingested should be the one to
     * drive the dissemination. Ingest in order 2,1
     */
    @Test
    public void testDeployFirstIngested21() throws Exception {

        String content = getDisseminatedContent();

        Assert.assertFalse("Wrong deployment used!", content
                .contains("CONTENT_2"));
        Assert.assertTrue("Did not disseminate expected content", content
                .contains("CONTENT_1"));

    }

    /**
     * Modifying the oldest SDep will make it the newest, thus switching the
     * SDep used.
     */
    @Test
    public void testModifyOldestSdep() throws Exception {

        modify(SDEP_1_PID);

        String content = getDisseminatedContent();

        /* Now deployment 2 should be oldest */
        Assert.assertFalse("Wrong deployment used!", content
                .contains("CONTENT_1"));
        Assert.assertTrue("Did not disseminate expected content", content
                .contains("CONTENT_2"));
    }

    /**
     * Modifying the newest SDep should have no effect
     */
    @Test
    public void testModifyNewestSdep() throws Exception {

        modify(SDEP_2_PID);

        String content = getDisseminatedContent();

        /* Now deployment 2 should be oldest */
        Assert.assertFalse("Wrong deployment used!", content
                .contains("CONTENT_2"));
        Assert.assertTrue("Did not disseminate expected content", content
                .contains("CONTENT_1"));
    }

    /**
     * Represents the most likely case. Should pass with flying colours.
     */
    @Test
    public void testPurgeReplace() throws Exception {

        s_client.getAPIMMTOM()
                .purgeObject(SDEP_1_PID, "removing first sDep", false);
        try{
            Assert.assertTrue("Did not disseminate expected content: ",
                    getDisseminatedContent().contains("CONTENT_2"));
        } finally {
            // this object is re-ingested here solely because the cleanup/purge
            // can't handle the potentially missing item
            ingestTestObjects(s_client, DEPLOYMENT_1_BASE);
        }
    }

    private void modify(String pid) throws Exception {
        s_client.getAPIMMTOM().addRelationship(pid,
                                           "http://example.org/isModified",
                                           "true",
                                           true,
                                           null);
    }

    private String getDisseminatedContent() throws Exception {
        return Util.getDissemination(s_client,
                                     OBJECT_PID,
                                     SDEF_PID,
                                     METHOD_NAME);
    }
}