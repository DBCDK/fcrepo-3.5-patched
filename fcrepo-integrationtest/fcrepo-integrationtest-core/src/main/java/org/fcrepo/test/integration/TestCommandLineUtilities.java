/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.test.integration;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;

import junit.framework.JUnit4TestAdapter;

import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.Constants;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.test.FedoraServerTestCase;
import org.fcrepo.utilities.ExecUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;

/**
 * @author Edwin Shin
 */
public class TestCommandLineUtilities
        extends FedoraServerTestCase
        implements Constants {

    static ByteArrayOutputStream sbOut = null;

    static ByteArrayOutputStream sbErr = null;

    static TestCommandLineUtilities curTest = null;
    
    private static FedoraClient s_client;

    private static final String RESOURCEBASE =
        System.getProperty("fcrepo-integrationtest-core.classes") != null ? System
                .getProperty("fcrepo-integrationtest-core.classes")
                + "test-objects/foxml/cli-utils"
                : "src/test/resources/test-objects/foxml/cli-utils";

    private static final String LOGDIR =
        RESOURCEBASE + "/logs";

    private static final String BUILDDIR =
        RESOURCEBASE + "/temp";

    private static File logDir = null;
    private static File buildDir = null;
    
    @BeforeClass
    public static void bootStrap() throws Exception {
        String baseURL =
                getProtocol() + "://" + getHost() + ":" + getPort() + "/"
                        + getFedoraAppServerContext();
        s_client = new FedoraClient(baseURL, getUsername(), getPassword());
        // do we actually use the demo objects in this suite of tests?
        //ingestDemoObjects(s_client);
    }
    
    @AfterClass
    public static void cleanUp() throws Exception {
        purgeDemoObjects(s_client);
        s_client.shutdown();
    }

    @Test
    public void testFedoraIngestAndPurge() {
        System.out.println("Ingesting object test:1001");
        ingestFoxmlFile(new File(RESOURCEBASE
                                 + "/test1001.xml"));
        String out = sbOut.toString();
        String err = sbErr.toString();
        if (out.indexOf("Ingested pid: test:1001") == -1) {
            System.err.println("Command-line ingest failed: STDOUT='" + out
                    + "', STDERR='" + err + "'");
        }
        assertEquals(true, out.indexOf("Ingested pid: test:1001") != -1);

        System.out.println("Purging object test:1001");
        System.out.println("FEDORA-HOME = " + FEDORA_HOME);
        purgeUsingScript("test:1001");
        assertEquals("Expected empty STDERR output, got '" + sbErr.toString()
                     + "'", 0, sbErr.size());

        System.out.println("Ingest and purge test succeeded");
    }

    @Test
    public void testBatchBuildAndBatchIngestAndPurge() throws Exception {
        System.out.println("Building batch objects");
        batchBuild(new File(RESOURCEBASE
                            + "/templates/foxml-template.xml"),
                   new File(RESOURCEBASE
                            + "/batch-objs"),
                   buildDir,
                   new File(LOGDIR + "/build.log"));
        String err = sbErr.toString();
        assertEquals(err,
                     true,
                     err
                             .indexOf("10 Fedora FOXML XML documents successfully created") != -1);
        System.out.println("Ingesting batch objects");
        batchIngest(buildDir,
                    new File(LOGDIR + "/junit_ingest.log"));
        err = sbErr.toString();
        assertTrue("Response did not contain expected string re: objects successfully ingested: <reponse>"
                + err + "</reponse>",
                err.contains("10 objects successfully ingested into Fedora"));
        String batchObjs[] =
                {"test:0001", "test:0002", "test:0003", "test:0004",
                 "test:0005", "test:0006", "test:0007", "test:0008",
                 "test:0009", "test:0010"};
        System.out.println("Purging batch objects");
        purgeFast(batchObjs, "testBatchBuildAndBatchIngestAndPurge-purge(array)");
        System.out.println("Build and ingest test succeeded");
    }

    @Test
    public void testBatchBuildIngestAndPurge() throws Exception {
        System.out.println("Building and Ingesting batch objects");
        batchBuildIngest(new File(RESOURCEBASE
                                  + "/templates/foxml-template.xml"),
                         new File(RESOURCEBASE
                                  + "/batch-objs"),
                         buildDir,
                         new File(LOGDIR
                                  + "/junit_buildingest.log"));
        String err = sbErr.toString();
        assertTrue("Response did not contain expected string re: FOXML XML documents: <reponse>"
                             + err + "</response>",
                     err.contains("10 Fedora FOXML XML documents successfully created"));
        assertTrue("Response did not contain expected string re: objects successfully ingested: <reponse>"
                             + err + "</reponse",
                     err.contains("10 objects successfully ingested into Fedora"));
        String batchObjs[] =
                {"test:0001", "test:0002", "test:0003", "test:0004",
                 "test:0005", "test:0006", "test:0007", "test:0008",
                 "test:0009", "test:0010"};
        System.out.println("Purging batch objects");
        purgeFast(batchObjs, "testBatchBuildIngestAndPurge-purge(array)");
        System.out.println("Build/ingest test succeeded");
    }

    @Test
    public void testBatchModify() throws Exception {
        // Note: test will fail if default control group for DC datastreams (fedora.fcfg) is not X
        // as the modify script specifies control group "X" when modifying DC
        System.out.println("Running batch modify of objects");
        batchModify(new File(RESOURCEBASE
                             + "/modify-batch-directives.xml"),
                    new File(LOGDIR + "/junit_modify.log"));
        String out = sbOut.toString();
        String err = sbErr.toString();
        if (out.indexOf("25 modify directives successfully processed.") == -1) {
            System.out.println(" out = " + out);
            System.out.println(" err = " + err);
        }

        if (out.indexOf("25 modify directives successfully processed.") == -1) {
            System.err.println(out);
        }
        assertEquals(String.format("%s; %s", out, err), false, out
                .indexOf("25 modify directives successfully processed.") == -1);
        assertEquals(String.format("%s; %s", out, err), false, out.indexOf("0 modify directives failed.") == -1);
        System.out.println("Purging batch modify object");
        purgeFast("test:1002", "testBatchModify-purge");
        System.out.println("Batch modify test succeeded");
    }

    @Test
    public void testBatchPurge() throws Exception {
        System.out.println("Batch purging objects from file containing PIDs");

        String base =
            System.getProperty("fcrepo-integrationtest-core.classes") != null ? System
                    .getProperty("fcrepo-integrationtest-core.classes")
                    : "src/test/resources/";

        /* first ingest the objects */
        batchBuildIngest(new File(RESOURCEBASE
                                  + "/templates/foxml-template.xml"),
                         new File(RESOURCEBASE
                                  + "/batch-objs"),
                         buildDir,
                         new File(LOGDIR
                                  + "/junit_buildingest.log"));
        /* try to purge from a bogus file */
        batchPurge(new File("/bogus/file"));
        String out = sbOut.toString();
        String err = sbErr.toString();
        assertTrue("Response did not contain expected string re: java.io.FileNotFoundException: <response>"
                     + err + "</response>",
                     err.contains("java.io.FileNotFoundException"));
        /* try to purge the objects from the valid file */
        batchPurge(new File(base + "/test-objects/test-batch-purge-file.txt"));
        out = sbOut.toString();
        err = sbErr.toString();
        assertTrue("Response did not contain expected string re: objects successfully purged: <response>"
                     + out + "</response>",
                     out.contains("10 objects successfully purged."));
        /* make sure they're gone */
        batchPurge(new File(base + "/test-objects/test-batch-purge-file.txt"));
        out = sbOut.toString();
        err = sbErr.toString();
        assertTrue("Response did not contain expected string re: Object not found in low-level storage: <response>"
                     + err + "</response>",
                     err.contains("Object not found in low-level storage"));
        assertTrue("Response did not contain expected string re: objects successfully purged: <response>"
                     + out + "</response>",
                     out.contains("0 objects successfully purged."));
        System.out.println("Batch purge test succeeded");
    }

    @Test
    public void testExport() throws Exception {
        System.out.println("Testing fedora-export");
        System.out.println("Ingesting object test:1001");
        ingestFoxmlFile(new File(RESOURCEBASE
                                 + "/test1001.xml"));
        String out = sbOut.toString();
        String err = sbErr.toString();
        if (out.indexOf("Ingested pid: test:1001") == -1) {
            System.err.println("Command-line ingest failed: STDOUT='" + out
                               + "', STDERR='" + err + "'");
        }
        assertEquals(true, out.indexOf("Ingested pid: test:1001") != -1);

        File outFile =
                new File(RESOURCEBASE + "/test_1001.xml");
        if (outFile.exists()) {
            outFile.delete();
        }
        System.out.println("Exporting object test:1001");
        exportObj("test:1001", new File(RESOURCEBASE));
        out = sbOut.toString();
        err = sbErr.toString();
        assertEquals(out.indexOf("Exported test:1001") != -1, true);
        File outFile2 =
                new File(RESOURCEBASE + "/test_1001.xml");
        assertEquals(outFile2.exists(), true);
        System.out.println("Deleting exported file");
        if (outFile2.exists()) {
            outFile2.delete();
        }
        purgeFast("test:1001", "testExport-purge");
        System.out.println("Export test succeeded");
    }

    @Test
    public void testValidatePolicy() {
        System.out.println("Testing Validate Policies");

        String base =
                System.getProperty("fcrepo-integrationtest-core.classes") != null ? System
                        .getProperty("fcrepo-integrationtest-core.classes")
                        : "src/test/resources/";

        File validDir =
                new File(base + "XACMLTestPolicies/valid-policies");
        traverseAndValidate(validDir, true);

        File invalidDir =
                new File(base + "XACMLTestPolicies/invalid-policies");
        traverseAndValidate(invalidDir, false);

        System.out.println("Validate Policies test succeeded");
    }

    @Test
    public void testFindObjects() {
        System.out.println("Testing Find Objects");
        execute(FEDORA_HOME + "/client/bin/fedora-find",
                getHost(),
                getPort(),
                getUsername(),
                getPassword(),
                "pid",
                "model",
                "http",
                getFedoraAppServerContext());
        assertEquals("Expected empty STDERR output, got '" + sbErr.toString()
                + "'", 0, sbErr.size());
        String out = sbOut.toString();
        assertNotNull(out);
        assertTrue(out.contains("#1"));
    }

    private void traverseAndValidate(File testDir, boolean expectValid) {
        //      assertEquals(testDir.isDirectory(), true);
        File testFiles[] = testDir.listFiles(new java.io.FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                if ((name.toLowerCase().startsWith("permit") || name
                        .toLowerCase().startsWith("deny"))
                        && name.endsWith(".xml")) {
                    return true;
                }
                return false;
            }
        });
        for (File element : testFiles) {
            System.out.println("Checking "
                    + (expectValid ? "valid" : "invalid") + " policy: "
                    + element.getName());
            execute(FEDORA_HOME + "/server/bin/validate-policy", element
                    .getAbsolutePath());
            String out = sbOut.toString();

            if (expectValid) {
                assertTrue("Expected \"Validation successful\", but received \""
                                   + out + "\"",
                           out.indexOf("Validation successful") != -1);
            } else {
                assertTrue("Expected \"Validation failed\", but received \""
                        + out + "\"", out.indexOf("Validation failed") != -1);
            }
        }
    }

    private void ingestFoxmlFile(File f) {
        execute(FEDORA_HOME + "/client/bin/fedora-ingest",
                "f",
                f.getAbsolutePath(),
                FOXML1_1.uri,
                getHost() + ":" + getPort(),
                getUsername(),
                getPassword(),
                getProtocol(),
                "junit-ingest",
                getFedoraAppServerContext());
    }

    private static void purgeUsingScript(String pid) {
        //    File exe = new File("client/bin/fedora-purge");
        execute(FEDORA_HOME + "/client/bin/fedora-purge",
                getHost() + ":" + getPort(),
                getUsername(),
                getPassword(),
                pid,
                getProtocol(),
                "junit-purge",
                getFedoraAppServerContext());
    }

    private static void purgeFast(String pid, String logMsg) throws Exception {
        getAPIM().purgeObject(pid, "purgeFast-single", false);
    }

    private static void purgeFast(String[] pids, String logMsg) throws Exception {
        FedoraAPIMMTOM apim = getAPIM();
        for (String element : pids) {
            apim.purgeObject(element, logMsg, false);
        }
    }

    private static FedoraAPIMMTOM getAPIM() throws Exception {
        FedoraAPIMMTOM result = s_client.getAPIMMTOM();
        return result;
    }

    private void batchBuild(File objectTemplateFile,
                            File objectSpecificDir,
                            File objectDir,
                            File logFile) {
        execute(FEDORA_HOME + "/client/bin/fedora-batch-build",
                objectTemplateFile.getAbsolutePath(),
                objectSpecificDir.getAbsolutePath(),
                objectDir.getAbsolutePath(),
                logFile.getAbsolutePath(),
                "text");
    }

    private void batchIngest(File objectDir, File logFile) {
        execute(FEDORA_HOME + "/client/bin/fedora-batch-ingest",
                objectDir.getAbsolutePath(),
                logFile.getAbsolutePath(),
                "text",
                FOXML1_1.uri,
                getHost() + ":" + getPort(),
                getUsername(),
                getPassword(),
                getProtocol(),
                getFedoraAppServerContext());
    }

    private void batchBuildIngest(File objectTemplateFile,
                                  File objectSpecificDir,
                                  File objectDir,
                                  File logFile) {
        execute(FEDORA_HOME + "/client/bin/fedora-batch-buildingest",
                objectTemplateFile.getAbsolutePath(),
                objectSpecificDir.getAbsolutePath(),
                objectDir.getAbsolutePath(),
                logFile.getAbsolutePath(),
                "text",
                getHost() + ":" + getPort(),
                getUsername(),
                getPassword(),
                getProtocol(),
                getFedoraAppServerContext());
    }

    private void batchPurge(File objectPurgeFile) {
        execute(FEDORA_HOME + "/client/bin/fedora-purge",
                getHost() + ":" + getPort(),
                getUsername(),
                getPassword(),
                objectPurgeFile.getAbsoluteFile().toURI().toString(),
                getProtocol(),
                "batch-purge",
                getFedoraAppServerContext());
    }

    private void batchModify(File batchDirectives, File logFile) {
        execute(FEDORA_HOME + "/client/bin/fedora-modify",
                getHost() + ":" + getPort(),
                getUsername(),
                getPassword(),
                batchDirectives.getAbsolutePath(),
                logFile.getAbsolutePath(),
                getProtocol(),
                "validate-only-option",
                getFedoraAppServerContext());
    }

    private void exportObj(String pid, File dir) {
        execute(FEDORA_HOME + "/client/bin/fedora-export",
                getHost() + ":" + getPort(),
                getUsername(),
                getPassword(),
                pid,
                FOXML1_1.uri,
                "public",
                dir.getAbsolutePath(),
                getProtocol(),
                getFedoraAppServerContext());
    }

    public static void execute(String... cmd) {
        String osName = System.getProperty("os.name");
        if (!osName.startsWith("Windows")) {
            // needed for the Fedora shell scripts
            cmd[0] = cmd[0] + ".sh";
        }
        if (sbOut != null && sbErr != null) {
            sbOut.reset();
            sbErr.reset();
            ExecUtility.execCommandLineUtility(cmd, sbOut, sbErr);
        } else {
            ExecUtility.execCommandLineUtility(cmd);
        }
    }

    @Before
    public void setUp() throws Exception {
        sbOut = new ByteArrayOutputStream();
        sbErr = new ByteArrayOutputStream();

        System.out.println("Creating temporary build and log directories");
        buildDir = new File(BUILDDIR);
        if(!buildDir.exists()) {
            buildDir.mkdir();
        }

        logDir = new File(LOGDIR);
        if(!logDir.exists()) {
            logDir.mkdir();
        }
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("Deleting temporary build and log directories");
        if (buildDir.exists() && buildDir.isDirectory()) {
            String[] list = buildDir.list();
            if (list != null) {
               for (int i = 0; i < list.length; i++) {
                   File entry = new File(buildDir, list[i]);
                   entry.delete();
                }
            }
            buildDir.delete();
        }

        if (logDir.exists() && logDir.isDirectory()) {
            String[] list = logDir.list();
            if (list != null) {
               for (int i = 0; i < list.length; i++) {
                   File entry = new File(logDir, list[i]);
                   entry.delete();
               }
            }
            logDir.delete();
        }
    }

    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TestCommandLineUtilities.class);
    }

    public static void main(String[] args) {
        JUnitCore.runClasses(TestCommandLineUtilities.class);
    }

}
