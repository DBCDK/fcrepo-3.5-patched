/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.test;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;

import java.util.Set;

import org.custommonkey.xmlunit.XMLUnit;

import org.w3c.dom.Document;

import org.xml.sax.InputSource;

import org.fcrepo.client.FedoraClient;
import org.fcrepo.client.search.SearchResultParser;
import org.fcrepo.client.utility.AutoPurger;
import org.fcrepo.client.utility.ingest.Ingest;
import org.fcrepo.client.utility.ingest.IngestCounter;

import org.fcrepo.common.Constants;
import org.fcrepo.common.http.HttpInputStream;

import org.fcrepo.server.access.FedoraAPIAMTOM;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.junit.runner.JUnitCore;


/**
 * Base class for JUnit tests that assume a running Fedora instance.
 *
 * @author Edwin Shin
 */
public abstract class FedoraServerTestCase
        extends FedoraTestCase
        implements Constants {

    public FedoraServerTestCase() {
        super();
    }

    public FedoraServerTestCase(String name) {
        super(name);
    }

    /**
     * Returns the requested HTTP resource as an XML Document
     *
     * @param location a URL relative to the Fedora base URL
     * @return Document
     * @throws Exception
     */
    @Deprecated
    public Document getXMLQueryResult(String location) throws Exception {
        return getXMLQueryResult(getFedoraClient(), location);
    }

    public Document getXMLQueryResult(FedoraClient client, String location)
            throws Exception {
        InputStream is = client.get(getBaseURL() + location, true, true);
        Document result = XMLUnit.buildControlDocument(new InputSource(is));
        is.close();
        return result;
    }

    public static boolean testingMETS() {
        String format = System.getProperty("demo.format");
        return format != null && format.equalsIgnoreCase("mets");
    }

    public static boolean testingAtom() {
        String format = System.getProperty("demo.format");
        return format != null && format.equalsIgnoreCase("atom");
    }

    public static boolean testingAtomZip() {
        String format = System.getProperty("demo.format");
        return format != null && format.equalsIgnoreCase("atom-zip");
    }

    @Deprecated
    public static void ingestDemoObjects() throws Exception {
        ingestDemoObjects("/");
    }
    
    public static void ingestDemoObjects(FedoraClient client) throws Exception {
        ingestDemoObjects(client, "/");
    }
    
    public static void ingestDemoObjects(FedoraAPIAMTOM apia, FedoraAPIMMTOM apim) throws Exception {
        ingestDemoObjects(apia, apim, "/");
    }

    /**
     * Ingest a specific directory of demo objects.
     * <p>
     * Given a path relative to the format-independent demo object hierarchy,
     * will ingest all files in the hierarchy denoted by the path.
     * </p>
     * <h2>example</h2>
     * <p>
     * <code>ingestDemoObjects(local-server-demos)</code> will ingest all files
     * underneath the <code>client/demo/[format]/local-server-demos/</code>
     * hierarchy
     * </p>
     *
     * @param path format-independent path to a directory within the demo object
     *             hierarchy.
     * @throws Exception
     */
    @Deprecated
    public static void ingestDemoObjects(String... paths) throws Exception {
        FedoraClient client = FedoraTestCase.getFedoraClient();
        ingestDemoObjects(client, paths);
        client.shutdown();
    }

    public static void ingestDemoObjects(FedoraClient client, String... paths) throws Exception {
        FedoraAPIAMTOM apia = client.getAPIAMTOM();
        FedoraAPIMMTOM apim = client.getAPIMMTOM();
        ingestDemoObjects(apia, apim, paths);
    }

    public static void ingestDemoObjects(FedoraAPIAMTOM apia, FedoraAPIMMTOM apim, String... paths) throws Exception {

        File dir = null;

        for (String path: paths) {
            String specificPath = File.separator + path;

            String ingestFormat;
            if (testingMETS()) {
                System.out.println("Ingesting demo objects in METS format from " + specificPath);
                dir = new File(FEDORA_HOME, "client/demo/mets" + specificPath);
                ingestFormat = METS_EXT1_1.uri;
            } else if (testingAtom()) {
                System.out.println("Ingesting demo objects in Atom format from " + specificPath);
                dir = new File(FEDORA_HOME, "client/demo/atom" + specificPath);
                ingestFormat = ATOM1_1.uri;
            } else if (testingAtomZip()) {
                System.out.println("Ingesting all demo objects in Atom Zip format from " + specificPath);
                dir = new File(FEDORA_HOME, "client/demo/atom-zip" + specificPath);
                ingestFormat = ATOM_ZIP1_1.uri;
            } else {
                System.out.println("Ingesting demo objects in FOXML format from " + specificPath);
                dir = new File(FEDORA_HOME, "client/demo/foxml" + specificPath);
                ingestFormat = FOXML1_1.uri;
            }

            Ingest.multiFromDirectory(dir,
                    ingestFormat,
                    apia,
                    apim,
                    null,
                    new PrintStream(File.createTempFile("demo",
                            null)),
                            new IngestCounter());
        }
    }
    
    public static void ingestDocumentTransformDemoObjects(FedoraClient client)
        throws Exception {
        ingestDemoObjects(client, "local-server-demos" + File.separator + "document-transform-demo");
    }

    public static void ingestFormattingObjectsDemoObjects(FedoraClient client)
            throws Exception {
            ingestDemoObjects(client, "local-server-demos" + File.separator + "formatting-objects-demo");
    }

    /**
     * Ingest the "Smiley" objects
     * @param client
     * @throws Exception
     */
    public static void ingestImageCollectionDemoObjects(FedoraClient client)
            throws Exception {
            ingestDemoObjects(client, "local-server-demos" + File.separator + "image-collection-demo");
    }

    public static void ingestImageManipulationDemoObjects(FedoraClient client)
            throws Exception {
            ingestDemoObjects(client, "local-server-demos" + File.separator + "image-manip-demo");
    }

    public static void ingestSimpleDocumentDemoObjects(FedoraClient client)
            throws Exception {
            ingestDemoObjects(client, "local-server-demos" + File.separator + "simple-document-demo");
    }

    public static void ingestSimpleImageDemoObjects(FedoraClient client)
            throws Exception {
            ingestDemoObjects(client, "local-server-demos" + File.separator + "simple-image-demo");
    }

    /**
     * Gets the PIDs of objects in the "demo" pid namespace that are in the
     * repository
     *
     * @return set of PIDs of the specified object type
     * @throws Exception
     */
    @Deprecated
    public static Set<String> getDemoObjects() throws Exception {

        FedoraClient client = getFedoraClient();
        Set<String> result = null;
        try {
            result = getDemoObjects(client);
        } finally {
            client.shutdown();
        }
        return result;
    }

    public static Set<String> getDemoObjects(FedoraClient client)
        throws Exception {
        HttpInputStream queryResult;
        queryResult =
                client.get(getBaseURL() + "/search?query=pid~demo:*"
                           + "&maxResults=1000&pid=true&xml=true", true, true);
        SearchResultParser parser = new SearchResultParser(queryResult);

        Set<String> result = parser.getPIDs();
        queryResult.close();
        return result;
    }
    
    @Deprecated
    public static void purgeDemoObjects() throws Exception {
        FedoraClient client = getFedoraClient();
        purgeDemoObjects(client);
        client.shutdown();
    }
    
    public static void purgeDemoObjects(FedoraClient client) throws Exception {
        FedoraAPIMMTOM apim = client.getAPIMMTOM();
        for (String pid : getDemoObjects(client)) {
            AutoPurger.purge(apim, pid, null);
        }
    }
    
    @Deprecated
    public static void purgeDemoObjects(FedoraAPIMMTOM apim) throws Exception {
        for (String pid : getDemoObjects()) {
            AutoPurger.purge(apim, pid, null);
        }
    }

    public static void main(String[] args) {
        JUnitCore.runClasses(FedoraServerTestCase.class);
    }
}
