/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.test.integration;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.custommonkey.xmlunit.NamespaceContext;
import org.custommonkey.xmlunit.SimpleNamespaceContext;
import org.custommonkey.xmlunit.XMLUnit;

import org.w3c.dom.Document;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.custommonkey.xmlunit.XpathEngine;

import org.fcrepo.client.FedoraClient;

import org.fcrepo.server.management.FedoraAPIM;
import org.fcrepo.server.utilities.StreamUtility;

import org.fcrepo.test.FedoraServerTestCase;


/**
 * @author Edwin Shin
 */
public class TestOAIService
        extends FedoraServerTestCase {

    private DocumentBuilderFactory factory;

    private DocumentBuilder builder;

    private FedoraClient client;

    public static Test suite() {
        TestSuite suite = new TestSuite("Test OAI Service");
        suite.addTestSuite(TestOAIService.class);
        return suite;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        client = new FedoraClient(getBaseURL(), getUsername(), getPassword());

        factory = DocumentBuilderFactory.newInstance();
        builder = factory.newDocumentBuilder();

        Map<String, String> nsMap = new HashMap<String, String>();
        nsMap.put(NS_FEDORA_TYPES_PREFIX, NS_FEDORA_TYPES);
        nsMap.put("oai", "http://www.openarchives.org/OAI/2.0/");
        NamespaceContext ctx = new SimpleNamespaceContext(nsMap);
        XMLUnit.setXpathNamespaceContext(ctx);
    }

    @Override
    public void tearDown() throws Exception {
        XMLUnit.setXpathNamespaceContext(SimpleNamespaceContext.EMPTY_CONTEXT);
        super.tearDown();
    }

    public void testListMetadataFormats() throws Exception {
        String request = "/oai?verb=ListMetadataFormats";
        Document result = getXMLQueryResult(request);
        assertXpathEvaluatesTo("oai_dc",
                               "/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat/oai:metadataPrefix",
                               result);
    }

    public void testIdentify() throws Exception {

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));

        // Timestamp before identify.
        Date before = new Date( System.currentTimeMillis() - 10000 );
        String beforeStr = format.format( before );

        String request = "/oai?verb=Identify";
        Document result = getXMLQueryResult(request);

        // Timestamp after identify.
        Date after = new Date( System.currentTimeMillis() + 10000 );
        String afterStr = format.format( after );


        Source source = new DOMSource(result);
        StringWriter stringWriter = new StringWriter();
        Result res = new StreamResult(stringWriter);
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(source, res);

        assertXpathEvaluatesTo( "1970-01-01T00:00:00Z", "/oai:OAI-PMH/oai:Identify/oai:earliestDatestamp", result );

        // responseDate must be inside the before-after interval
        XpathEngine xpathEngine = XMLUnit.newXpathEngine();
        String responseDateStr = xpathEngine.evaluate( "/oai:OAI-PMH/oai:responseDate", result );
        Date responseDate = format.parse( responseDateStr );
        assertTrue( responseDateStr + ">" + beforeStr, responseDate.after( before ));
        assertTrue( responseDateStr + "<" + afterStr, responseDate.before( after ));

    }

    public void testListRecords() throws Exception {
        FedoraAPIM apim = client.getAPIM();
        FileInputStream in =
                new FileInputStream(FEDORA_HOME
                                    + "/client/demo/foxml/local-server-demos/simple-document-demo/obj_demo_31.xml");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamUtility.pipeStream(in, out, 4096);

        apim.ingest(out.toByteArray(), FOXML1_1.uri, "for testing");

        String request = "/oai?verb=ListRecords&metadataPrefix=oai_dc";
        Document result = getXMLQueryResult(request);
        assertXpathExists("/oai:OAI-PMH/oai:ListRecords/oai:record", result);

        request = "/oai?verb=ListRecords&metadataPrefix=oai_dc&from=2000-01-01";
        result = getXMLQueryResult(request);
        assertXpathExists("/oai:OAI-PMH/oai:ListRecords/oai:record", result);

        apim.purgeObject("demo:31", "for testing", false);
    }

    public void testListIdentifiersInterval() throws Exception {

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));

        // Timestamp before ingest. Used for OAI request
        Date from = new Date( System.currentTimeMillis() - 1000 );
        String fromStr = format.format( from );

        FedoraAPIM apim = client.getAPIM();
        FileInputStream in =
                new FileInputStream(FEDORA_HOME
                                    + "/client/demo/foxml/local-server-demos/simple-document-demo/obj_demo_31.xml");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamUtility.pipeStream(in, out, 4096);

        apim.ingest(out.toByteArray(), FOXML1_1.uri, "for testing");

        // Timestamp after ingest. Used for OAI request
        Date until = new Date( System.currentTimeMillis() + 1000 );
        String untilStr = format.format( until );

        String request = "/oai?verb=ListIdentifiers&metadataPrefix=oai_dc&from=" + fromStr + "&until=" + untilStr;
        Document result = getXMLQueryResult(request);

        Source source = new DOMSource(result);
        StringWriter stringWriter = new StringWriter();
        Result res = new StreamResult(stringWriter);
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(source, res);
        assertXpathExists("/oai:OAI-PMH/oai:ListIdentifiers/oai:header/oai:datestamp", result);

        // identifier datestamp must be inside the before-after interval
        assertXpathEvaluatesTo( "oai:dbc.dk:demo:31", "/oai:OAI-PMH/oai:ListIdentifiers/oai:header/oai:identifier", result );
        XpathEngine xpathEngine = XMLUnit.newXpathEngine();
        String datestampStr = xpathEngine.evaluate( "/oai:OAI-PMH/oai:ListIdentifiers/oai:header/oai:datestamp", result );
        Date datestamp = format.parse( datestampStr );

        assertTrue( datestampStr + ">" + fromStr, datestamp.after( from ));
        assertTrue( datestampStr + "<" + untilStr, datestamp.before( until ));

        apim.purgeObject("demo:31", "for testing", false);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TestOAIService.class);
    }

}