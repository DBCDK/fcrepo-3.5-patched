/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.xml.serialize.Method;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMConfiguration;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.SAXException;

/**
 * A utility class for XML serialization.
 *
 * @author Edwin Shin
 * @version $Id$
 */
@SuppressWarnings("deprecation")
public class XmlSerializer {

    private static final Logger logger =
            LoggerFactory.getLogger(XmlSerializer.class);

    public static void serialize(InputStream in, OutputStream out)
            throws Exception {
        serialize(in, out, false);
    }

    public static void serialize(Document document, OutputStream out)
            throws IOException {
        serialize(document, out, false);
    }

    public static void serialize(InputStream in,
                                 OutputStream out,
                                 boolean prettyPrint)
            throws Exception {
        Document document = getDocument(in);
        serialize(document, out, prettyPrint);
    }

    public static void serialize(Document document,
                                 OutputStream out,
                                 boolean prettyPrint) throws IOException {
        DOMImplementationLS domImpl = getDOMImplementationLS(document);
        LSSerializer lsSerializer = domImpl.createLSSerializer();
        if (prettyPrint) {
            prettyPrintWithXMLSerializer(document, out);
            return;
            /*
             * DOMConfiguration domConfiguration = lsSerializer.getDomConfig();
             * if (domConfiguration.canSetParameter("format-pretty-print",
             * Boolean.TRUE)) {
             * lsSerializer.getDomConfig().setParameter("format-pretty-print",
             * Boolean.TRUE); } else {
             * logger.warn("DOMConfiguration 'format-pretty-print' not supported. "
             * + "Falling back to deprecated Xerces XMLSerializer.");
             * prettyPrintWithXMLSerializer(document, out); return; }
             */
        }

        LSOutput lsOutput = domImpl.createLSOutput();
        lsOutput.setEncoding("UTF-8");
        lsOutput.setByteStream(out);
        lsSerializer.write(document, lsOutput);
    }

    private static DOMImplementationLS getDOMImplementationLS(Document document) {
        DOMImplementation domImplementation = document.getImplementation();
        if (domImplementation.hasFeature("LS", "3.0")
                && domImplementation.hasFeature("Core", "2.0")) {
            DOMImplementationLS domImplementationLS;
            try {
                domImplementationLS =
                    (DOMImplementationLS) domImplementation.getFeature("LS",
                                                                       "3.0");
            } catch(NoSuchMethodError nse) {
                logger.warn("Caught NoSuchMethodError for " +
                         domImplementation.getClass().getName() + "#getFeature. " +
                 		 "Trying fallback for DOMImplementationLS.");
                try {
                    domImplementationLS = getDOMImplementationLS();
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }

            }
            return domImplementationLS;
        } else {
            throw new RuntimeException("DOM 3.0 LS and/or DOM 2.0 Core not supported.");
        }
    }

    static DOMImplementationLS getDOMImplementationLS()
            throws ClassCastException, ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        DOMImplementationRegistry registry;
        registry = DOMImplementationRegistry.newInstance();
        DOMImplementationLS impl =
                (DOMImplementationLS) registry.getDOMImplementation("LS");
        return impl;
    }

    static void prettyPrintWithDOM3LS(Document document, OutputStream out) {
        DOMImplementationLS domImpl = getDOMImplementationLS(document);
        LSSerializer lsSerializer = domImpl.createLSSerializer();
        DOMConfiguration domConfiguration = lsSerializer.getDomConfig();
        if (domConfiguration.canSetParameter("format-pretty-print",
                                             Boolean.TRUE)) {
            lsSerializer.getDomConfig().setParameter("format-pretty-print",
                                                     Boolean.TRUE);
            LSOutput lsOutput = domImpl.createLSOutput();
            lsOutput.setEncoding("UTF-8");
            lsOutput.setByteStream(out);
            lsSerializer.write(document, lsOutput);
        } else {
            throw new RuntimeException("DOMConfiguration 'format-pretty-print' parameter isn't settable.");
        }
    }

    public static void prettyPrintWithTransformer(Document doc, OutputStream out) {
        try {
            final Transformer serializer = XmlTransformUtility.getTransformer();
            //Setup indenting to "pretty print"
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
            serializer
                    .setOutputProperty("{http://xml.apache.org/xslt}indent-amount",
                                       "2");

            DOMSource xmlSource = new DOMSource(doc);
            StreamResult outputTarget = new StreamResult(out);
            serializer.transform(xmlSource, outputTarget);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    public static void prettyPrintWithXMLSerializer(InputStream in,
                                                    OutputStream out)
            throws Exception {

        prettyPrintWithXMLSerializer(in, out, false);
    }

    @Deprecated
    public static void prettyPrintWithXMLSerializer(InputStream in,
                                                    OutputStream out,
                                                    boolean embeddable)
            throws Exception {
        Document document = getDocument(in);
        prettyPrintWithXMLSerializer(document, out, embeddable);
    }

    @Deprecated
    public static void prettyPrintWithXMLSerializer(Document document,
                                                    OutputStream out)
            throws IOException {
        prettyPrintWithXMLSerializer(document, out, false);
    }

    @Deprecated
    public static void prettyPrintWithXMLSerializer(Document document,
                                                    OutputStream out,
                                                    boolean embeddable)
            throws IOException {
        if (embeddable) {
            prettyPrintWithXMLSerializer(document, out, true, true);
        } else {
            prettyPrintWithXMLSerializer(document, out, false, false);
        }
    }

    @Deprecated
    public static void prettyPrintWithXMLSerializer(InputStream in,
                                                    OutputStream out,
                                                    boolean omitXMLDeclaration,
                                                    boolean omitDocumentType)
            throws Exception {
        Document document = getDocument(in);
        prettyPrintWithXMLSerializer(document,
                                     out,
                                     omitXMLDeclaration,
                                     omitDocumentType);
    }

    @Deprecated
    public static void prettyPrintWithXMLSerializer(Document document,
                                                    OutputStream out,
                                                    boolean omitXMLDeclaration,
                                                    boolean omitDocumentType)
            throws IOException {
        OutputFormat format = getOutputFormat(omitXMLDeclaration, omitDocumentType);
        XMLSerializer serializer = new XMLSerializer(out, format);
        serializer.serialize(document);
    }

    @Deprecated
    public static void prettyPrintWithXMLSerializer(Document document,
                                                    Writer writer,
                                                    boolean omitXMLDeclaration,
                                                    boolean omitDocumentType)
            throws IOException {
        OutputFormat format = getOutputFormat(omitXMLDeclaration, omitDocumentType);
        XMLSerializer serializer = new XMLSerializer(writer, format);
        serializer.serialize(document);
    }

    @Deprecated
    private static OutputFormat getOutputFormat(boolean omitXMLDeclaration,
                                                boolean omitDocumentType) {
        OutputFormat format = new OutputFormat(Method.XML, "UTF-8", true);
        format.setIndent(2);
        format.setLineWidth(80);
        if (omitXMLDeclaration) {
            format.setOmitXMLDeclaration(true);
        }
        if (omitDocumentType) {
            format.setOmitDocumentType(true);
        }
        return format;
    }

    /**
     * Get a new DOM Document object from parsing the specified InputStream.
     *
     * @param in
     *        the InputStream to parse.
     * @return a new DOM Document object.
     * @throws ParserConfigurationException
     *         if a DocumentBuilder cannot be created.
     * @throws SAXException
     *         If any parse errors occur.
     * @throws IOException
     *         If any IO errors occur.
     */
    protected static Document getDocument(InputStream in)
            throws Exception {
        DocumentBuilder builder = XmlTransformUtility.borrowDocumentBuilder();
        Document doc = null;
        try {
            doc = builder.parse(in);
        } finally {
            XmlTransformUtility.returnDocumentBuilder(builder);
        }
        return doc;
    }
}
