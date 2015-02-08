/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.client.utility.export;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.rpc.ServiceException;

import org.fcrepo.common.Constants;
import org.fcrepo.server.access.FedoraAPIAMTOM;
import org.fcrepo.server.management.FedoraAPIMMTOM;
import org.fcrepo.server.types.gen.RepositoryInfo;
import org.fcrepo.server.utilities.TypeUtility;
import org.fcrepo.utilities.FileUtils;
import org.fcrepo.utilities.xml.XercesXmlSerializers;
import org.w3c.dom.Document;


/**
 * Utility class for exporting objects from a Fedora repository.
 *
 * @author Chris Wilper
 * @version $Id$
 */
public class AutoExporter
        implements Constants {

    private final FedoraAPIMMTOM m_apim;

    private final FedoraAPIAMTOM m_apia;

    private static HashMap<FedoraAPIAMTOM, RepositoryInfo> s_repoInfo =
            new HashMap<FedoraAPIAMTOM, RepositoryInfo>();

    public AutoExporter(FedoraAPIAMTOM apia, FedoraAPIMMTOM apim)
            throws MalformedURLException, ServiceException {
        m_apia = apia;
        m_apim = apim;
    }

    public void export(String pid,
                       String format,
                       String exportContext,
                       OutputStream outStream) throws RemoteException,
            IOException {
        export(m_apia, m_apim, pid, format, exportContext, outStream);
    }

    public static void export(FedoraAPIAMTOM apia,
                              FedoraAPIMMTOM apim,
                              String pid,
                              String format,
                              String exportContext,
                              OutputStream outStream) throws RemoteException,
            IOException {

        // Get the repository info from the hash for the repository that APIA is using,
        // unless it isn't in the hash yet... in which case, ask the server.
        RepositoryInfo repoinfo = s_repoInfo.get(apia);
        if (repoinfo == null) {
            repoinfo = apia.describeRepository();
            s_repoInfo.put(apia, repoinfo);
        }

        byte[] bytes;
        // For backward compatibility:
        // Pre-2.0 repositories will only export "metslikefedora1" format.
        // 2.x repositories will only export "metslikefedora1" and "foxml1.0" formats.
        // For 3.0+ repositories the format arg is sent to "export" method.
        StringTokenizer stoken =
                new StringTokenizer(repoinfo.getRepositoryVersion(), ".");
        int majorVersion = new Integer(stoken.nextToken()).intValue();
        if (majorVersion < 2) {
            if (format == null || format.equals(METS_EXT1_1.uri)
                    || format.equals(METS_EXT1_0.uri)
                    || format.equals("METS_EXT1_0_LEGACY")
                    || format.equals("default")) {
                if (format.equals(METS_EXT1_1.uri)) {
                    System.out.println("WARNING: Repository does not support METS Fedora " +
                                       "Extension 1.1; exporting older format (v1.0) instead");
                }
                bytes = TypeUtility.convertDataHandlerToBytes(apim.export(pid, METS_EXT1_0_LEGACY, exportContext));
            } else {
                throw new IOException("You are connected to a pre-2.0 Fedora repository "
                        + "which will only export the XML format \"metslikefedora1\".");
            }
        } else {
            if (majorVersion < 3) {
                if (format != null) {
                    if (format.equals(FOXML1_1.uri)) {
                        System.out.println("WARNING: Repository does not support FOXML 1.1; " +
                                           "exporting older format (v1.0) instead");
                        format = FOXML1_0_LEGACY;
                    } else if (format.equals(FOXML1_0.uri)) {
                        format = FOXML1_0_LEGACY;
                    } else if (format.equals(METS_EXT1_1.uri)) {
                        System.out.println("WARNING: Repository does not support METS Fedora " +
                                           "Extension 1.1; exporting older format (v1.0) instead");
                        format = METS_EXT1_0_LEGACY;
                    } else if (format.equals(METS_EXT1_0.uri)) {
                        format = METS_EXT1_0_LEGACY;
                    } else {
                        throw new IOException("You are connected to a 2.x Fedora repository " +
                                              "which will only export FOXML and METS XML formats.");
                    }
                }
            } else { // majorVersion >= 3
                if (format != null) {
                    if (format.equals(FOXML1_0_LEGACY)) {
                        format = FOXML1_0.uri;
                    } else if (format.equals(METS_EXT1_0_LEGACY)) {
                        format = METS_EXT1_0.uri;
                    }
                }
                validateFormat(format);
            }
            bytes = TypeUtility.convertDataHandlerToBytes(apim.export(pid, format, exportContext));
        }
        try {
            // TODO This export method has assumed the output is always XML,
            // but with ATOM_ZIP (and in the future, IMS CP/SCORM) this is no
            // longer the case. Perhaps we move pretty printing into the
            // serializers themselves.
            if (ATOM_ZIP1_1.uri.equals(format)) {
                FileUtils.copy(new ByteArrayInputStream(bytes), outStream);
            } else {
                DocumentBuilderFactory factory =
                        DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(true);
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(new ByteArrayInputStream(bytes));
                OutputStreamWriter writer =
                        new OutputStreamWriter(outStream, Charset.forName("UTF-8"));
                // use xerces to pretty print the xml, assuming it's well formed
                XercesXmlSerializers.writeMgmtNoDecl(doc, writer);
                writer.close();
            }
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getClass().getName() + " : "
                    + e.getMessage());
        } finally {
            outStream.close();
        }
    }

    public void getObjectXML(String pid, OutputStream outStream)
            throws RemoteException, IOException {
        getObjectXML(m_apia, m_apim, pid, outStream);
    }

    public static void getObjectXML(FedoraAPIAMTOM apia,
                                    FedoraAPIMMTOM apim,
                                    String pid,
                                    OutputStream outStream)
            throws RemoteException, IOException {

        // Get the repository info from the hash for the repository that APIA is using,
        // unless it isn't in the hash yet... in which case, ask the server.
        RepositoryInfo repoinfo = s_repoInfo.get(apia);
        if (repoinfo == null) {
            repoinfo = apia.describeRepository();
            s_repoInfo.put(apia, repoinfo);
        }
        // get the object XML as it exists in the repository's
        // persitent storage area.
        byte[] bytes = TypeUtility.convertDataHandlerToBytes(apim.getObjectXML(pid));
        try {
            // use xerces to pretty print the xml, assuming it's well formed
            DocumentBuilderFactory factory =
                    DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(bytes));
            OutputStreamWriter writer =
                    new OutputStreamWriter(outStream, Charset.forName("UTF-8"));
            XercesXmlSerializers.writeMgmtNoDecl(doc, writer);
            writer.close();
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getClass().getName() + " : "
                    + e.getMessage());
        } finally {
            outStream.close();
        }
    }

    public static void validateFormat(String format) throws IOException {
        if (format == null) {
            return;
        }
        if (!format.equals(FOXML1_1.uri) &&
            !format.equals(FOXML1_0.uri) &&
            !format.equals(METS_EXT1_1.uri) &&
            !format.equals(METS_EXT1_0.uri) &&
            !format.equals(ATOM1_1.uri) &&
            !format.equals(ATOM_ZIP1_1.uri) &&
            !format.equals("default")) {
            throw new IOException("Invalid export format. Valid FORMAT values are: '"
                    + FOXML1_1.uri
                    + "' '"
                    + FOXML1_0.uri
                    + "' '"
                    + METS_EXT1_1.uri
                    + "' '"
                    + METS_EXT1_0.uri
                    + "' '"
                    + ATOM1_1.uri
                    + "' '"
                    + ATOM_ZIP1_1.uri
                    + "' and 'default'");
        }
    }
}
