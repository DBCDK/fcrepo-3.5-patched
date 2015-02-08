/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.storage;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

import javax.activation.MimetypesFileTypeMap;

import org.apache.commons.httpclient.util.DateUtil;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.fcrepo.common.Constants;
import org.fcrepo.common.http.HttpInputStream;
import org.fcrepo.common.http.WebClient;
import org.fcrepo.common.http.WebClientConfiguration;
import org.fcrepo.server.Context;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.utilities.MD5Utility;
import org.fcrepo.server.errors.GeneralException;
import org.fcrepo.server.errors.HttpServiceNotFoundException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.RangeNotSatisfiableException;
import org.fcrepo.server.errors.authorization.AuthzException;
import org.fcrepo.server.security.Authorization;
import org.fcrepo.server.security.BackendPolicies;
import org.fcrepo.server.security.BackendSecurity;
import org.fcrepo.server.security.BackendSecuritySpec;
import org.fcrepo.server.storage.translation.DOTranslationUtility;
import org.fcrepo.server.storage.types.MIMETypedStream;
import org.fcrepo.server.storage.types.Property;
import org.fcrepo.server.utilities.ServerUtility;
import org.fcrepo.utilities.io.NullInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides a mechanism to obtain external HTTP-accessible content.
 *
 * @author Ross Wayland
 * @version $Id$
 *
 */
public class DefaultExternalContentManager
        extends Module
        implements ExternalContentManager {

    private static final Logger logger =
            LoggerFactory.getLogger(DefaultExternalContentManager.class);

    private static final String DEFAULT_MIMETYPE="text/plain";
    
    private static final MimetypesFileTypeMap MIME_MAP =
            new MimetypesFileTypeMap();

    private String m_userAgent;

    @SuppressWarnings("unused")
    private String fedoraServerHost;

    private String fedoraServerPort;

    private String fedoraServerRedirectPort;

    private WebClientConfiguration m_httpconfig;

    private WebClient m_http;

    /**
     * Creates a new DefaultExternalContentManager.
     *
     * @param moduleParameters
     *        The name/value pair map of module parameters.
     * @param server
     *        The server instance.
     * @param role
     *        The module role name.
     * @throws ModuleInitializationException
     *         If initialization values are invalid or initialization fails for
     *         some other reason.
     */
    public DefaultExternalContentManager(Map<String, String> moduleParameters,
                                         Server server,
                                         String role)
            throws ModuleInitializationException {
        super(moduleParameters, server, role);
    }

    /**
     * Initializes the Module based on configuration parameters. The
     * implementation of this method is dependent on the schema used to define
     * the parameter names for the role of
     * <code>org.fcrepo.server.storage.DefaultExternalContentManager</code>.
     *
     * @throws ModuleInitializationException
     *         If initialization values are invalid or initialization fails for
     *         some other reason.
     */
    @Override
    public void initModule() throws ModuleInitializationException {
        try {
            Server s_server = getServer();
            m_userAgent = getParameter("userAgent");
            if (m_userAgent == null) {
                m_userAgent = "Fedora";
            }

            fedoraServerPort = s_server.getParameter("fedoraServerPort");
            fedoraServerHost = s_server.getParameter("fedoraServerHost");
            fedoraServerRedirectPort =
                    s_server.getParameter("fedoraRedirectPort");

            m_httpconfig = s_server.getWebClientConfig();
            if (m_httpconfig.getUserAgent() == null ) {
                m_httpconfig.setUserAgent(m_userAgent);
            }

            m_http = new WebClient(m_httpconfig);

        } catch (Throwable th) {
            throw new ModuleInitializationException("[DefaultExternalContentManager] "
                                                            + "An external content manager "
                                                            + "could not be instantiated. The underlying error was a "
                                                            + th.getClass()
                                                                    .getName()
                                                            + "The message was \""
                                                            + th.getMessage()
                                                            + "\".",
                                                    getRole(), th);
        }
    }

    /*
     * Retrieves the external content.
     * Currently the protocols <code>file</code> and
     * <code>http[s]</code> are supported.
     *
     * @see
     * org.fcrepo.server.storage.ExternalContentManager#getExternalContent(fedora
     * .server.storage.ContentManagerParams)
     */
    @Override
    public MIMETypedStream getExternalContent(ContentManagerParams params)
            throws GeneralException, HttpServiceNotFoundException, RangeNotSatisfiableException {
        logger.debug("in getExternalContent(), url={}", params.getUrl());
        try {
            if(params.getProtocol().equals("file")){
                return getFromFilesystem(params);
            }
            if (params.getProtocol().equals("http") || params.getProtocol().equals("https")){
                return getFromWeb(params);
            }
            throw new GeneralException("protocol for retrieval of external content not supported. URL: " + params.getUrl());
        }
        catch (RangeNotSatisfiableException re) {
        	throw re;
        } catch (Exception ex) {
            // catch anything but generalexception
            ex.printStackTrace();
            throw new HttpServiceNotFoundException("[" + this.getClass().getSimpleName() + "] "
                    + "returned an error.  The underlying error was a "
                    + ex.getClass().getName()
                    + "  The message "
                    + "was  \""
                    + ex.getMessage() + "\"  .  ",ex);
        }
    }

    /**
     * Get a MIMETypedStream for the given URL. If user or password are
     * <code>null</code>, basic authentication will not be attempted.
     */
    private MIMETypedStream getFromWeb(String url, String user, String pass,
            String knownMimeType, boolean headOnly, Context context)
            throws GeneralException, RangeNotSatisfiableException {
        logger.debug("DefaultExternalContentManager.getFromWeb({})", url);
        if (url == null) throw new GeneralException("null url");
        HttpInputStream response = null;
        try {
            if (headOnly) {
                response = m_http.head(url, true, user, pass,
                        context.getHeaderValue(HttpHeaders.IF_NONE_MATCH),
                        context.getHeaderValue(HttpHeaders.IF_MODIFIED_SINCE),
                        context.getHeaderValue("Range"));
            } else {
                response = m_http.get(
                        url, true, user, pass,
                        context.getHeaderValue(HttpHeaders.IF_NONE_MATCH),
                        context.getHeaderValue(HttpHeaders.IF_MODIFIED_SINCE),
                        context.getHeaderValue("Range"));
            }
            String mimeType =
                    response.getResponseHeaderValue(HttpHeaders.CONTENT_TYPE,
                                                    knownMimeType);
            long length = Long.parseLong(response.getResponseHeaderValue(HttpHeaders.CONTENT_LENGTH,"-1"));
            Property[] headerArray =
                    toPropertyArray(response.getResponseHeaders());
            if (mimeType == null || mimeType.isEmpty()) {
                mimeType = DEFAULT_MIMETYPE;
            }
            if (response.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
                response.close();
                Header[] respHeaders = response.getResponseHeaders();
                Property[] properties = new Property[respHeaders.length];
                for (int i = 0; i < respHeaders.length; i++){
                    properties[i] =
                        new Property(respHeaders[i].getName(), respHeaders[i].getValue());
                }
                return MIMETypedStream.getNotModified(properties);
            } else if (response.getStatusCode() == HttpStatus.SC_PARTIAL_CONTENT) {
            	MIMETypedStream pc_response = new MIMETypedStream(mimeType, response,
                        headerArray, length);
            	pc_response.setStatusCode(HttpStatus.SC_PARTIAL_CONTENT);
            	return pc_response;
            } else if (response.getStatusCode() == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
            	response.close();
            	throw new RangeNotSatisfiableException("External URL datastream request returned 416 Range Not Satisfiable");
            } else {
                if (headOnly) {
                    try {
                        response.close();
                    } catch (IOException ioe) {
                        logger.warn("problem closing HEAD response: {}", ioe.getMessage());
                    }
                    
                    return new MIMETypedStream(mimeType, NullInputStream.NULL_STREAM,
                            headerArray, length);
                } else {
                    return new MIMETypedStream(mimeType, response, headerArray, length);
                }
            }
        } catch (RangeNotSatisfiableException re){
        	logger.error(re.getMessage(), re);
        	throw re;                    
        } catch (Exception e) {
            throw new GeneralException("Error getting " + url, e);
        }
    }

    /**
     * Convert the given HTTP <code>Headers</code> to an array of
     * <code>Property</code> objects.
     */
    private static Property[] toPropertyArray(Header[] headers) {

        Property[] props = new Property[headers.length];
        for (int i = 0; i < headers.length; i++) {
            props[i] = new Property();
            props[i].name = headers[i].getName();
            props[i].value = headers[i].getValue();
        }
        return props;
    }

    /**
     * Get a MIMETypedStream for the given URL. If user or password are
     * <code>null</code>, basic authentication will not be attempted.
     *
     * @param params
     * @return
     * @throws HttpServiceNotFoundException
     * @throws GeneralException
     * @throws RangeNotSatisfiableException
     */
    private MIMETypedStream getFromFilesystem(ContentManagerParams params)
            throws HttpServiceNotFoundException,RangeNotSatisfiableException,GeneralException {
        logger.debug("in getFromFilesystem(), url={}", params.getUrl());

        try {
            URL fileUrl = new URL(params.getUrl());
            File cFile = new File(fileUrl.toURI()).getCanonicalFile();
            // security check
            URI cURI = cFile.toURI();
            logger.info("Checking resolution security on {}", cURI);
            Authorization authModule = getServer()
                    .getBean("org.fcrepo.server.security.Authorization", Authorization.class);
            if (authModule == null) {
                throw new GeneralException(
                "Missing required Authorization module");
            }
            String cUriString = cURI.toString();
            authModule.enforceRetrieveFile(params.getContext(), cUriString);
            // end security check
            String mimeType = params.getMimeType();

            // if mimeType was not given, try to determine it automatically
            if (mimeType == null || mimeType.equalsIgnoreCase("")){
                mimeType = determineMimeType(cFile);
            }
            Property [] headers = getFileDatastreamHeaders(cUriString, cFile.lastModified());
            if (ServerUtility.isStaleCache(params.getContext(), headers)) {
                MIMETypedStream result;
                if (isHEADRequest(params)) {
                    result = new MIMETypedStream(mimeType, NullInputStream.NULL_STREAM,
                            headers, cFile.length());
                } else {
                    result = new MIMETypedStream(mimeType, fileUrl.openStream(),
                            headers, cFile.length());
                }
                String rangeHdr =
                        params.getContext().getHeaderValue(HttpHeaders.RANGE);
                if (rangeHdr != null) {
                    result.setRange(rangeHdr);;
                }
                return result;
                
            } else {
                return MIMETypedStream.getNotModified(headers);
            }
        }
        catch(AuthzException ae){
            logger.error(ae.getMessage(),ae);
            throw new HttpServiceNotFoundException("Policy blocked datastream resolution",ae);
        }
        catch (RangeNotSatisfiableException re){
        	logger.error(re.getMessage(), re);
        	throw re;
        }
        catch (GeneralException me) {
            logger.error(me.getMessage(),me);
            throw me;
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            // catch anything but generalexception
            logger.error(th.getMessage(),th);
             throw new HttpServiceNotFoundException("[FileExternalContentManager] "
                    + "returned an error.  The underlying error was a "
                    + th.getClass().getName()
                    + "  The message "
                    + "was  \""
                    + th.getMessage() + "\"  .  ",th);
        }
    }

    /**
     * Retrieves external content via http or https.
     *
     * @return A MIMETypedStream
     * @throws ModuleInitializationException
     * @throws GeneralException
     * @throws RangeNotSatisfiableException
     */
    private MIMETypedStream getFromWeb(ContentManagerParams params)
            throws ModuleInitializationException, GeneralException, RangeNotSatisfiableException {
           String username = params.getUsername();
        String password = params.getPassword();
        boolean backendSSL = false;
        String url = params.getUrl();
        // in case host is 'local.fedora.server', and has not been normalized (e.g. on validating datastream add)
        url = DOTranslationUtility.makeAbsoluteURLs(url);
        if (ServerUtility.isURLFedoraServer(url) && !params.isBypassBackend()) {
            BackendSecuritySpec m_beSS;
            BackendSecurity m_beSecurity =
                    (BackendSecurity) getServer()
                            .getModule("org.fcrepo.server.security.BackendSecurity");
            try {
                m_beSS = m_beSecurity.getBackendSecuritySpec();
            } catch (Exception e) {
                throw new ModuleInitializationException(
                        "Can't intitialize BackendSecurity module (in default access) from Server.getModule",
                        getRole());
            }
            Hashtable<String, String> beHash =
                    m_beSS.getSecuritySpec(BackendPolicies.FEDORA_INTERNAL_CALL);
            username = beHash.get("callUsername");
            password = beHash.get("callPassword");
            backendSSL =
                    Boolean.parseBoolean(beHash.get("callSSL"));
            if (backendSSL) {
                if (params.getProtocol().equals("http")) {
                    url = url.replaceFirst("http:", "https:");
                }
                url =
                        url.replaceFirst(":" + fedoraServerPort + "/",
                                            ":" + fedoraServerRedirectPort
                                                    + "/");
            }
            if (logger.isDebugEnabled()) {
                logger.debug("************************* backendUsername: "
                        + username + "     backendPassword: "
                        + password + "     backendSSL: " + backendSSL);
                logger.debug("************************* doAuthnGetURL: " + url);
            }

        }
        return getFromWeb(url, username, password, params.getMimeType(),
                isHEADRequest(params), params.getContext());
    }

/**
     * Determines the mime type of a given file
     *
     * @param file for which the mime type needs to be detected
     * @return the detected mime type
     */
    private String determineMimeType(File file){
        String mimeType = MIME_MAP.getContentType(file);
        // if mimeType detection failed, fall back to the default
        if (mimeType == null || mimeType.equalsIgnoreCase("")){
            mimeType = DEFAULT_MIMETYPE;
        }
        return mimeType;
    }
    
    /**
     * determine whether the context is a HEAD http request
     */
    private static boolean isHEADRequest(ContentManagerParams params) {
        Context context = params.getContext();
        if (context != null) {
            String method =
                    context.getEnvironmentValue(
                            Constants.HTTP_REQUEST.METHOD.attributeId);
            return "HEAD".equalsIgnoreCase(method);
        }
        return false;
    }
    
    /**
     * Content-Length is determined elsewhere
     * Content-Type is determined elsewhere
     * Last-Modified
     * ETag
     * @param String canonicalPath: the canonical path to a file system resource
     * @param long lastModified: the date of last modification
     * @return
     */
    private static Property[] getFileDatastreamHeaders(String canonicalPath, long lastModified) {
        Property[] result = new Property[3];
        String eTag =
            MD5Utility.getBase16Hash(canonicalPath.concat(Long.toString(lastModified)));
        result[0] = new Property(HttpHeaders.ACCEPT_RANGES,"bytes");
        result[1] = new Property(HttpHeaders.ETAG, eTag);
        result[2] = new Property(HttpHeaders.LAST_MODIFIED,
                DateUtil.formatDate(new Date(lastModified)));
        return result;
    }

}

