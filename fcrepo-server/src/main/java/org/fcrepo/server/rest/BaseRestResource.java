/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.rest;

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.net.URI;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.map.ObjectMapper;
import org.fcrepo.common.Constants;
import org.fcrepo.server.Context;
import org.fcrepo.server.ReadOnlyContext;
import org.fcrepo.server.Server;
import org.fcrepo.server.access.Access;
import org.fcrepo.server.errors.StorageMaintenanceException;
import org.fcrepo.server.errors.ObjectValidityException;
import org.fcrepo.server.errors.RangeNotSatisfiableException;
import org.fcrepo.server.errors.ResourceLockedError;
import org.fcrepo.server.errors.ResourceNotFoundError;
import org.fcrepo.server.errors.authorization.AuthzException;
import org.fcrepo.server.management.Management;
import org.fcrepo.server.storage.DOManager;
import org.fcrepo.server.storage.types.MIMETypedStream;
import org.fcrepo.server.storage.types.Property;
import org.fcrepo.utilities.XmlTransformUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A barebone RESTFUL resource implementation.
 *
 * @author cuong.tran@yourmediashelf.com
 * @version $Id$
 */
public class BaseRestResource {
    public static final String VALID_PID_PART = "/{pid : ([A-Za-z0-9]|-|\\.)+(:|%3A|%3a)(([A-Za-z0-9])|-|\\.|~|_|(%[0-9A-F]{2}))+}";

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseRestResource.class);

    static final String[] EMPTY_STRING_ARRAY = new String[0];
    static final String DEFAULT_ENC = "UTF-8";

    public static final String FORM = "multipart/form-data";
    public static final String HTML = "text/html";
    public static final String XML = "text/xml";
    public static final String ZIP = "application/zip";

    public static final MediaType TEXT_HTML = new MediaType("text", "html");
    public static final MediaType TEXT_XML = new MediaType("text", "xml");

    public static final MediaType APP_ZIP = new MediaType("application", "zip");

    protected Server m_server;
    protected Management m_management;
    protected Access m_access;
    protected String m_hostname;
    protected ObjectMapper m_mapper;
    
    protected DatastreamFilenameHelper m_datastreamFilenameHelper;

    @javax.ws.rs.core.Context
    protected HttpServletRequest m_servletRequest;

    @javax.ws.rs.core.Context
    protected UriInfo m_uriInfo;

    @javax.ws.rs.core.Context
    protected javax.ws.rs.core.HttpHeaders m_headers;

    public BaseRestResource(Server server) {
        try {
            this.m_server = server;
            this.m_management = (Management) m_server.getModule("org.fcrepo.server.management.Management");
            this.m_access = (Access) m_server.getModule("org.fcrepo.server.access.Access");
            this.m_hostname = m_server.getParameter("fedoraServerHost");
            m_datastreamFilenameHelper = new DatastreamFilenameHelper(m_server,
                    (DOManager) m_server.getModule("org.fcrepo.server.storage.DOManager"));
            m_mapper = new ObjectMapper();
        } catch (Exception ex) {
            throw new RestException("Unable to locate Fedora server instance", ex);
        }
    }

    protected Context getContext() {
        return ReadOnlyContext.getContext(Constants.HTTP_REQUEST.REST.uri,
                                          m_servletRequest);
    }

    protected DefaultSerializer getSerializer(Context context) {
        return new DefaultSerializer(m_hostname, context);
    }

    protected void transform(String xml, String xslt, Writer out)
    throws TransformerFactoryConfigurationError,
           TransformerConfigurationException,
           TransformerException {
        transform(new StringReader(xml), xslt, out);
    }
    
    protected void transform(Reader xml, String xslt, Writer out)
                throws TransformerFactoryConfigurationError,
                       TransformerConfigurationException,
                       TransformerException {
        File xslFile = new File(m_server.getHomeDir(), xslt);

        // XmlTransformUtility maintains a cache of Templates
        Templates template =
                XmlTransformUtility.getTemplates(xslFile);
        Transformer transformer = template.newTransformer();
        String appContext = getContext().getEnvironmentValue(Constants.FEDORA_APP_CONTEXT_NAME);
        transformer.setParameter("fedora", appContext);
        transformer.transform(new StreamSource(xml), new StreamResult(out));
    }

    protected Response buildResponse(MIMETypedStream result) throws Exception {
        ResponseBuilder builder = null;
        switch (result.getStatusCode()) {
            case HttpStatus.SC_MOVED_TEMPORARILY:
                URI location = URI.create(IOUtils.toString(result.getStream()));
                return Response.temporaryRedirect(location).build();
            case HttpStatus.SC_NOT_MODIFIED:
                builder = Response.notModified();
                break;
            default:
                builder = Response.status(result.getStatusCode());
                if (result.getSize() != -1L){
                    builder.header(HttpHeaders.CONTENT_LENGTH,result.getSize());
                }

                if (!result.getMIMEType().isEmpty()){
                    builder.type(result.getMIMEType());
                }
                InputStream content = result.getStream();
                if (content != null) {
                    builder.entity(result.getStream());
                }
        }

        if (result.header != null) {
            for (Property header : result.header) {
                if (header.name != null
                        && !(header.name.equalsIgnoreCase(HttpHeaders.TRANSFER_ENCODING))
                        && !(header.name.equalsIgnoreCase(HttpHeaders.CONTENT_LENGTH))
                        && !(header.name.equalsIgnoreCase(HttpHeaders.CONTENT_TYPE))) {
                    builder.header(header.name, header.value);
                }
            }
        }
        return builder.build();
    }

    private Response handleException(Exception ex) {
        if (ex instanceof ResourceNotFoundError) {
            LOGGER.warn("Resource not found: " + ex.getMessage() + "; unable to fulfill REST API request", ex);
            return Response.status(Status.NOT_FOUND).entity(ex.getMessage()).type("text/plain").build();
        } else if (ex instanceof AuthzException) {
            LOGGER.warn("Authorization failed; unable to fulfill REST API request", ex);
            return Response.status(Status.UNAUTHORIZED).entity(ex.getMessage()).type("text/plain").build();
        } else if (ex instanceof IllegalArgumentException) {
            LOGGER.warn("Bad request; unable to fulfill REST API request", ex);
            return Response.status(Status.BAD_REQUEST).entity(ex.getMessage()).type("text/plain").build();
        } else if (ex instanceof ResourceLockedError) {
            LOGGER.warn("Lock exception; unable to fulfill REST API request", ex);
            return Response.status(Status.CONFLICT).entity(ex.getMessage()).type(MediaType.TEXT_PLAIN).build();
        } else if(ex instanceof StorageMaintenanceException){ 
            LOGGER.warn("Storage maintenance exception exception; unable to fulfill REST API request", ex);
            return Response.status(Status.SERVICE_UNAVAILABLE).entity(ex.getMessage()).type(MediaType.TEXT_PLAIN).build();
        } else if (ex instanceof ObjectValidityException){
            LOGGER.warn("Validation exception; unable to fulfill REST API request", ex);
			if (((ObjectValidityException) ex).getValidation() != null) {
				String errors = DefaultSerializer.objectValidationToXml(((ObjectValidityException) ex).getValidation());
	            return Response.status(Status.BAD_REQUEST).entity(errors).type(MediaType.TEXT_XML).build();
			} else {
	            return Response.status(Status.BAD_REQUEST).entity(ex.getMessage()).type(MediaType.TEXT_PLAIN).build();
			}
        } else if (ex instanceof RangeNotSatisfiableException) {
            LOGGER.warn("Bad range request: " + ex.getMessage() + "; unable to fulfill REST API request", ex);
            return Response.status(RangeNotSatisfiableException.STATUS_CODE).entity(ex.getMessage()).type(MediaType.TEXT_PLAIN).build();
        } else {
            LOGGER.error("Unexpected error fulfilling REST API request", ex);
            throw new WebApplicationException(ex);
        }
    }

    protected Response handleException(Exception ex, boolean flash) {
        Response error = handleException(ex);
        if (flash) error = Response.ok(error.getEntity()).build();
        return error;
    }
    
}
