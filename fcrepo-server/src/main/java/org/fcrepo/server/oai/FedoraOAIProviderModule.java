/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.oai;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.fcrepo.oai.BadResumptionTokenException;
import org.fcrepo.oai.CannotDisseminateFormatException;
import org.fcrepo.oai.DateGranularitySupport;
import org.fcrepo.oai.DeletedRecordSupport;
import org.fcrepo.oai.IDDoesNotExistException;
import org.fcrepo.oai.MetadataFormat;
import org.fcrepo.oai.NoMetadataFormatsException;
import org.fcrepo.oai.NoRecordsMatchException;
import org.fcrepo.oai.NoSetHierarchyException;
import org.fcrepo.oai.OAIProvider;
import org.fcrepo.oai.Record;
import org.fcrepo.oai.RepositoryException;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.search.FieldSearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OAIProvider that acts as a server module and wraps FedoraOAIProvider.
 * </p>
 *
 * @author Chris Wilper
 */
public class FedoraOAIProviderModule
        extends Module
        implements OAIProvider {

    private static final Logger logger =
            LoggerFactory.getLogger(FedoraOAIProviderModule.class);

    private FedoraOAIProvider m_wrappedOAIProvider;

    public FedoraOAIProviderModule(Map<String, String>  params, Server server, String role)
            throws ModuleInitializationException {
        super(params, server, role);
    }

    @Override
    public void postInitModule() throws ModuleInitializationException {
        String repositoryName = getParameter("repositoryName");
        if (repositoryName == null) {
            throw new ModuleInitializationException("repositoryName must be specified.",
                                                    getRole());
        }
        String repositoryDomainName = getParameter("repositoryDomainName");
        if (repositoryDomainName == null) {
            throw new ModuleInitializationException("repositoryDomainName must be specified.",
                                                    getRole());
        }
        String host = getServer().getParameter("fedoraServerHost");
        if (host == null) {
            throw new ModuleInitializationException("fedoraServerHost must be specified as primary server config element.",
                                                    getRole());
        }
        String context = getServer().getParameter("fedoraAppServerContext");
        if (context == null) {
            throw new ModuleInitializationException("fedoraAppServerContext must be specified as primary server config element.",
                                                    getRole());
        }

        String port = getServer().getParameter("fedoraServerPort");
        if (port == null) {
            throw new ModuleInitializationException("fedoraServerPort must be specified as primary server config element.",
                                                    getRole());
        }
        Module mgr = getServer().getModule("org.fcrepo.server.storage.DOManager");
        if (mgr == null) {
            throw new ModuleInitializationException("DOManager is required (for pidNamespace param), but isn't loaded.",
                                                    getRole());
        }
        String pidNamespace = mgr.getParameter("pidNamespace");
        if (pidNamespace == null) {
            throw new ModuleInitializationException("DOManager did not specify a pidNamespace, but this module requires that it does.",
                                                    getRole());
        }
        String aes = getParameter("adminEmails");
        if (aes == null) {
            throw new ModuleInitializationException("adminEmails must be specified.",
                                                    getRole());
        }
        HashSet<String> adminEmails = new HashSet<String>();
        if (aes.indexOf(" ") == -1) {
            adminEmails.add(aes);
        } else {
            String[] emails = aes.split(" ");
            for (String element : emails) {
                adminEmails.add(element);
            }
        }
        HashSet<String> friends = new HashSet<String>();
        if (getParameter("friends") != null) {
            String f = getParameter("friends");
            if (f.indexOf(" ") == -1) {
                friends.add(f);
            } else {
                String[] fs = f.split(" ");
                for (String element : fs) {
                    friends.add(element);
                }
            }
        }
        FieldSearch fieldSearch =
                (FieldSearch) getServer()
                        .getModule("org.fcrepo.server.search.FieldSearch");
        if (fieldSearch == null) {
            throw new ModuleInitializationException("FieldSearch module was not loaded, but is required.",
                                                    getRole());
        }
        Module fsModule =
                getServer().getModule("org.fcrepo.server.search.FieldSearch");

        if (fsModule.getParameter("maxResults") == null) {
            throw new ModuleInitializationException("maxResults parameter must be specified in FieldSearch module's configuration.",
                                                    getRole());
        }
        int maxResults = 0;
        try {
            maxResults = Integer.parseInt(fsModule.getParameter("maxResults"));
            if (maxResults < 1) {
                throw new NumberFormatException("");
            }
        } catch (NumberFormatException nfe) {
            throw new ModuleInitializationException("maxResults specified in FieldSearch module's configuration must be a positive integer.",
                                                    getRole());
        }

        long maxSets = 100; // unused for now, but passed in the constructor anyway
        long maxRecords = maxResults;
        long maxHeaders = maxResults;
        String maxRecordsString = getParameter("maxRecords");
        if (maxRecordsString != null) {
            try {
                maxRecords = Long.parseLong(maxRecordsString);
                if (maxRecords > maxResults) {
                    logger.warn("maxRecords was over the limit given by the FieldSearch module, using highest possible value: "
                                    + maxResults);
                    maxRecords = maxResults;
                }
            } catch (NumberFormatException nfe) {
                throw new ModuleInitializationException("maxRecords value is invalid.",
                                                        getRole());
            }
        }
        String maxHeadersString = getParameter("maxHeaders");
        if (maxHeadersString != null) {
            try {
                maxHeaders = Long.parseLong(maxHeadersString);
                if (maxHeaders > maxResults) {
                    logger.warn("maxHeaders was over the limit given by the FieldSearch module, using highest possible value: "
                                    + maxResults);
                    maxHeaders = maxResults;
                }
            } catch (NumberFormatException nfe) {
                throw new ModuleInitializationException("maxHeaders value is invalid.",
                                                        getRole());
            }
        }
        m_wrappedOAIProvider =
                new FedoraOAIProvider(repositoryName,
                                      repositoryDomainName,
                                      host,
                                      "/" + context + "/oai",
                                      adminEmails,
                                      friends,
                                      pidNamespace,
                                      maxSets,
                                      maxRecords,
                                      maxHeaders,
                                      fieldSearch);
    }

    public String getRepositoryName() {
        return m_wrappedOAIProvider.getRepositoryName();
    }

    public String getBaseURL(String protocol, String port) {
        return m_wrappedOAIProvider.getBaseURL(protocol, port);
    }

    public String getProtocolVersion() {
        return m_wrappedOAIProvider.getProtocolVersion();
    }

    public Date getEarliestDatestamp() {
        return m_wrappedOAIProvider.getEarliestDatestamp();
    }

    public DeletedRecordSupport getDeletedRecordSupport() {
        return m_wrappedOAIProvider.getDeletedRecordSupport();
    }

    public DateGranularitySupport getDateGranularitySupport() {
        return m_wrappedOAIProvider.getDateGranularitySupport();
    }

    public Set<String> getAdminEmails() {
        return m_wrappedOAIProvider.getAdminEmails();
    }

    public Set<String> getSupportedCompressionEncodings() {
        return m_wrappedOAIProvider.getSupportedCompressionEncodings();
    }

    public Set<String> getDescriptions() {
        return m_wrappedOAIProvider.getDescriptions();
    }

    public Record getRecord(String identifier, String metadataPrefix)
            throws CannotDisseminateFormatException, IDDoesNotExistException,
            RepositoryException {
        return m_wrappedOAIProvider.getRecord(identifier, metadataPrefix);
    }

    public List<?> getRecords(Date from,
                           Date until,
                           String metadataPrefix,
                           String set) throws CannotDisseminateFormatException,
            NoRecordsMatchException, NoSetHierarchyException,
            RepositoryException {
        return m_wrappedOAIProvider
                .getRecords(from, until, metadataPrefix, set);
    }

    public List<?> getRecords(String resumptionToken)
            throws CannotDisseminateFormatException, NoRecordsMatchException,
            NoSetHierarchyException, BadResumptionTokenException,
            RepositoryException {
        return m_wrappedOAIProvider.getRecords(resumptionToken);
    }

    public List<?> getHeaders(Date from,
                           Date until,
                           String metadataPrefix,
                           String set) throws CannotDisseminateFormatException,
            NoRecordsMatchException, NoSetHierarchyException,
            RepositoryException {
        return m_wrappedOAIProvider
                .getHeaders(from, until, metadataPrefix, set);
    }

    public List<?> getHeaders(String resumptionToken)
            throws CannotDisseminateFormatException, NoRecordsMatchException,
            NoSetHierarchyException, BadResumptionTokenException,
            RepositoryException {
        return m_wrappedOAIProvider.getHeaders(resumptionToken);
    }

    public List<?> getSets() throws NoSetHierarchyException, RepositoryException {
        return m_wrappedOAIProvider.getSets();
    }

    public List<?> getSets(String resumptionToken)
            throws BadResumptionTokenException, NoSetHierarchyException,
            RepositoryException {
        return m_wrappedOAIProvider.getSets(resumptionToken);
    }

    public Set<MetadataFormat> getMetadataFormats(String id) throws NoMetadataFormatsException,
            IDDoesNotExistException, RepositoryException {
        return m_wrappedOAIProvider.getMetadataFormats(id);
    }

    public long getMaxSets() throws RepositoryException {
        return m_wrappedOAIProvider.getMaxSets();
    }

    public long getMaxRecords() throws RepositoryException {
        return m_wrappedOAIProvider.getMaxRecords();
    }

    public long getMaxHeaders() throws RepositoryException {
        return m_wrappedOAIProvider.getMaxHeaders();
    }

}
