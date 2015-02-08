/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.storage.lowlevel;

import java.io.InputStream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ConnectionPoolNotFoundException;
import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.storage.ConnectionPool;
import org.fcrepo.server.storage.ConnectionPoolManager;


/**
 * @author Edwin Shin
 * @version $Id$
 */
public class DefaultLowlevelStorageModule
        extends Module
        implements ILowlevelStorage, IListable, ISizable, ICheckable {

    private DefaultLowlevelStorage m_llstore;

    public DefaultLowlevelStorageModule(Map<String, String> moduleParameters,
                                        Server server,
                                        String role)
            throws ModuleInitializationException {
        super(moduleParameters, server, role);
    }

    @Override
    public void postInitModule() throws ModuleInitializationException {
        try {
            m_llstore = new DefaultLowlevelStorage(getModuleParameters());
        } catch (LowlevelStorageException e) {
            throw new ModuleInitializationException(e.getMessage(), getRole());
        }
    }

    protected Map<String, Object> getModuleParameters() throws ModuleInitializationException {
        // Parameter validation
        String objectStoreBase =
                getModuleParameter(DefaultLowlevelStorage.OBJECT_STORE_BASE,
                                   true);
        String datastreamStoreBase =
                getModuleParameter(DefaultLowlevelStorage.DATASTREAM_STORE_BASE,
                                   true);
        // FIXME object/datastream store location sanity checks (e.g. no overlapping)

        String filesystem =
                getModuleParameter(DefaultLowlevelStorage.FILESYSTEM, false);
        String pathAlgorithm =
                getModuleParameter(DefaultLowlevelStorage.PATH_ALGORITHM, false);
        String pathRegistry =
                getModuleParameter(DefaultLowlevelStorage.PATH_REGISTRY, false);

        // parameter required by DBPathRegistry
        String backslashIsEscape;
        String param =
                getModuleParameter("backslash_is_escape", false).toLowerCase();
        if (param.equals("true") || param.equals("false")) {
            backslashIsEscape = param;
        } else {
            throw new ModuleInitializationException("backslash_is_escape parameter must be either true or false",
                                                    getRole());
        }

        // get connectionPool from ConnectionPoolManager
        ConnectionPoolManager cpm =
                (ConnectionPoolManager) getServer()
                        .getModule("org.fcrepo.server.storage.ConnectionPoolManager");
        if (cpm == null) {
            throw new ModuleInitializationException("ConnectionPoolManager module was required, but apparently has "
                                                            + "not been loaded.",
                                                    getRole());
        }

        ConnectionPool cPool;
        try {
            cPool = cpm.getPool();
        } catch (ConnectionPoolNotFoundException e1) {
            throw new ModuleInitializationException("Could not find requested "
                    + "connectionPool.", getRole());
        }

        Map<String, Object> configuration = new HashMap<String, Object>();
        configuration.put(DefaultLowlevelStorage.FILESYSTEM, filesystem);
        configuration.put(DefaultLowlevelStorage.PATH_ALGORITHM, pathAlgorithm);
        configuration.put(DefaultLowlevelStorage.PATH_REGISTRY, pathRegistry);
        configuration.put(DefaultLowlevelStorage.OBJECT_STORE_BASE,
                          objectStoreBase);
        configuration.put(DefaultLowlevelStorage.DATASTREAM_STORE_BASE,
                          datastreamStoreBase);
        configuration.put("connectionPool", cPool);
        configuration.put("backslashIsEscape", backslashIsEscape);

        return configuration;
    }

    protected String getModuleParameter(String parameterName,
                                        boolean parameterAsAbsolutePath)
            throws ModuleInitializationException {
        String parameterValue =
                getParameter(parameterName, parameterAsAbsolutePath);

        if (parameterValue == null) {
            throw new ModuleInitializationException(parameterName
                    + " parameter must be specified", getRole());
        }
        return parameterValue;
    }

    public void addObject(String pid, InputStream content, Map<String, String> hints)
            throws LowlevelStorageException {
        m_llstore.addObject(pid, content, hints);
    }

    public void replaceObject(String pid, InputStream content, Map<String, String> hints)
            throws LowlevelStorageException {
        m_llstore.replaceObject(pid, content, hints);
    }

    public InputStream retrieveObject(String pid)
            throws LowlevelStorageException {
        return m_llstore.retrieveObject(pid);
    }

    public void removeObject(String pid) throws LowlevelStorageException {
        m_llstore.removeObject(pid);
    }

    public void rebuildObject() throws LowlevelStorageException {
        m_llstore.rebuildObject();
    }

    public void auditObject() throws LowlevelStorageException {
        m_llstore.auditObject();
    }

    public long addDatastream(String pid, InputStream content, Map<String, String> hints)
            throws LowlevelStorageException {
        return m_llstore.addDatastream(pid, content, hints);
    }

    public long replaceDatastream(String pid, InputStream content, Map<String, String> hints)
            throws LowlevelStorageException {
        return m_llstore.replaceDatastream(pid, content, hints);
    }

    public InputStream retrieveDatastream(String pid)
            throws LowlevelStorageException {
        return m_llstore.retrieveDatastream(pid);
    }

    public void removeDatastream(String pid) throws LowlevelStorageException {
        m_llstore.removeDatastream(pid);
    }

    public void rebuildDatastream() throws LowlevelStorageException {
        m_llstore.rebuildDatastream();
    }

    public void auditDatastream() throws LowlevelStorageException {
        m_llstore.auditDatastream();
    }

    // IListable methods

    public Iterator<String> listObjects() {
        return m_llstore.listObjects();
    }

    public Iterator<String> listDatastreams() {
        return m_llstore.listDatastreams();
    }

    // ISizable methods

    @Override
    public long getDatastreamSize(String dsKey) throws LowlevelStorageException {
        return m_llstore.getDatastreamSize(dsKey);
    }
    
    // ICheckable methods
    @Override
    public boolean objectExists(String objectKey) {
        return m_llstore.objectExists(objectKey);
    }

 }
