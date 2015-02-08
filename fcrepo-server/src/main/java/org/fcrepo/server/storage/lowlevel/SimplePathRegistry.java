/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.storage.lowlevel;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;

import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ObjectNotInLowlevelStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Bill Niebel
 */
class SimplePathRegistry
        extends PathRegistry {

    private static final Logger logger =
            LoggerFactory.getLogger(PathRegistry.class);

    private Hashtable<String, String> hashtable = null;

    public SimplePathRegistry(Map<String, ?> configuration)
            throws LowlevelStorageException {
        super(configuration);
        rebuild();
    }
    
    @Override
    public boolean exists(String pid) throws LowlevelStorageException {
        try {
            return hashtable.containsKey(pid);
        } catch (Exception e) {
            throw new LowlevelStorageException(true, "SimplePathRegistry.exists("
                    + pid + ")", e);
        }
    }

    @Override
    public String get(String pid) throws LowlevelStorageException {
        String result;
        try {
            result = hashtable.get(pid);
        } catch (Exception e) {
            throw new LowlevelStorageException(true, "SimplePathRegistry.get("
                    + pid + ")", e);
        }
        if (null == result || 0 == result.length()) {
            throw new ObjectNotInLowlevelStorageException("SimplePathRegistry.get("
                    + pid + "): object not found");
        }
        return result;
    }

    @Override
    public void put(String pid, String path) throws LowlevelStorageException {
        try {
            hashtable.put(pid, path);
        } catch (Exception e) {
            throw new LowlevelStorageException(true, "SimplePathRegistry.put("
                    + pid + ")", e);
        }
    }

    @Override
    public void remove(String pid) throws LowlevelStorageException {
        try {
            hashtable.remove(pid);
        } catch (Exception e) {
            throw new LowlevelStorageException(true,
                                               "SimplePathRegistry.remove("
                                                       + pid + ")",
                                               e); // <<===
        }
    }

    @Override
    public void auditFiles() throws LowlevelStorageException {
        logger.info("begin audit:  files-against-registry");
        traverseFiles(storeBases, AUDIT_FILES, false, FULL_REPORT);
        logger.info("end audit:  files-against-registry (ending normally)");
    }

    @Override
    public void rebuild() throws LowlevelStorageException {
        int report = FULL_REPORT;
        Hashtable<String, String> temp = hashtable;
        hashtable = new Hashtable<String, String>();
        try {
            logger.info("begin rebuilding registry from files");
            traverseFiles(storeBases, REBUILD, false, report); // allows bad files
            logger.info("end rebuilding registry from files (ending normally)");
        } catch (Exception e) {
            hashtable = temp;
            if (report != NO_REPORT) {
                logger.error("ending rebuild unsuccessfully", e);
            }
            throw new LowlevelStorageException(true,
                                               "ending rebuild unsuccessfully",
                                               e); //<<====
        }
    }

    @Override
    public Enumeration<String> keys() throws LowlevelStorageException {
        return hashtable.keys();
    }
}
