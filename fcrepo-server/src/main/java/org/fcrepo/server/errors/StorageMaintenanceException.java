/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also 
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.errors;

/**
 * Signals that writes are temporarily disabled.
 * 
 */
public class StorageMaintenanceException
        extends StorageException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates an WritesDisabledException.
     * 
     * @param message
     *        An informative message explaining what happened and (possibly) how
     *        to fix it.
     */
    public StorageMaintenanceException(String message) {
        super(message);
    }

}
