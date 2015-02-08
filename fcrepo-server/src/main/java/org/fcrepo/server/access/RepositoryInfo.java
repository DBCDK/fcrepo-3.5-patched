/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also 
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.access;

/**
 * Data structure to contain a key information about the repository.
 * 
 * <p>This information is the return value of the API-A 
 * <code>describeRepository</code> request.
 * 
 * @author Sandy Payette
 */
public class RepositoryInfo {
    
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    public String repositoryName = null;

    public String repositoryBaseURL = null;

    public String repositoryVersion = null;

    public String repositoryPIDNamespace = null;

    public String defaultExportFormat = null;

    public String OAINamespace = null;

    public String[] adminEmailList = EMPTY_STRING_ARRAY;

    public String samplePID = null;

    public String sampleOAIIdentifer = null;

    public String sampleSearchURL = null;

    public String sampleAccessURL = null;

    public String sampleOAIURL = null;

    public String[] retainPIDs = EMPTY_STRING_ARRAY;
}
