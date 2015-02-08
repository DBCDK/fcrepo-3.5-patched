/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also 
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.storage.service;

import java.util.Hashtable;

import org.fcrepo.server.storage.types.MethodDef;


/**
 * @author Sandy Payette
 */
public class MmapMethodDef
        extends MethodDef {

    // For linkages to WSDL
    public String wsdlOperationName = null;

    public String wsdlMessageName = null;

    public String wsdlOutputMessageName = null;

    public MmapMethodParmDef[] wsdlMsgParts = new MmapMethodParmDef[0];

    public Hashtable<String, MmapMethodParmDef> wsdlMsgPartToParmDefTbl;

    public MmapMethodDef() {
    }

}
