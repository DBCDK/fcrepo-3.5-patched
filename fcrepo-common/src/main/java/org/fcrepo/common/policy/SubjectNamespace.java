/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.common.policy;

import org.jboss.security.xacml.sunxacml.attr.StringAttribute;

/**
 * The Fedora Subject XACML namespace.
 *
 * <pre>
 * Namespace URI    : urn:fedora:names:fedora:2.1:subject
 * </pre>
 */
public class SubjectNamespace
        extends XacmlNamespace {

    public final XacmlName LOGIN_ID;

    public final XacmlName ROLE;

    public final XacmlName USER_REPRESENTED;

    private SubjectNamespace(XacmlNamespace parent, String localName) {
        super(parent, localName);
        LOGIN_ID =
                addName(new XacmlName(this,
                                      "loginId",
                                      StringAttribute.identifier));
        ROLE =
                addName(new XacmlName(this,
                                      "role",
                                      StringAttribute.identifier));

        USER_REPRESENTED =
                addName(new XacmlName(this,
                                      "subjectRepresented",
                                      StringAttribute.identifier));
    }

    public static SubjectNamespace onlyInstance =
            new SubjectNamespace(Release2_1Namespace.getInstance(), "subject");

    public static final SubjectNamespace getInstance() {
        return onlyInstance;
    }

}
