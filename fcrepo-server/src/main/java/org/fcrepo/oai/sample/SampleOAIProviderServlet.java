/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.oai.sample;

import org.fcrepo.oai.OAIProviderServlet;
import org.fcrepo.oai.OAIResponder;
import org.fcrepo.oai.RepositoryException;

/**
 * @author Chris Wilper
 */
public class SampleOAIProviderServlet
        extends OAIProviderServlet {

    private static final long serialVersionUID = 1L;

    private OAIResponder m_responder;

    @Override
    public OAIResponder getResponder() throws RepositoryException {
        if (m_responder == null) {
            m_responder = new OAIResponder(new SampleOAIProvider(),null);
        }
        return m_responder;
    }

    public static void main(String[] args) throws Exception {
        new SampleOAIProviderServlet().test(args);
    }

}
