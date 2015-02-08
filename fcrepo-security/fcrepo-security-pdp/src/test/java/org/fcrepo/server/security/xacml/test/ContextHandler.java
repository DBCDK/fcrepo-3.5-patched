/*
 * File: ContextHandler.java
 *
 * Copyright 2007 Macquarie E-Learning Centre Of Excellence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.server.security.xacml.test;


import org.fcrepo.server.security.RequestCtx;
import org.fcrepo.server.security.xacml.pdp.MelcoePDP;
import org.fcrepo.server.security.xacml.util.ContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.security.xacml.sunxacml.ctx.ResponseCtx;

/**
 * Borrowed <b>heavily</b> from sunxacml samples SampleRequestBuilder.java
 *
 * @author nishen@melcoe.mq.edu.au
 */
public class ContextHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(ContextHandler.class);

    private static final ContextUtil contextUtil = new ContextUtil();

    private MelcoePDP m_melcoePDP;

    public ContextHandler() {
    }

    public void setPDP(MelcoePDP melcoePDP) {
        m_melcoePDP = melcoePDP;
    }



    /**
     * @param reqCtx
     *        an XACML request context for resolution.
     * @return an XACML response context based on the evaluation of the request
     *         context.
     */
    public ResponseCtx evaluate(RequestCtx reqCtx) throws Exception {
        logger.debug("Resolving RequestCtx request!");

        String request = contextUtil.makeRequestCtx(reqCtx);
        String response = evaluate(request);
        ResponseCtx resCtx = contextUtil.makeResponseCtx(response);

        return resCtx;
    }

    /**
     * @param req
     *        an XACML request context for resolution.
     * @return an XACML response context based on the evaluation of the request
     *         context.
     */
    public String evaluate(String req) throws Exception {
        String res = m_melcoePDP.evaluate(req);
        return res;
    }
}