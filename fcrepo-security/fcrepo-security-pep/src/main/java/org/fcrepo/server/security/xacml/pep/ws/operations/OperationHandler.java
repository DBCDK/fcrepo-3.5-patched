/*
 * File: OperationHandler.java
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

package org.fcrepo.server.security.xacml.pep.ws.operations;

import javax.xml.ws.handler.soap.SOAPMessageContext;

import org.fcrepo.server.security.RequestCtx;

public interface OperationHandler {

    /**
     * Method to handle requests.
     *
     * @param context
     *        the message context
     * @return a RequestCtx if necessary or else null
     * @throws OperationHandlerException
     */
    public RequestCtx handleRequest(SOAPMessageContext context)
            throws OperationHandlerException;

    /**
     * Method to handle responses.
     *
     * @param context
     *        the message context
     * @return a RequestCtx if necessary or else null
     * @throws OperationHandlerException
     */
    public RequestCtx handleResponse(SOAPMessageContext context)
            throws OperationHandlerException;
}
