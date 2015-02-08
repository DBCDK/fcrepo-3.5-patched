/*
 * File: ContextHandlerTest.java
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

import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fcrepo.common.Constants;
import org.fcrepo.server.security.RequestCtx;
import org.fcrepo.server.security.xacml.pep.ContextHandlerImpl;
import org.fcrepo.server.security.xacml.pep.EvaluationEngine;
import org.fcrepo.server.security.xacml.util.ContextUtil;
import org.fcrepo.server.security.xacml.util.RelationshipResolver;
import org.fcrepo.server.utilities.CXFUtility;
import org.jboss.security.xacml.sunxacml.attr.AnyURIAttribute;
import org.jboss.security.xacml.sunxacml.attr.AttributeValue;
import org.jboss.security.xacml.sunxacml.attr.StringAttribute;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


/**
 * @author nishen@melcoe.mq.edu.au
 */
@RunWith(MockitoJUnitRunner.class)
public class ContextHandlerTest {

    private ContextHandlerImpl test;

    private ContextUtil fixtureUtil;
    @Mock
    private RelationshipResolver mockResolver;
    @Mock
    private EvaluationEngine mockEval;
    @Before
    public void setUp() throws Exception{
        fixtureUtil = new ContextUtil();
        test = new ContextHandlerImpl();
        test.setContextUtil(fixtureUtil);
        test.setRelationshipResolver(mockResolver);
        test.setEvaluationEngine(mockEval);
        when(mockResolver.buildRESTParentHierarchy("FedoraRepository")).thenReturn("/FedoraRepository");
    }
    @Test
    public void testRoundTrip() throws Exception {
        RequestCtx req;

        long a, b;
        String request;
        String response;

        request =
                "<Request><Subject SubjectCategory=\"urn:oasis:names:tc:xacml:1.0:subject-category:access-subject\"><Attribute AttributeId=\"urn:oasis:names:tc:xacml:1.0:subject:subject-id\" DataType=\"http://www.w3.org/2001/XMLSchema#string\"><AttributeValue>fedoraAdmin</AttributeValue></Attribute></Subject><Subject SubjectCategory=\"urn:oasis:names:tc:xacml:1.0:subject-category:access-subject\"><Attribute AttributeId=\"urn:fedora:names:fedora:2.1:subject:loginId\" DataType=\"http://www.w3.org/2001/XMLSchema#string\"><AttributeValue>fedoraAdmin</AttributeValue></Attribute><Attribute AttributeId=\"urn:fedora:names:fedora:2.1:subject:subjectRepresented\" DataType=\"http://www.w3.org/2001/XMLSchema#string\"><AttributeValue>fedoraAdmin</AttributeValue></Attribute></Subject><Resource><Attribute AttributeId=\"urn:fedora:names:fedora:2.1:resource:object:pid\" DataType=\"http://www.w3.org/2001/XMLSchema#string\"><AttributeValue>FedoraRepository</AttributeValue></Attribute><Attribute AttributeId=\"urn:oasis:names:tc:xacml:1.0:resource:resource-id\" DataType=\"http://www.w3.org/2001/XMLSchema#anyURI\"><AttributeValue>FedoraRepository</AttributeValue></Attribute></Resource><Action><Attribute AttributeId=\"urn:fedora:names:fedora:2.1:action:api\" DataType=\"http://www.w3.org/2001/XMLSchema#string\"><AttributeValue>urn:fedora:names:fedora:2.1:action:api-a</AttributeValue></Attribute><Attribute AttributeId=\"urn:fedora:names:fedora:2.1:action:id\" DataType=\"http://www.w3.org/2001/XMLSchema#string\"><AttributeValue>urn:fedora:names:fedora:2.1:action:id-describeRepository</AttributeValue></Attribute></Action></Request>";
        a = System.nanoTime();

        req =
                test.buildRequest(getSubjects("nishen"),
                                 getActions(),
                                 getResources().get(0),
                                 getEnvironment());
        
        response = test.evaluate(request);

        b = System.nanoTime();
        req.encode(System.out);

        System.out.println(response);
        System.out.println("Time taken: " + (b - a) + " nanos");
    }

    private static List<Map<URI, AttributeValue>> getResources()
            throws URISyntaxException {
        Map<URI, AttributeValue> resAttr = null;
        List<Map<URI, AttributeValue>> resList =
                new ArrayList<Map<URI, AttributeValue>>();

        resAttr = new HashMap<URI, AttributeValue>();
        resAttr.put(Constants.OBJECT.PID.getURI(),
                    new StringAttribute("melcoe:test:01"));
        resAttr
                .put(new URI("urn:oasis:names:tc:xacml:1.0:resource:resource-id"),
                     new AnyURIAttribute(new URI("melcoe:test:01")));
        resAttr.put(Constants.DATASTREAM.ID.getURI(),
                    new AnyURIAttribute(new URI("melcoe:test:02")));
        resAttr.put(Constants.DATASTREAM.MIME_TYPE.getURI(),
                    new StringAttribute("text/xml"));
        resAttr.put(Constants.DATASTREAM.FORMAT_URI.getURI(),
                    new AnyURIAttribute(new URI("some:format:or:the:other")));
        resAttr.put(Constants.DATASTREAM.LOCATION.getURI(),
                    new StringAttribute("http://www.whipitgood.com"));
        resAttr.put(Constants.DATASTREAM.CONTROL_GROUP.getURI(),
                    new StringAttribute("E"));
        resAttr.put(Constants.DATASTREAM.STATE.getURI(),
                    new StringAttribute("ACTIVE"));
        resList.add(resAttr);

        resAttr = new HashMap<URI, AttributeValue>();
        resAttr.put(Constants.OBJECT.PID.getURI(),
                    new StringAttribute("melcoe:test:03"));
        resAttr
                .put(new URI("urn:oasis:names:tc:xacml:1.0:resource:resource-id"),
                     new AnyURIAttribute(new URI("melcoe:test:03")));
        resAttr.put(Constants.DATASTREAM.ID.getURI(),
                    new AnyURIAttribute(new URI("melcoe:test:04")));
        resAttr.put(Constants.DATASTREAM.MIME_TYPE.getURI(),
                    new StringAttribute("text/xml"));
        resAttr.put(Constants.DATASTREAM.FORMAT_URI.getURI(),
                    new AnyURIAttribute(new URI("some:format:or:the:other")));
        resAttr.put(Constants.DATASTREAM.LOCATION.getURI(),
                    new StringAttribute("http://www.whipitgood.com"));
        resAttr.put(Constants.DATASTREAM.CONTROL_GROUP.getURI(),
                    new StringAttribute("E"));
        resAttr.put(Constants.DATASTREAM.STATE.getURI(),
                    new StringAttribute("ACTIVE"));
        resList.add(resAttr);

        resAttr = new HashMap<URI, AttributeValue>();
        resAttr.put(Constants.OBJECT.PID.getURI(),
                    new StringAttribute("melcoe:test:05"));
        resAttr
                .put(new URI("urn:oasis:names:tc:xacml:1.0:resource:resource-id"),
                     new AnyURIAttribute(new URI("melcoe:test:05")));
        resAttr.put(Constants.DATASTREAM.ID.getURI(),
                    new AnyURIAttribute(new URI("melcoe:test:06")));
        resAttr.put(Constants.DATASTREAM.MIME_TYPE.getURI(),
                    new StringAttribute("text/xml"));
        resAttr.put(Constants.DATASTREAM.FORMAT_URI.getURI(),
                    new AnyURIAttribute(new URI("some:format:or:the:other")));
        resAttr.put(Constants.DATASTREAM.LOCATION.getURI(),
                    new StringAttribute("http://www.whipitgood.com"));
        resAttr.put(Constants.DATASTREAM.CONTROL_GROUP.getURI(),
                    new StringAttribute("E"));
        resAttr.put(Constants.DATASTREAM.STATE.getURI(),
                    new StringAttribute("ACTIVE"));
        resList.add(resAttr);

        return resList;
    }

    private static Map<URI, AttributeValue> getActions()
            throws URISyntaxException {
        Map<URI, AttributeValue> actions = new HashMap<URI, AttributeValue>();
        actions.put(Constants.ACTION.API.getURI(),
                    new StringAttribute(Constants.ACTION.APIM.getURI()
                            .toASCIIString()));
        actions.put(Constants.ACTION.ID.getURI(),
                    new StringAttribute(Constants.ACTION.ADD_DATASTREAM
                            .getURI().toASCIIString()));
        actions.put(new URI("urn:oasis:names:tc:xacml:1.0:action:action-id"),
                    new StringAttribute(Constants.ACTION.ADD_DATASTREAM
                            .getURI().toASCIIString()));
        return actions;
    }

    private static List<Map<URI, List<AttributeValue>>> getSubjects(String uid){
        // setup the id and value for the requesting subject
        Map<URI, List<AttributeValue>> subAttr =
                new HashMap<URI, List<AttributeValue>>();
        List<AttributeValue> attrList = null;
        try {
            attrList = new ArrayList<AttributeValue>();
            attrList.add(new StringAttribute(uid));
            subAttr.put(Constants.SUBJECT.LOGIN_ID.getURI(), attrList);

            attrList = new ArrayList<AttributeValue>();
            attrList.add(new StringAttribute(uid));
            subAttr
                    .put(new URI("urn:oasis:names:tc:xacml:1.0:subject:subject-id"),
                         attrList);
        } catch (URISyntaxException use) {
            throw CXFUtility.getFault(use);
        }

        List<Map<URI, List<AttributeValue>>> subjects =
                new ArrayList<Map<URI, List<AttributeValue>>>();
        subjects.add(subAttr);

        return subjects;
    }

    private static Map<URI, AttributeValue> getEnvironment() {
        return new HashMap<URI, AttributeValue>();
    }
}
