/*
 * File: ResponseCache.java
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

package org.fcrepo.server.security.xacml.pep;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fcrepo.server.security.Attribute;
import org.fcrepo.server.security.RequestCtx;
import org.fcrepo.server.security.xacml.MelcoeXacmlException;
import org.fcrepo.server.security.xacml.util.AttributeComparator;
import org.fcrepo.server.security.xacml.util.ContextUtil;
import org.fcrepo.server.security.xacml.util.SubjectComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.security.xacml.sunxacml.ctx.ResponseCtx;
import org.jboss.security.xacml.sunxacml.ctx.Subject;

/**
 * @author nishen@melcoe.mq.edu.au
 */
public class ResponseCacheImpl
        implements ResponseCache {

    private static final Logger logger =
            LoggerFactory.getLogger(ResponseCacheImpl.class);

    private final ContextUtil m_contextUtil;

    private static final int DEFAULT_CACHE_SIZE = 1000;

    private static final long DEFAULT_TTL = 10 * 60 * 1000; // 10 minutes

    private static final Attribute[] ATTRIBUTE_TYPE = new Attribute[0];

    private static final AttributeComparator ATTRIBUTE_COMPARATOR = new AttributeComparator();

    private static final Subject[] SUBJECT_TYPE = new Subject[0];

    private static final SubjectComparator SUBJECT_COMPARATOR = new SubjectComparator();

    private final int CACHE_SIZE;

    private long TTL;

    private Map<String, ResponseCtx> requestCache = null;

    private Map<String, Long> requestCacheTimeTracker = null;

    private List<String> requestCacheUsageTracker = null;

    private MessageDigest digest = null;

    /**
     * The default constructor that initialises the cache with default values.
     *
     * @throws PEPException
     */
    public ResponseCacheImpl(ContextUtil contextUtil)
            throws PEPException {
        this(contextUtil, new Integer(DEFAULT_CACHE_SIZE), new Long(DEFAULT_TTL));
    }

    /**
     * Constructor that initialises the cache with the size and time to live
     * values.
     *
     * @param size
     *        size of the cache
     * @param ttl
     *        maximum time for a cache item to be valid in milliseconds
     * @throws PEPException
     */
    public ResponseCacheImpl(ContextUtil contextUtil, Integer size, Long ttl)
            throws PEPException {

        m_contextUtil = contextUtil;

        TTL = ttl.longValue();

        CACHE_SIZE = size.intValue();

        String noCache = System.getenv("PEP_NOCACHE");
        String noCacheProp = System.getProperty("fedora.fesl.pep_nocache");

        // if system property is set, use that
        if (noCacheProp != null && noCacheProp.toLowerCase().startsWith("t")) {
            TTL = 0;
        } else {
            // if system property is not set ..
            if (noCacheProp == null || noCacheProp.length() == 0) {
                // use env variable if set
                if (noCache != null && noCache.toLowerCase().startsWith("t")) {
                    TTL = 0;
                }
            }
        }

        // Note - HashMap, ArrayList are not thread-safe
        requestCache = new HashMap<String, ResponseCtx>(CACHE_SIZE);
        requestCacheTimeTracker = new HashMap<String, Long>(CACHE_SIZE);
        requestCacheUsageTracker = new ArrayList<String>(CACHE_SIZE);

        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            throw new PEPException("Could not initialize the ResponseCache", e);
        }
    }

    @Override
    public void setTTL(long ttl) {
        TTL = ttl;
    }

    /*
     * (non-Javadoc)
     * @see org.fcrepo.server.security.xacml.pep.ResponseCache#addCacheItem(java.lang.String,
     * java.lang.String)
     */
    @Override
    public void addCacheItem(String request, ResponseCtx response) {
        String hash = null;

        try {
            hash = makeHash(request);

            // thread-safety on cache operations
            synchronized (requestCache) {

                // if we have a maxxed cache, remove least used item
                if (requestCache.size() >= CACHE_SIZE) {
                    String key = requestCacheUsageTracker.remove(0);
                    requestCache.remove(key);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Purging cache element");
                    }
                }

                requestCache.put(hash, response);
                requestCacheUsageTracker.add(hash);
                requestCacheTimeTracker.put(hash, new Long(System
                                                           .currentTimeMillis()));
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Adding Cache Item (" + requestCache.size() + "/"
                             + requestCacheUsageTracker.size() + "/"
                             + requestCacheTimeTracker.size() + "): " + hash);
            }
        } catch (Exception e) {
            logger.warn("Error adding cache item: " + e.getMessage(), e);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.fcrepo.server.security.xacml.pep.ResponseCache#getCacheItem(java.lang.String)
     */
    @Override
    public ResponseCtx getCacheItem(String request) {
        String hash = null;
        ResponseCtx response = null;

        try {
            hash = makeHash(request);

            // thread-safety on cache operations
            synchronized (requestCache) {

                if (logger.isDebugEnabled()) {
                    logger.debug("Getting Cache Item (" + requestCache.size() + "/"
                                 + requestCacheUsageTracker.size() + "/"
                                 + requestCacheTimeTracker.size() + "): " + hash);
                }

                response = requestCache.get(hash);

                if (response == null) {
                    return null;
                }

                // if this item is older than CACHE_ITEM_TTL then we can't use it
                long usedLast =
                    System.currentTimeMillis()
                    - requestCacheTimeTracker.get(hash).longValue();
                if (usedLast > TTL) {
                    requestCache.remove(hash);
                    requestCacheUsageTracker.remove(hash);
                    requestCacheTimeTracker.remove(hash);

                    if (logger.isDebugEnabled()) {
                        logger.debug("CACHE_ITEM_TTL exceeded: " + hash);
                    }

                    return null;
                }

                // we just used this item, move it to the end of the list (items at
                // beginning get removed...)
                requestCacheUsageTracker.add(requestCacheUsageTracker
                                             .remove(requestCacheUsageTracker.indexOf(hash)));
            }
        } catch (Exception e) {
            logger.warn("Error getting cache item: " + e.getMessage(), e);
            response = null;
        }

        return response;
    }

    /*
     * (non-Javadoc)
     * @see org.fcrepo.server.security.xacml.pep.ResponseCache#invalidate()
     */
    @Override
    public void invalidate() {
        // thread-safety on cache operations
        synchronized (requestCache) {
            requestCache = new HashMap<String, ResponseCtx>(CACHE_SIZE);
            requestCacheTimeTracker = new HashMap<String, Long>(CACHE_SIZE);
            requestCacheUsageTracker = new ArrayList<String>(CACHE_SIZE);
        }
    }

    /**
     * Given a request, this method generates a hash.
     *
     * @param request
     *        the request to hash
     * @return the hash
     * @throws CacheException
     */
    private String makeHash(String request) throws CacheException {
        RequestCtx reqCtx = null;
        try {
            reqCtx = m_contextUtil.makeRequestCtx(request);
        } catch (MelcoeXacmlException pe) {
            throw new CacheException("Error converting request", pe);
        }
        byte[] hash = null;
        // ensure thread safety, don't want concurrent invocations of this method all modifying digest at once
        // (alternative is to construct a new digest for each(
        synchronized(digest) {
            digest.reset();

            hashSubjectList(reqCtx.getSubjectsAsList(), digest);

            hashAttributeList(reqCtx.getResourceAsList(), digest);

            hashAttributeList(reqCtx.getActionAsList(), digest);

            hashAttributeList(reqCtx.getEnvironmentAttributesAsList(), digest);

            hash = digest.digest();
        }

        return byte2hex(hash);
    }

    @SuppressWarnings("unchecked")
    private static void hashSubjectList(List<Subject> subjList, MessageDigest digest) {
        Subject[] subjs = subjList.toArray(SUBJECT_TYPE);
        Arrays.sort(subjs, SUBJECT_COMPARATOR);
        for (Subject s:subjs) {
            hashAttributeList(s.getAttributesAsList(), digest);
        }
    }

    private static void hashAttributeList(List<Attribute> attList, MessageDigest digest) {
        Attribute[] atts = attList.toArray(ATTRIBUTE_TYPE);
        Arrays.sort(atts, ATTRIBUTE_COMPARATOR);
        for (Attribute a:atts) {
            hashAttribute(a, digest);
        }
    }
    /**
     * Utility function to add an attribute to the hash digest.
     *
     * @param a
     *        the attribute to hash
     */
    private static void hashAttribute(Attribute a, MessageDigest dig) {
        dig.update(a.getId().toString().getBytes());
        dig.update(a.getType().toString().getBytes());
        dig.update(a.getValue().encode().getBytes());
        if (a.getIssuer() != null) {
            dig.update(a.getIssuer().getBytes());
        }
        if (a.getIssueInstant() != null) {
            dig.update(a.getIssueInstant().encode().getBytes());
        }
    }

    /**
     * Converts a hash into its hexadecimal string representation.
     *
     * @param bytes
     *        the byte array to convert
     * @return the hexadecimal string representation
     */
    private String byte2hex(byte[] bytes) {
        char[] hexChars =
                {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
                        'c', 'd', 'e', 'f'};

        StringBuffer sb = new StringBuffer();
        for (byte b : bytes) {
            sb.append(hexChars[b >> 4 & 0xf]);
            sb.append(hexChars[b & 0xf]);
        }

        return new String(sb);
    }
}