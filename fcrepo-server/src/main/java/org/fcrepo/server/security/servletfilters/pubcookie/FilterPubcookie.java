/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.security.servletfilters.pubcookie;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.apache.http.cookie.Cookie;
import org.fcrepo.server.security.servletfilters.BaseCaching;
import org.fcrepo.server.security.servletfilters.CacheElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * @author Bill Niebel
 * @deprecated
 */
@Deprecated
public class FilterPubcookie
        extends BaseCaching {

    private static final Logger logger =
            LoggerFactory.getLogger(FilterPubcookie.class);

    private static final Map<?,?> NO_REQUEST_PARAMETERS = Collections.emptyMap();

    private static final Cookie[] NO_COOKIES = new Cookie[0];

    public static final String PUBCOOKIE_NAME_KEY = "pubcookie-name";

    public static final String PUBCOOKIE_LOGINPAGE_URL_KEY =
            "pubcookie-loginpage-url";

    public static final String PUBCOOKIE_LOGINPAGE_FORM_NAME_KEY =
            "pubcookie-loginpage-form-name";

    public static final String PUBCOOKIE_LOGINPAGE_INPUT_NAME_USERID_KEY =
            "pubcookie-loginpage-input-name-userid";

    public static final String PUBCOOKIE_LOGINPAGE_INPUT_NAME_PASSWORD_KEY =
            "pubcookie-loginpage-input-name-password";

    public static final String TRUSTSTORE_LOCATION_KEY =
            "javax.net.ssl.trustStore";

    public static final String TRUSTSTORE_PASSWORD_KEY =
            "javax.net.ssl.trustStorePassword";

    public static final String KEYSTORE_LOCATION_KEY = "javax.net.ssl.keyStore";

    public static final String KEYSTORE_PASSWORD_KEY =
            "javax.net.ssl.keyStorePassword";

    private String PUBCOOKIE_NAME = "";

    private String PUBCOOKIE_LOGINPAGE_URL = "";

    private String PUBCOOKIE_LOGINPAGE_FORM_NAME = "";

    private String PUBCOOKIE_LOGINPAGE_INPUT_NAME_USERID = "";

    private String PUBCOOKIE_LOGINPAGE_INPUT_NAME_PASSWORD = "";

    private String TRUSTSTORE_LOCATION = null;

    private String TRUSTSTORE_PASSWORD = null;

    @Override
    public void destroy() {
        String method = "destroy()";
        if (logger.isDebugEnabled()) {
            logger.debug(enter(method));
        }
        super.destroy();
        if (logger.isDebugEnabled()) {
            logger.debug(exit(method));
        }
    }

    @Override
    protected void initThisSubclass(String key, String value) {
        String method = "initThisSubclass()";
        if (logger.isDebugEnabled()) {
            logger.debug(enter(method));
        }
        boolean setLocally = false;
        if (PUBCOOKIE_NAME_KEY.equals(key)) {
            PUBCOOKIE_NAME = value;
            setLocally = true;
        } else if (PUBCOOKIE_LOGINPAGE_URL_KEY.equals(key)) {
            PUBCOOKIE_LOGINPAGE_URL = value;
            setLocally = true;
        } else if (PUBCOOKIE_LOGINPAGE_FORM_NAME_KEY.equals(key)) {
            PUBCOOKIE_LOGINPAGE_FORM_NAME = value;
            setLocally = true;
        } else if (PUBCOOKIE_LOGINPAGE_INPUT_NAME_USERID_KEY.equals(key)) {
            PUBCOOKIE_LOGINPAGE_INPUT_NAME_USERID = value;
            setLocally = true;
        } else if (PUBCOOKIE_LOGINPAGE_INPUT_NAME_PASSWORD_KEY.equals(key)) {
            PUBCOOKIE_LOGINPAGE_INPUT_NAME_PASSWORD = value;
            setLocally = true;
        } else if (TRUSTSTORE_LOCATION_KEY.equals(key)) {
            TRUSTSTORE_LOCATION = value;
            setLocally = true;
        } else if (TRUSTSTORE_PASSWORD_KEY.equals(key)) {
            TRUSTSTORE_PASSWORD = value;
            setLocally = true;
        } else {
            if (logger.isErrorEnabled()) {
                logger.error(format(method, "deferring to super"));
            }
            super.initThisSubclass(key, value);
        }
        if (setLocally) {
            if (logger.isInfoEnabled()) {
                logger.info(format(method, "known parameter", key, value));
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug(exit(method));
        }
    }

    private final String getAction(Node parent,
                                   String pubcookieLoginpageFormName) {
        String method = "getAction()";
        if (logger.isDebugEnabled()) {
            logger.debug(enter(method));
        }
        String action = "";
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            String tag = child.getNodeName();
            if ("form".equalsIgnoreCase(tag)) {
                NamedNodeMap attributes = child.getAttributes();
                Node nameNode = attributes.getNamedItem("name");
                String name = nameNode.getNodeValue();
                Node actionNode = attributes.getNamedItem("action");
                if (pubcookieLoginpageFormName.equalsIgnoreCase(name)
                        && actionNode != null) {
                    action = actionNode.getNodeValue();
                    break;
                }
            }
            action = getAction(child, pubcookieLoginpageFormName);
            if (!"".equals(action)) {
                break;
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug(exit(method));
        }
        return action;
    }

    //initial, setup call
    private final Map<String, String> getFormFields(Node parent) {
        String method = "getFormFields(Node parent)";
        if (logger.isDebugEnabled()) {
            logger.debug(enter(method));
        }
        Map<String, String> formfields = new Hashtable<String, String>();
        getFormFields(parent, formfields);
        if (logger.isDebugEnabled()) {
            logger.debug(exit(method));
        }
        return formfields;
    }

    //inner, recursive call
    private final void getFormFields(Node parent, Map<String, String> formfields) {
        String method = "getFormFields(Node parent, Map formfields)";
        if (logger.isDebugEnabled()) {
            logger.debug(enter(method));
        }
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            String tag = child.getNodeName();
            if ("input".equalsIgnoreCase(tag)) {
                NamedNodeMap attributes = child.getAttributes();
                Node typeNode = attributes.getNamedItem("type");
                String type = typeNode.getNodeValue();
                Node nameNode = attributes.getNamedItem("name");
                String name = nameNode.getNodeValue();
                Node valueNode = attributes.getNamedItem("value");
                String value = "";
                if (valueNode != null) {
                    value = valueNode.getNodeValue();
                }
                if ("hidden".equalsIgnoreCase(type) && value != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(format("capturing hidden fields", name, value));
                    }
                    formfields.put(name, value);
                }
            }
            getFormFields(child, formfields);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(exit(method));
        }
    }

    @Override
    public void populateCacheElement(CacheElement cacheElement, String password) {
        String method = "populateCacheElement()";
        if (logger.isDebugEnabled()) {
            logger.debug(enter(method));
        }
        Boolean authenticated = null;
        ConnectPubcookie tidyConnect = new ConnectPubcookie();
        if (logger.isDebugEnabled()) {
            logger.debug(format(method, "b4 first connect()", "tidyConnect"));
            logger.debug(tidyConnect.toString());
            logger.debug(format(method,
                             null,
                             "PUBCOOKIE_LOGINPAGE_URL",
                             PUBCOOKIE_LOGINPAGE_URL));
        }
        tidyConnect.connect(PUBCOOKIE_LOGINPAGE_URL,
                            NO_REQUEST_PARAMETERS,
                            NO_COOKIES,
                            TRUSTSTORE_LOCATION,
                            TRUSTSTORE_PASSWORD);
        if (!tidyConnect.completedFully()) {
            if (logger.isInfoEnabled()) {
                logger.info(format(method, "form page did not load"));
            }
        } else {
            Cookie[] formpageCookies = tidyConnect.getResponseCookies();
            Node formpageDocument = tidyConnect.getResponseDocument();
            String action =
                    getAction(formpageDocument, PUBCOOKIE_LOGINPAGE_FORM_NAME);
            if (logger.isDebugEnabled()) {
                logger.debug(format(method, "action", action));
            }
            Map<String, String> formpageFields = getFormFields(formpageDocument);
            Iterator<String> iter = null;

            if (logger.isDebugEnabled()) {
                iter = formpageFields.keySet().iterator();
                while (iter.hasNext()) {
                    String key = iter.next();
                    logger.debug(format(method, null, key, (String) formpageFields
                            .get(key)));
                }
            }

            formpageFields.put(PUBCOOKIE_LOGINPAGE_INPUT_NAME_USERID,
                               cacheElement.getUserid());
            formpageFields.put(PUBCOOKIE_LOGINPAGE_INPUT_NAME_PASSWORD,
                               password);

            if (logger.isDebugEnabled()) {
                iter = formpageFields.keySet().iterator();
                while (iter.hasNext()) {
                    String key = (String) iter.next();
                    logger.debug(format(method,
                                     " form field after",
                                     key,
                                     (String) formpageFields.get(key)));
                }
            }

            tidyConnect = new ConnectPubcookie();
            if (logger.isDebugEnabled()) {
                logger.debug(format(method, "b4 second connect()"));
            }
            tidyConnect.connect(action,
                                formpageFields,
                                formpageCookies,
                                TRUSTSTORE_LOCATION,
                                TRUSTSTORE_PASSWORD);
            if (!tidyConnect.completedFully()) {
                if (logger.isDebugEnabled()) {
                    logger.debug(format(method, "result page did not load"));
                }
            } else {
                Cookie[] resultpageCookies = tidyConnect.getResponseCookies();
                if (logger.isDebugEnabled()) {
                    logger.debug(format(method, " cookies receieved", "n", Integer
                            .toString(resultpageCookies.length)));
                }
                for (Cookie cookie : resultpageCookies) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(format(method,
                                         "another cookie",
                                         "cookie name" + cookie.getName()));
                        logger.debug(format(method,
                                         "another cookie",
                                         "length",
                                         Integer.toString(cookie.getName()
                                                 .length())));
                    }
                    if (PUBCOOKIE_NAME.equals(cookie.getName())) {
                        if (logger.isInfoEnabled()) {
                            logger.debug(format(method,
                                             " found pubcookie login cookie"));
                        }
                        authenticated = Boolean.TRUE;
                        break;
                    }
                }
                if (authenticated == null) {
                    authenticated = Boolean.FALSE;
                } else {
                    if (!authenticated.booleanValue()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug(format(method, "didn't find a pubcookie login cookie"));
                        }
                    }
                }
            }
            cacheElement.populate(authenticated, null, null, null);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(exit(method));
        }
    }

}
