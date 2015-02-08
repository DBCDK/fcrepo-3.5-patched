/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.access;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.Hashtable;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * This servlet accepts the result of a posted web form containing information
 * about which method parameter values were selected for a dissemination
 * request.
 * <p>
 * The information is read from the form and translated into the corresponding
 * API-A-LITE interface dissemination request in the form of a URI. The initial
 * request is then redirected to the API-A-LITE interface to execute the
 * dissemination request.
 *
 * @author Ross Wayland
 * @version $Id$
 */
public class MethodParameterResolverServlet
        extends HttpServlet {

    private static final Logger logger =
            LoggerFactory.getLogger(MethodParameterResolverServlet.class);

    private static final long serialVersionUID = 1L;

    /** A string constant for the html MIME type */
    private static final String HTML_CONTENT_TYPE = "text/html; charset=UTF-8";

    /**
     * Treat Get request identical to Post request.
     *
     * @param request
     *        The servlet request.
     * @param response
     *        The servlet response.
     * @throws ServletException
     *         If an error occurs that affects the servlet's basic operation.
     * @throws IOException
     *         If an error occurs within an input or output operation.
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        doPost(request, response);
    }

    /**
     * Process Post request from web form.
     *
     * @param request
     *        The servlet request.
     * @param response
     *        The servlet response.
     * @throws ServletException
     *         If an error occurs that affects the servlet's basic operation.
     * @throws IOException
     *         If an error occurs within an input or output operation.
     */
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String PID = null;
        String sDefPID = null;
        String methodName = null;
        String versDateTime = null;
        StringBuffer methodParms = new StringBuffer();
        response.setContentType(HTML_CONTENT_TYPE);
        Hashtable<String,String> h_methodParms = new Hashtable<String,String>();

        // Get parameters passed from web form.
        @SuppressWarnings("unchecked")
        Enumeration<String> parms = request.getParameterNames();
        while (parms.hasMoreElements()) {
            String name = parms.nextElement();
            if (name.equals("PID")) {
                PID = request.getParameter(name);
            } else if (name.equals("sDefPID")) {
                sDefPID = request.getParameter(name);
            } else if (name.equals("methodName")) {
                methodName = request.getParameter(name);
            } else if (name.equals("asOfDateTime")) {
                versDateTime = request.getParameter(name).trim();
                if (versDateTime.equalsIgnoreCase("null")
                        || versDateTime.equalsIgnoreCase("")) {
                    versDateTime = null;
                }
            } else if (name.equals("Submit")) {
                // Submit parameter is ignored.
            } else {
                // Any remaining parameters are assumed to be method parameters
                // so
                // decode and place in hashtable.
                h_methodParms.put(name, request.getParameter(name));
            }
        }

        // Check that all required parameters are present.
        if (PID == null || PID.equalsIgnoreCase("") || sDefPID == null
                || sDefPID.equalsIgnoreCase("") || methodName == null
                || methodName.equalsIgnoreCase("")) {
            String message =
                    "[MethodParameterResolverServlet] Insufficient "
                            + "information to construct dissemination request. Parameters "
                            + "received from web form were: PID: " + PID
                            + " -- sDefPID: " + sDefPID + " -- methodName: "
                            + methodName + " -- methodParms: "
                            + methodParms.toString() + "\".  ";
            logger.error(message);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                               message);
        } else {
            // Translate web form parameters into dissemination request.
            StringBuffer redirectURL = new StringBuffer();
            redirectURL.append(request.getContextPath() + "/get/" + PID + "/"
                    + sDefPID + "/" + methodName);

            // Add method parameters.
            int i = 0;
            for (Enumeration<String> e = h_methodParms.keys(); e.hasMoreElements();) {
                String name =
                        URLEncoder.encode(e.nextElement(), "UTF-8");
                String value =
                        URLEncoder.encode(h_methodParms.get(name),
                                          "UTF-8");
                i++;
                if (i == h_methodParms.size()) {
                    methodParms.append(name + "=" + value);
                } else {
                    methodParms.append(name + "=" + value + "&");
                }

            }
            if (h_methodParms.size() > 0) {
                if (versDateTime == null || versDateTime.equalsIgnoreCase("")) {
                    redirectURL.append("?" + methodParms.toString());

                } else {
                    redirectURL.append("/" + versDateTime + "?"
                            + methodParms.toString());
                }
            } else {
                if (versDateTime == null || versDateTime.equalsIgnoreCase("")) {
                    redirectURL.append("/");
                } else {
                    redirectURL.append("/" + versDateTime + "/");
                }
            }

            // Redirect request back to FedoraAccessServlet.
            response.sendRedirect(redirectURL.toString());
        }
    }

    // Clean up resources
    @Override
    public void destroy() {
    }

}
