/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server;

import dk.dbc.opensearch.fedora.search.FieldSearchLucene;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.fcrepo.common.Constants;
import org.fcrepo.server.errors.DatastreamNotFoundException;
import org.fcrepo.server.errors.GeneralException;
import org.fcrepo.server.errors.ObjectNotFoundException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.errors.authorization.AuthzDeniedException;
import org.fcrepo.server.errors.authorization.AuthzOperationalException;
import org.fcrepo.server.errors.authorization.AuthzPermittedException;
import org.fcrepo.server.errors.servletExceptionExtensions.BadRequest400Exception;
import org.fcrepo.server.errors.servletExceptionExtensions.Continue100Exception;
import org.fcrepo.server.errors.servletExceptionExtensions.Forbidden403Exception;
import org.fcrepo.server.errors.servletExceptionExtensions.InternalError500Exception;
import org.fcrepo.server.errors.servletExceptionExtensions.NotFound404Exception;
import org.fcrepo.server.errors.servletExceptionExtensions.Ok200Exception;
import org.fcrepo.server.management.DefaultManagement;
import org.fcrepo.server.management.ManagementModule;
import org.fcrepo.server.security.Authorization;
import org.fcrepo.server.storage.DefaultDOManager;
import org.fcrepo.server.utilities.PIDStreamIterableWrapper;
import org.fcrepo.server.utilities.ServerUtilitySerializer;
import org.fcrepo.server.utilities.status.ServerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Server Controller.
 *
 * @author Chris Wilper
 */
public class ServerController
        extends SpringServlet {

    private static final Logger logger =
        LoggerFactory.getLogger(DefaultManagement.class);

    private static final long serialVersionUID = 1L;
    
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private static String PROTOCOL_FILE = "file:///";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String actionLabel = "server control";
        String action = request.getParameter("action");

        if (action == null) {
            throw new BadRequest400Exception(request,
                                             actionLabel,
                                             "no action",
                                             EMPTY_STRING_ARRAY);
        }

        if (action.equals("status")) {
            statusAction(request, response);
        } else if (action.equals("reloadPolicies")) {
            reloadPoliciesAction(request, response);
        } else if (action.equals("modifyDatastreamControlGroup")) {
            modifyDatastreamControlGroupAction(request, response);
        } else if (action.equals("modifyWriteAccess")) {
            modifyWriteAccessAction(request, response);
        }else {
            throw new BadRequest400Exception(request, actionLabel, "bad action:  "
                                             + action, EMPTY_STRING_ARRAY);
        }
    }

    private void statusAction(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String actionLabel = "getting server status";
            Context context =
                    ReadOnlyContext.getContext(Constants.HTTP_REQUEST.REST.uri,
                                               request);
            if (m_server == null) {
                throw new InternalError500Exception(request,
                                                    actionLabel,
                                                    "server not available",
                                                    EMPTY_STRING_ARRAY);
            }
            try {
                m_server.status(context);
            } catch (AuthzOperationalException aoe) {
                throw new Forbidden403Exception(request,
                                                actionLabel,
                                                "authorization failed",
                                                EMPTY_STRING_ARRAY);
            } catch (AuthzDeniedException ade) {
                throw new Forbidden403Exception(request,
                                                actionLabel,
                                                "authorization denied",
                                                EMPTY_STRING_ARRAY);
            } catch (AuthzPermittedException ape) {
                throw new Continue100Exception(request,
                                               actionLabel,
                                               "authorization permitted",
                                               EMPTY_STRING_ARRAY);
            } catch (Throwable t) {
                throw new InternalError500Exception(request,
                                                    actionLabel,
                                                    "error performing action2",
                                                    EMPTY_STRING_ARRAY);
            }
            throw new Ok200Exception(request,
                                     actionLabel,
                                     "server running",
                                     EMPTY_STRING_ARRAY);

        }

    private void reloadPoliciesAction(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String actionLabel = "reloading repository policies";
            Context context =
                    ReadOnlyContext.getContext(Constants.HTTP_REQUEST.REST.uri,
                                               request);
            if (m_server == null) {
                throw new InternalError500Exception(request,
                                                    actionLabel,
                                                    "server not available",
                                                    EMPTY_STRING_ARRAY);
            }
            Authorization authModule = null;
            authModule =
                    m_server.getBean("org.fcrepo.server.security.Authorization", Authorization.class);
            if (authModule == null) {
                throw new InternalError500Exception(request,
                                                    actionLabel,
                                                    "Required Authorization module not available",
                                                    EMPTY_STRING_ARRAY);
            }
            try {
                authModule.reloadPolicies(context);
            } catch (AuthzOperationalException aoe) {
                throw new Forbidden403Exception(request,
                                                actionLabel,
                                                "authorization failed",
                                                EMPTY_STRING_ARRAY);
            } catch (AuthzDeniedException ade) {
                throw new Forbidden403Exception(request,
                                                actionLabel,
                                                "authorization denied",
                                                EMPTY_STRING_ARRAY);
            } catch (AuthzPermittedException ape) {
                throw new Continue100Exception(request,
                                               actionLabel,
                                               "authorization permitted",
                                               EMPTY_STRING_ARRAY);
            } catch (Throwable t) {
                throw new InternalError500Exception(request,
                                                    actionLabel,
                                                    "error performing action2",
                                                    EMPTY_STRING_ARRAY);
            }
            throw new Ok200Exception(request,
                                     actionLabel,
                                     "server running",
                                     EMPTY_STRING_ARRAY);

        }


    private boolean getParameterAsBoolean(HttpServletRequest request, String name, boolean defaultValue) {

        String parameter = request.getParameter(name);
        boolean res;

        if (parameter == null || parameter.isEmpty()) {
            res = defaultValue;
        } else {
            if (parameter.toLowerCase().equals("true") || parameter.toLowerCase().equals("yes")) {
                res = true;
            } else if (parameter.toLowerCase().equals("false") || parameter.toLowerCase().equals("no")) {
                res = false;
            } else {
                throw new IllegalArgumentException("Invalid value " + parameter + " supplied for " + name + ".  Please use true or false");
    }
        }



        return res;
    }


    // FIXME: see FCREPO-765 - this should be migrated to an admin API (possibly with other methods from this class)
    private void modifyDatastreamControlGroupAction(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String actionLabel = "modifying datastream control group";

        Context context =
                ReadOnlyContext.getContext(Constants.HTTP_REQUEST.REST.uri,
                                           request);
        if (m_server == null) {
            throw new InternalError500Exception(request,
                                                actionLabel,
                                                "server not available",
                                                EMPTY_STRING_ARRAY);
        }
        // FIXME: see FCREPO-765 Admin methods are currently in DefaultManagement and carried through to ManagementModule
        ManagementModule apimDefault = (ManagementModule) m_server.getModule("org.fcrepo.server.management.Management");

        // FIXME: see FCREPO-765. tidy up output writing

        // get parameters
        String pid = request.getParameter("pid");
        String dsID = request.getParameter("dsID");
        String controlGroup = request.getParameter("controlGroup");
        boolean addXMLHeader = getParameterAsBoolean(request, "addXMLHeader", false);
        boolean reformat = getParameterAsBoolean(request, "reformat", false);
        boolean setMIMETypeCharset = getParameterAsBoolean(request, "setMIMETypeCharset", false);

        // get datastream list (single ds id is a list of one)
        String[] datastreams = dsID.split(",");

        // get iterable for pid looping
        boolean singlePID;
        Iterable<String> pids = null;
        if (pid.startsWith(PROTOCOL_FILE)) {
            File pidFile = new File(pid.substring(PROTOCOL_FILE.length()));
            pids = new PIDStreamIterableWrapper(new FileInputStream(pidFile));
            singlePID = false;

        } else { // pid list
            String[] pidList = pid.split(",");
            pids = new ArrayList<String>(Arrays.asList(pidList));
            singlePID = (pidList.length == 1);
        }

        try {

            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");

            PrintWriter pw = response.getWriter();

            // if doing a single pid/datastream, simple xml output
            if (singlePID && datastreams.length == 1) {
                response.setContentType("text/xml; charset=UTF-8");
                Date[] versions = apimDefault.modifyDatastreamControlGroup(context, pid, dsID, controlGroup, addXMLHeader, reformat, setMIMETypeCharset);
                pw.write("<versions>\n");
                    for (Date version : versions) {
                        pw.write("<version>" + version.toString() + "</version>\n");
                    }
                pw.write("</versions>\n");

            } else { // logging style output
                response.setContentType("text/plain; charset=UTF-8");
                ServerUtilitySerializer ser = new ServerUtilitySerializer(pw);
                for (String curpid : pids) {
                    ser.startObject(curpid);
                    for (String curdsID : datastreams) {
                        ser.startDatastream(curdsID);
                        Date[] versions;
                        try {
                            versions = apimDefault.modifyDatastreamControlGroup(context, curpid, curdsID, controlGroup, addXMLHeader, reformat, setMIMETypeCharset);
                        } catch (DatastreamNotFoundException e) {
                            versions = null;
                        }
                        ser.writeVersions(versions);
                        ser.endDatastream();
                    }
                    ser.endObject();
                }

                ser.finish();
            }
            pw.flush();


        } catch (ObjectNotFoundException e) {
            logger.error("Object not found: " + pid + " - " + e.getMessage());
            throw new NotFound404Exception(request,
                                           actionLabel,
                                           e.getMessage(),
                                           EMPTY_STRING_ARRAY);
        } catch (DatastreamNotFoundException e) {
            logger.error("Datastream not found: " + pid + "/" + dsID + " - " + e.getMessage());
            throw new NotFound404Exception(request,
                                           actionLabel,
                                           e.getMessage(),
                                           EMPTY_STRING_ARRAY);


        } catch (GeneralException e) {
            logger.error(e.getMessage());
            throw new InternalError500Exception(request,
                                                actionLabel,
                                                e.getMessage(),
                                                EMPTY_STRING_ARRAY);
        } catch (AuthzOperationalException aoe) {
            throw new Forbidden403Exception(request,
                                            actionLabel,
                                            "authorization failed",
                                            EMPTY_STRING_ARRAY);
        } catch (AuthzDeniedException ade) {
            throw new Forbidden403Exception(request,
                                            actionLabel,
                                            "authorization denied",
                                            EMPTY_STRING_ARRAY);
        } catch (AuthzPermittedException ape) {
            throw new Continue100Exception(request,
                                           actionLabel,
                                           "authorization permitted",
                                           EMPTY_STRING_ARRAY);

        } catch (ServerException e) {
            logger.error(e.getMessage(),e);
            throw new InternalError500Exception(request,
                                                actionLabel,
                                                "Unexpected error: " + e.getMessage(),
                                                EMPTY_STRING_ARRAY);
        }

    }

    private void modifyWriteAccessAction(HttpServletRequest request, HttpServletResponse response) throws InternalError500Exception {
        String actionLabel = "Modifying write access";
        logger.info(actionLabel);
        File fedoraHome = new File(Constants.FEDORA_HOME);  
        PrintWriter pw = null;
        Server server = null;
        DefaultDOManager doManager = null;
        FieldSearchLucene fsLucene = null;
        boolean disableWrites;
        int timeout;
        
        try {
            pw = response.getWriter();
        } catch (IOException ex) {
            logger.error("Failed to modify write access",ex);
            throw new InternalError500Exception(request, actionLabel, "Error getting resposne writer", new String[0]);
        }
        
        
        try{
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            
            try {
                server = Server.getInstance(fedoraHome, false);
            } catch (Exception ex) {
                throw new InternalError500Exception(request, actionLabel, "Error getting Server instance", new String[0]);
            } 
            
            if(server == null){
                throw new InternalError500Exception(request, actionLabel, "Server instance is null", new String[0]);
            }
            
            doManager = (DefaultDOManager) server.getModule("org.fcrepo.server.storage.DOManager");
            if(doManager == null){
                throw new InternalError500Exception(request, actionLabel, "Error getting DefaultDOManager", new String[0]);
            }
            
            fsLucene = (FieldSearchLucene) server.getModule("org.fcrepo.server.search.FieldSearch");
            if(fsLucene == null){
                throw new InternalError500Exception(request, actionLabel, "Error getting FieldSearchLucene", new String[0]);
            }
            
            try{
                disableWrites = Boolean.parseBoolean(request.getParameter("disable"));
                timeout = Integer.parseInt(request.getParameter("timeoutms"));
            }catch(NumberFormatException t){
                throw new InternalError500Exception(request, actionLabel, "Error parsing parameters. need disable={true|false} and timeoutms={integer}", new String[0]);
            }
            
            pw.write(actionLabel+"\n"); 
            if(disableWrites){

                doManager.setWritesDisabled(true);
                pw.write("- Write access disabled\n");  
                
                long start = System.currentTimeMillis();
                while(doManager.writesInProgress() > 0){        
                    if(System.currentTimeMillis()-start > timeout){                
                        String errorDetails = "Could not finish up writes within given time limit. Writes in progress: "+doManager.writesInProgress();
                        throw new InternalError500Exception(request, actionLabel, errorDetails, new String[0]);
                    }
                    pw.write("- Waiting.. Writes in progress: "+doManager.writesInProgress()+"\n");  
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        throw new InternalError500Exception(request, actionLabel, "Error thread couldn't sleep", new String[0]);
                    }
                }
                pw.write("- Current writes flushed succesfully\n");
             
                try {
                    fsLucene.flush();
                    pw.write("- Lucene WriteAheadLog flushed succesfully\n");
                } catch (Exception ex) {
                    throw new InternalError500Exception(request, actionLabel, "Could not flush Lucene WriteAheadLog"+ex.getMessage(), new String[0]);
                }
                pw.write("- All ok!");
            }else{
                doManager.setWritesDisabled(false);
                pw.write("- Write access enabled\n");
                pw.write("- All ok!");
            } 
        }catch(InternalError500Exception e){
            logger.error("Failed to modify write access: "+e.getDetail());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            pw.write(e.getDetail());   
            throw e;
        }finally{
            pw.flush();
            pw.close();
        }

    }

    @Override
    public void init() throws ServletException {
        super.init();
    }

    @Override
    public void destroy() {

        if (m_server != null) {
            try {
                m_status.append(ServerState.STOPPING,
                               "Shutting down Fedora Server and modules");
                m_server.shutdown(null);
                m_status.append(ServerState.STOPPED, "Shutdown Successful");
            } catch (Throwable th) {
                try {
                    m_status.appendError(ServerState.STOPPED_WITH_ERR, th);
                } catch (Exception e) {
                }
            }
            m_server = null;
        }

        super.destroy();
    }

    

}
