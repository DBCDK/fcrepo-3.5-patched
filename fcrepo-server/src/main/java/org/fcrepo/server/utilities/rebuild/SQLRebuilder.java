/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.utilities.rebuild;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.fcrepo.common.Constants;
import org.fcrepo.common.Models;

import org.fcrepo.server.Context;
import org.fcrepo.server.ReadOnlyContext;
import org.fcrepo.server.Server;
import org.fcrepo.server.config.DatastoreConfiguration;
import org.fcrepo.server.config.ModuleConfiguration;
import org.fcrepo.server.config.Parameter;
import org.fcrepo.server.config.ServerConfiguration;
import org.fcrepo.server.errors.InitializationException;
import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.errors.StorageDeviceException;
import org.fcrepo.server.management.PIDGenerator;
import org.fcrepo.server.search.FieldSearch;
import org.fcrepo.server.storage.ConnectionPool;
import org.fcrepo.server.storage.ConnectionPoolManager;
import org.fcrepo.server.storage.DOManager;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.lowlevel.ILowlevelStorage;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.fcrepo.server.storage.types.RelationshipTuple;
import org.fcrepo.server.utilities.TableSpec;

/**
 * A Rebuilder for the SQL database.
 */
public class SQLRebuilder
        implements Rebuilder {

    private static final Logger logger =
            LoggerFactory.getLogger(Rebuilder.class);
    public static final String THREADS_OPTION = "threads";

    private ServerConfiguration m_serverConfig;

    private Server m_server;

    private ConnectionPool m_connectionPool;

    private Context m_context;

    ExecutorService threadPool;

    /**
     * Get a short phrase describing what the user can do with this rebuilder.
     */
    @Override
    public String getAction() {
        return "Rebuild SQL database.";
    }

    /**
     * Returns true is the server _must_ be shut down for this rebuilder to
     * safely operate.
     */
    @Override
    public boolean shouldStopServer() {
        return true;
    }

    /**
     * Initialize the rebuilder, given the server configuration.
     *
     * @returns a map of option names to plaintext descriptions.
     */
    @Override
    public void setServerConfiguration(ServerConfiguration serverConfig) {
        m_serverConfig = serverConfig;
    }

    @Override
    public void setServerDir(File serverBaseDir) {

    }

    @Override
    public void init() {

    }

    @Override
    public Map<String, String> getOptions()
 {
        Map<String, String> m = new HashMap<String, String>();
        m.put( THREADS_OPTION, "number of threads to use for index rebuild" );
        return m;
    }

    /**
     * Validate the provided options and perform any necessary startup tasks.
     */
    @Override
    public void start(Map<String, String> options) throws Exception {
        int threads = Integer.parseInt(options.get(THREADS_OPTION));
        threadPool = new ThreadPoolExecutor(threads, threads,
                                      0L, TimeUnit.MILLISECONDS,
                                      new ArrayBlockingQueue<Runnable>( 500 ),
                                      new ThreadPoolExecutor.CallerRunsPolicy());

        // This must be done before starting "RebuildServer"
        // rather than after, so any application caches
        // (in particular the hash map held by PIDGenerator)
        // don't get out of sync with the database.
        blankExistingTables();

        try {
            m_server = Rebuild.getServer();
            // now get the connectionpool
            ConnectionPoolManager cpm =
                    (ConnectionPoolManager) m_server
                            .getModule("org.fcrepo.server.storage.ConnectionPoolManager");
            if (cpm == null) {
                throw new ModuleInitializationException("ConnectionPoolManager not loaded.",
                                                        "ConnectionPoolManager");
            }
            m_connectionPool = cpm.getPool();
            m_context =
                    ReadOnlyContext.getContext("utility", "fedoraAdmin", "", /* null, */
                    ReadOnlyContext.DO_OP);
            ILowlevelStorage llstore =
                    (ILowlevelStorage) m_server
                            .getModule("org.fcrepo.server.storage.lowlevel.ILowlevelStorage");
            try {
                llstore.rebuildObject();
                llstore.rebuildDatastream();
            } catch (LowlevelStorageException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        } catch (InitializationException ie) {
            logger.error("Error initializing", ie);
            throw ie;
        }
    }

    public static List<String> getExistingTables(Connection conn)
            throws SQLException {

        ArrayList<String> existing = new ArrayList<String>();
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet r = null;
        // Get a list of tables that don't exist, if any
        try {
            r = dbMeta.getTables(null, null, "%", null);
            while (r.next()) {
                existing.add(r.getString("TABLE_NAME"));
            }
            r.close();
            r = null;
        } finally {
            try {
                if (r != null) {
                    r.close();
                }
            } finally {
                r = null;
            }
        }
        return existing;
    }

    /**
     * Delete all rows from all Fedora-related tables (except the resource index
     * ones) that exist in the database.
     */
    public void blankExistingTables() {
        Connection connection = null;
        Statement s = null;
        try {
            connection = getDefaultConnection();
            List<String> existingTables = getExistingTables(connection);
            List<String> fedoraTables = getFedoraTables();
            s = connection.createStatement();
            for (int i = 0; i < existingTables.size(); i++) {
                String origTableName = existingTables.get(i);
                String tableName = origTableName.toUpperCase();
                if (fedoraTables.contains(tableName)
                        && !tableName.startsWith("RI")) {
                    System.out.println("Cleaning up table: " + origTableName);
                    try {
                        s.executeUpdate("DELETE FROM " + origTableName);
                    } catch (Exception lle) {
                        System.err.println(lle.getMessage());
                        System.err.flush();
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB error while blanking existing tables",
                                       e);
        } finally {
            try {
            	if (s != null) {
            		s.close();
            		s = null;
            	}
                connection.close();
            } catch (Exception e) {
            }
        }
    }

    /**
     * Get the names of all Fedora tables listed in the server's dbSpec file.
     * Names will be returned in ALL CAPS so that case-insensitive comparisons
     * can be done.
     */
    private List<String> getFedoraTables() {
        try {
            String dbSpecLocation =
                    "org/fcrepo/server/storage/resources/DefaultDOManager.dbspec";
            InputStream in =
                    getClass().getClassLoader()
                            .getResourceAsStream(dbSpecLocation);
            List<TableSpec> specs = TableSpec.getTableSpecs(in);
            ArrayList<String> names = new ArrayList<String>();
            for (int i = 0; i < specs.size(); i++) {
                TableSpec spec = specs.get(i);
                names.add(spec.getName().toUpperCase());
            }
            return names;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unexpected error reading dbspec file",
                                       e);
        }
    }

    /**
     * Add the data of interest for the given object.
     */
    @Override
    public void addObject(final DigitalObject obj) {

        Runnable task = new Runnable() {
            @Override
            public void run()
            {
                // CURRENT TIME:
                // Get the current time to use for created dates on object
                // and object components (if they are not already there).
                Date nowUTC = new Date();

                // DOReplicator replicator=(DOReplicator)
                // m_server.getModule("org.fcrepo.server.storage.replication.DOReplicator");
                DOManager manager =
                        (DOManager) m_server
                                .getModule("org.fcrepo.server.storage.DOManager");
                FieldSearch fieldSearch =
                        (FieldSearch) m_server
                                .getModule("org.fcrepo.server.search.FieldSearch");
                PIDGenerator pidGenerator =
                        (PIDGenerator) m_server
                                .getModule("org.fcrepo.server.management.PIDGenerator");

                // SET OBJECT PROPERTIES:
                logger.debug("Rebuild: Setting object/component states and create dates if unset...");
                // set object state to "A" (Active) if not already set
                if (obj.getState() == null || obj.getState().equals("")) {
                    obj.setState("A");
                }
                // set object create date to UTC if not already set
                if (obj.getCreateDate() == null || obj.getCreateDate().equals("")) {
                    obj.setCreateDate(nowUTC);
                }
                // set object last modified date to UTC
                obj.setLastModDate(nowUTC);

                // SET OBJECT PROPERTIES:
                logger.debug("Rebuild: Setting object/component states and create dates if unset...");
                // set object state to "A" (Active) if not already set
                if (obj.getState() == null || obj.getState().equals("")) {
                    obj.setState("A");
                }
                // set object create date to UTC if not already set
                if (obj.getCreateDate() == null || obj.getCreateDate().equals("")) {
                    obj.setCreateDate(nowUTC);
                }
                // set object last modified date to UTC
                obj.setLastModDate(nowUTC);

                // SET DATASTREAM PROPERTIES...
                Iterator<String> dsIter = obj.datastreamIdIterator();
                while (dsIter.hasNext()) {
                    for (Datastream ds : obj.datastreams(dsIter.next())) {
                        // Set create date to UTC if not already set
                        if (ds.DSCreateDT == null || ds.DSCreateDT.equals("")) {
                            ds.DSCreateDT = nowUTC;
                        }
                        // Set state to "A" (Active) if not already set
                        if (ds.DSState == null || ds.DSState.equals("")) {
                            ds.DSState = "A";
                        }
                    }
                }

                // GET DIGITAL OBJECT WRITER:
                // get an object writer configured with the DEFAULT export format
                // barmintor: this appears to be unused code, commenting out with intent to delete
                //logger.debug("INGEST: Instantiating a SimpleDOWriter...");
                //try {
                //    DOWriter w =
                //            manager.getWriter(Server.USE_DEFINITIVE_STORE,
                //                              m_context,
                //                              obj.getPid());
                //} catch (ServerException se) {
                //}

                // PID GENERATION:
                // have the system generate a PID if one was not provided
                logger.debug("INGEST: Stream contained PID with retainable namespace-id... will use PID from stream.");
                try {
                    pidGenerator.neverGeneratePID(obj.getPid());
                } catch (IOException e) {
                    throw new RuntimeException("Error calling pidGenerator.neverGeneratePID(): "
                                                       + e.getMessage(),
                                               e);
                }

                // REGISTRY:
                // at this point the object is valid, so make a record
                // of it in the digital object registry
                try {
                    registerObject(obj);
                } catch (StorageDeviceException e) {
                }

                try {
                    logger.info("COMMIT: Attempting replication: " + obj.getPid());
                    DOReader reader =
                            manager.getReader(Server.USE_DEFINITIVE_STORE,
                                              m_context,
                                              obj.getPid());
                    logger.info("COMMIT: Updating FieldSearch indexes...");
                    fieldSearch.update(reader);

                } catch (ServerException se) {
                    System.out.println("Error while replicating: "
                            + se.getClass().getName() + ": " + se.getMessage());
                    se.printStackTrace();
                } catch (Throwable th) {
                    System.out.println("Error while replicating: "
                            + th.getClass().getName() + ": " + th.getMessage());
                    th.printStackTrace();
                }
            }
        };
        threadPool.execute( task);
    }

    /**
     * Adds a new object.
     */
    private void registerObject(DigitalObject obj)
            throws StorageDeviceException {

        Connection conn = null;
        try {
            conn = m_connectionPool.getReadWriteConnection();
            if (obj.hasContentModel(Models.SERVICE_DEPLOYMENT_3_0)){
                updateDeploymentMap(obj, conn);
            }
        } catch (SQLException sqle) {
            throw new StorageDeviceException("Unexpected error from SQL database while registering object: "
                    + sqle.getMessage());
        } finally {
            if (conn != null) {
                m_connectionPool.free(conn);
            }
        }
    }

    /**
     * Free up any system resources associated with rebuilding.
     */
    @Override
    public void finish() {
        threadPool.shutdown();
        try
        {
            threadPool.awaitTermination( 14, TimeUnit.DAYS );
        }
        catch( InterruptedException ex )
        {
            System.out.println("Interrupted while waiting for completion: "
                    + ex.getClass().getName() + ": " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * Gets a connection to the database specified in connection pool module's
     * "defaultPoolName" config value. This allows us to the connect to the
     * database without the server running.
     */
    private Connection getDefaultConnection() {
        ModuleConfiguration poolConfig =
                m_serverConfig
                        .getModuleConfiguration("org.fcrepo.server.storage.ConnectionPoolManager");
        String datastoreID =
                poolConfig.getParameter("defaultPoolName",Parameter.class).getValue();
        DatastoreConfiguration dbConfig =
                m_serverConfig.getDatastoreConfiguration(datastoreID);
        return getConnection(dbConfig.getParameter("jdbcDriverClass",Parameter.class)
                                     .getValue(),
                             dbConfig.getParameter("jdbcURL",Parameter.class).getValue(),
                             dbConfig.getParameter("dbUsername",Parameter.class).getValue(),
                             dbConfig.getParameter("dbPassword",Parameter.class).getValue());
    }

    private static Connection getConnection(String driverClass,
                                            String url,
                                            String username,
                                            String password) {
        try {
            Class.forName(driverClass);
            return DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException("Error getting database connection", e);
        }
    }

    /**
     * Update the registry and deployment cache to reflect the latest state of
     * reality.
     *
     * @param obj
     *        DOReader of a service deployment object
     */
    private synchronized void updateDeploymentMap(DigitalObject obj,
                                                  Connection c)
            throws SQLException {

        Set<RelationshipTuple> sDefs =
                obj.getRelationships(Constants.MODEL.IS_DEPLOYMENT_OF, null);
        Set<RelationshipTuple> models =
                obj.getRelationships(Constants.MODEL.IS_CONTRACTOR_OF, null);

        for (RelationshipTuple sDefTuple : sDefs) {
            String sDef = sDefTuple.getObjectPID();
            for (RelationshipTuple cModelTuple : models) {
                String cModel = cModelTuple.getObjectPID();
                addDeployment(cModel, sDef, obj, c);
            }
        }
    }

    private void addDeployment(String cModel,
                               String sDef,
                               DigitalObject sDep,
                               Connection c) throws SQLException {

        String query =
        	"INSERT INTO modelDeploymentMap (cModel, sDef, sDep) VALUES (?, ?, ?)";
        PreparedStatement s = c.prepareStatement(query);

        try {
            s.setString(1, cModel);
            s.setString(2, sDef);
            s.setString(3, sDep.getPid());
            s.executeUpdate();
        } finally {
            if (s != null) {
                s.close();
            }
        }
    }
}
