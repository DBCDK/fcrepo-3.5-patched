/*
  This file is part of opensearch.
  Copyright © 2009, Dansk Bibliotekscenter a/s,
  Tempovej 7-11, DK-2750 Ballerup, Denmark. CVR: 15149043

  opensearch is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  opensearch is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with opensearch.  If not, see <http://www.gnu.org/licenses/>.
 */


package dk.dbc.opensearch.fedora.search;


import org.fcrepo.common.rdf.RDFName;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.InvalidStateException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ModuleShutdownException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.errors.UnknownSessionTokenException;
import org.fcrepo.server.search.FieldSearch;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.storage.DOManager;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.types.DatastreamXMLMetadata;
import org.fcrepo.server.storage.types.RelationshipTuple;
import org.fcrepo.server.utilities.DCField;
import org.fcrepo.server.utilities.DCFields;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;


/**
 * <p>
 * This FieldSearch implementation uses a dedicated Lucene index for indexing
 * fields from the modifying operations of fedora (delete, ingest, modify)
 * FieldSearchLucene provides the roles of both Module and FieldSearch
 * implementations.
 * </p>
 * <p>
 * Regarding https://jira.duraspace.org/browse/FCREPO-793:
 * </p>
 * <p>
 * Because of some quite unintuitive uses of UTC and conversions from
 * UTC to localtime, this implementation will have some specific
 * expectations to certain input values.
 * </p>
 * <p>
 * Expectations to update input:
 * <ul>
 * <li> The properties of the Digital Object (in its serialized form) "createdDate" and "modifiedDate" are expected to be delivered in localtime formatted.</li>
 * <li> All dates given in the Dublin Core datastream in the Digital Object are expected to be delivered in UTC.</li>
 * <li> All dates used as parameters in searches are expected to be delivered in UTC.</li>
 * </ul>
 * </p>
 * <p>
 * If any or all of the above criteria are not met, there can be given
 * no guarantees of the correctness of queries involving dates or the
 * internal representation of dates.
 * </p>
 */
public final class FieldSearchLucene extends Module implements FieldSearch
{
    private final String role;
    private DOManager doManager;
    private static final Logger log = LoggerFactory.getLogger( FieldSearchLucene.class );

    private FieldSearchLuceneImpl fsl;

    /**
     * Constructor for initializing the FieldSearch module. The server will
     * initialize one instance of this class. Most of the server specific
     * initialization will be performed in the
     * {@link FieldSearchLucene#postInitModule()} method.
     *
     * @param moduleParameters a {@link Map} of parameters from the server
     * @param server an instance of the {@link fedora.server.Server} with access to objects and information about these
     * @param moduleRole a String describing this module to the server
     * @throws ModuleInitializationException if the super class {@link fedora.server.Module} could not be properly initialized
     */
    public FieldSearchLucene( final Map<String, String> moduleParameters, final Server server, final String moduleRole ) throws ModuleInitializationException
    {
        super( moduleParameters, server, moduleRole );
        log.trace( "Entered FieldSearchLucene constructor" );
        this.role = moduleRole;
        log.trace( "Finished constructing FieldSearchLucene instance" );
    }


    @Override
    public void postInitModule() throws ModuleInitializationException
    {
        log.trace( "Initializing module with parameters" );

        // Version:
        String version = this.getClass().getPackage().getImplementationVersion();
        log.info( "Running FieldSearchLucene version {}", version );

        // doManager        
        doManager = (DOManager) getServer().getModule( "org.fcrepo.server.storage.DOManager" );
        if( null == doManager )
        {
            String error = "FATAL: DOManager module was required, but apparently has not been loaded.";
            log.error( error );
            throw new ModuleInitializationException( error, getRole() );
        }
        log.trace( "Retrieved DOManager instance" );

        // resultLifeTimeInSeconds
        String resultLifetime = getParameter( "resultLifetime" );
        if ( resultLifetime.equals( "" ) )
        {
            String error = "FATAL: parameter resultLifetime must be specified.";
            log.error( error );
            throw new ModuleInitializationException( error, getRole() );
        }
        int resultLifeTimeInSeconds = Integer.parseInt( resultLifetime );
        log.debug( "resultLifeTimeInSeconds = {}", resultLifetime );

        // luceneWriteLockTimeout
        String writeLockTimeout = getParameter( "writeLockTimeout" );
        if ( writeLockTimeout.equals( "" ) )
        {
            String error = "FATAL: parameter writeLockTimeout must be specified.";
            log.error( error );
            throw new ModuleInitializationException( error, getRole() );
        }
        long luceneWriteLockTimeout = Long.parseLong( writeLockTimeout );
        log.debug( "luceneWritelockTimeout = {}", writeLockTimeout );

        // directory
        String sDirectory = getParameter( "luceneDirectory" );
        if ( sDirectory.equals( "" ) )
        {
            String error = "FATAL: parameter luceneDirectory must be specified.";
            log.error( error );
            throw new ModuleInitializationException( error, getRole() );
        }
        Directory directory;
        try
        {
            directory = initializeDirectoryString( sDirectory );
        } 
        catch( IOException ex )
        {
            String error = String.format( "FATAL: Could not initialize lucene directory '%s': %s", sDirectory, ex.getMessage() );
            log.error( error );
            throw new ModuleInitializationException( error, getRole() );
        }
        log.debug( "LuceneDirectory: {}", sDirectory );

        // luceneindexer
        Analyzer analyzer = new WhitespaceAnalyzer( Version.LUCENE_35 );
        try
        {
            fsl = new FieldSearchLuceneImpl( luceneWriteLockTimeout, analyzer, directory, resultLifeTimeInSeconds );
        } 
        catch( IOException ex )
        {
            log.error( "FATAL:", ex );
            throw new ModuleInitializationException( ex.getMessage(), this.role, ex );
        }
        log.trace( "Constructed LuceneIndex instance" );

    }


    @Override
    public void update( final DOReader reader ) throws ServerException
    {
        log.trace( "Entering update" );

        log.trace( "Preparing object data fields for indexing" );

        Date fedoraCreateDate = reader.getCreateDate();
        Date fedoraLastModDate = reader.getLastModDate();
        String objectPID = reader.GetObjectPID();
        String objectState = reader.GetObjectState();
        String objectLabel = reader.GetObjectLabel();
        String ownerId = reader.getOwnerId();

        DCFields dcFields = null;
        Date dcmCreatedDate = null;
        DatastreamXMLMetadata dcmd = null;
        try
        {
            dcmd = (DatastreamXMLMetadata) reader.GetDatastream( "DC", null );
            log.debug( "Retrieved Dublin Core metadata from digital object" );
        } 
        catch( ClassCastException cce )
        {
            String pid = objectPID;
            String error = String.format( "Object %s has a DC datastream, but it's not inline XML.", pid );
            log.error( error, cce );
            log.warn( "This means that the object with pid {} will have no dc values indexed", pid );
        }
        if( null != dcmd )
        {
            dcFields = new DCFields( dcmd.getContentStream() );
            dcmCreatedDate = dcmd.DSCreateDT;
        }

        Set< RelationshipTuple > relations;
        try
        {
            // Include relationships in lucene index.
            relations = reader.getRelationships();
        }
        catch ( ServerException e )
        {
            log.error( "ServerException from DOReader getRelationships: pid [{}]", objectPID );
            throw( e );
        }

        try
        {
            this.fsl.update( fedoraCreateDate,
                             fedoraLastModDate,
                             objectPID,
                             objectState,
                             objectLabel,
                             ownerId,
                             dcFields,
                             dcmCreatedDate,
                             relations );
        } 
        catch( CorruptIndexException ex )
        {
            // because of insufficient information on this exception type, the
            // information provided here is almost meaningless. I just hope that
            // clients call .getMessage or .getStackTrace from the wrapped ex
            // instance
            String error = String.format( "FATAL: Could not write fields to index: %s", ex.getMessage() );
            log.error( error, ex );
            throw new InvalidStateException( error );
        } 
        catch( IOException ex )
        {
            // because of insufficient information on this exception type, the
            // information provided here is almost meaningless. I just hope that
            // clients call .getMessage og .getStackTrace from the wrapped ex
            // instance
            String error = String.format( "FATAL: Could not write fields to index: %s", ex.getMessage() );
            log.error( error, ex );
            throw new InvalidStateException( error );
        }
    }


    @Override
    public boolean delete( final String identifier ) throws ServerException
    {
        boolean res = false;
        try {
            res = this.fsl.delete( identifier );
        } catch( CorruptIndexException ex ) {
            log.error( "FATAL: Could not delete object {}: {}", identifier, ex.getMessage() );
            throw new InvalidStateException( "bundleName", "code", new String[]
                    {
                        "replacements"
                    }, new String[]
                    {
                        "details"
                    }, ex );
        }
        return res;
    }


    /**
     * Find {@link FedoraFieldName object resultFields} filtered by {@code resultFields}, limited
     * by {@code maxResults} and specified by the {@code FieldSearchQuery} object.
     * 
     * @param returnFields the resultFields for which values should be returned
     * @param maxResults maximum number of hits for each of the resultFields in {@code resultFields}
     * @param fsq the query, wrapped in a {@code FieldSearchQuery} object
     * @return A FieldSearchResult object containing the results of the query
     * @throws ServerException if an error occurs with the search
     */
    @Override
    public FieldSearchResult findObjects( final String[] returnFields, final int maxResults, final FieldSearchQuery fsq ) throws ServerException
    {
        log.trace( "Entering findObjects" );

        String[] validReturnFields = FieldSearchLuceneImpl.getValidatedReturnFields( returnFields );

        if( 0 == validReturnFields.length )
        {
            String error = "No valid return fields provided";
            log.error( error );
            throw new InvalidStateException( error );
        }

        log.trace( "Retrieving search result" );
        FieldSearchResult fsr = new FieldSearchResultLucene( this.fsl.luceneindexer, this.doManager, validReturnFields, fsq, maxResults, this.fsl.resultLifeTimeInSeconds );

        String currentToken = fsr.getToken();

        if( null != currentToken ) //no more search results
        {
            log.debug( "Caching result with token '{}'", currentToken );
            FieldSearchResult previousValue = this.fsl.cachedResult.putIfAbsent( currentToken, fsr );
            if( null != previousValue )
            {
                log.debug( "Replaced FieldSearchResult with token {} in cache with FieldSearchResult with token {}", previousValue.getToken(), currentToken );
            }
            else
            {
                log.debug( "No cached results for token {} found", currentToken );
            }
        }

        log.trace( "Returning new search result from findObjects" );
        return fsr;

        // return fsl.findObjects( validReturnFields, maxResults, fsq );
    }


    @Override
    public FieldSearchResult resumeFindObjects( final String token ) throws ServerException
    {
        log.trace( "Entering resumingFindObjects" );
        if( !this.fsl.cachedResult.containsKey( token ) )
        {
            String error = "Session is expired or never existed.";
            log.error( error );
            throw new UnknownSessionTokenException( error );
        }

        FieldSearchResultLucene cachedFsr = ( (FieldSearchResultLucene) this.fsl.cachedResult.get( token ) );
        if( null == cachedFsr )
        {
            String error = String.format( "The token %s should refer to a cached result, but none was available", token );
            log.error( error );
            throw new IllegalStateException( error );
        }

        FieldSearchResult fsr = cachedFsr.stepAndCacheResult();

        this.fsl.cachedResult.remove( token );

        String currentToken = fsr.getToken();

        if( null != currentToken ) //no more search results
        {
            log.debug( "Caching result with token '{}'", currentToken );
            FieldSearchResult previousValue = this.fsl.cachedResult.putIfAbsent( currentToken, fsr );
            if( null != previousValue )
            {
                log.debug( "Replaced FieldSearchResult with token {} in cache with FieldSearchResult with token {}", previousValue.getToken(), currentToken );
            }
            else
            {
                log.debug( "No cached results for token {} found", currentToken );
            }
        }

        log.debug( "Retrieved and returning cached result" );
        return fsr;
    }


    /**
     * Closes the underlying lucene indexer.
     *
     * @throws ModuleShutdownException if the underlying lucene indexer class can not be shutdown properly
     */
    @Override
    public void shutdownModule() throws ModuleShutdownException
    {
        try
        {
            this.fsl.shutdown();
        } 
        catch( IOException ex )
        {
            // todo: This same string is in FieldSearchLuceneImpl.shutdown();
            String error = String.format( "Failed to shutdown the index properly. This means that the index could still be locked and/or in an unstable state. Please do a manual check." );
            log.error( error, ex );
            throw new ModuleShutdownException( error, role, ex );
        }
    }


    /**
     * Helper method for the initialization phase of this class. Given a String
     * describing the requested lucene Directory type, this method will contruct
     * an instance of the requested Directory or throw a ModuleInitializationException if
     * no Directories match the requested type.
     *
     * @param directoryName the name of the Directory to be instantiated
     * @return an instance of the {@code Directory} identified by {@code directoryName}, if any
     * @throws ModuleInitializationException if the {@code directoryName} does not match any {@code Directory} class
     */
    private Directory initializeDirectoryString( final String directoryName ) throws ModuleInitializationException, IOException
    {
        Directory directory = null;
        if( directoryName.equals( "RAMDirectory" ) )
        {
            directory = new RAMDirectory();
            log.debug( "Returning a {} instance", directoryName );
        }
        else
        { //with anything other than a RAMDirectory, we'll need a location
            String idxLoc = getParameter( "indexLocation" );
            if( null == idxLoc || idxLoc.equals( "" ) )
            {
                throw new ModuleInitializationException( "parameter indexLocation must be specified.", getRole() );
            }
            File location = new File( idxLoc );

            try
            {
                if( directoryName.equals( "NIOFSDirectory" ) )
                {
                    directory = new NIOFSDirectory( location );
                }
                else if( directoryName.equals( "SimpleFSDirectory" ) )
                {
                    directory = new SimpleFSDirectory( location );
                }
                else
                {
                    throw new ModuleInitializationException( String.format( "parameter %s is not a valid value", directoryName ), getRole() );
                }
                log.debug( "Returning a {} instance, at {}", directoryName, location.getAbsolutePath() );
            } 
            catch( IOException ex )
            {
                String error = String.format( "FATAL: Could not initialize instance of %s", directoryName );
                log.error( error, ex );
                throw new ModuleInitializationException( error, getRole(), ex );
            }

            if( directory.fileExists( "segments.gen" ) )
            {
                log.info( "Opening already existing index at {}", location );
            }
        }

        return directory;
    }

}
