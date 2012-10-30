package dk.dbc.opensearch.fedora.search;


import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadLog implements WriteAheadLogMBean
{
    private final static String LOG_OPEN_POSTFIX = ".log";
    private final static String LOG_COMITTING_POSTFIX = ".committing";
    private final static String LOG_NAME = "writeaheadlog";

    private final static String PID_FIELD_NAME = "pid";

    private static final Logger log = LoggerFactory.getLogger( WriteAheadLog.class );

    private final IndexWriter writer;

    private final File storageDirectory;

    private final int commitSize;

    private final boolean keepFileOpen;

    private File currentFile;

    private volatile int updatedDocuments = 0;

    private volatile int uncomittedDocuments = 0;

    private volatile int commits = 0;

    RandomAccessFile fileAccess = null;

    public WriteAheadLog( IndexWriter writer, File storageDirectory, int commitSize, boolean keepFileOpen ) throws IOException
    {
        log.info( "Creating Write Ahead Log in directory {}, with commit size: {} and keepFileOpen: {}",
                new Object[] { storageDirectory.getAbsolutePath(), commitSize, keepFileOpen} );

        checkParameterForNullLogAndThrow( "writer", writer );
        checkParameterForNullLogAndThrow( "storageDirectory", storageDirectory );

        if ( commitSize < 1 )
        {
            String error = String.format( "Parameter commitSize must be positive: %d", commitSize );
            log.error( error );
            throw new IllegalArgumentException( error );
        }

        this.writer = writer;
        this.storageDirectory = storageDirectory;
        this.commitSize = commitSize;
        this.keepFileOpen = keepFileOpen;

        // Register the JMX monitoring bean
        try
        {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();

            server.registerMBean( this, new ObjectName( "FieldSearchLucene:name=WriteAheadLog" ) );
        }
        catch( JMException ex )
        {
            log.error( "Unable to register monitor. JMX Monitoring will be unavailable", ex);
        }

    }

    private void checkParameterForNullLogAndThrow( String name, Object value)
    {
        if ( value == null )
        {
            String error = String.format( "Parameter %s may not be null", name );
            log.error( error );
            throw new NullPointerException( error );
        }
    }

    public void initialize() throws IOException
    {
        log.debug( "Initializing Write Ahead Log" );
        if ( storageDirectory.listFiles().length > 0 )
        {
            recoverUncomittedFiles();
        }
        currentFile = createNewFile();
    }


    void recoverUncomittedFiles( ) throws IOException
    {
        File comittingFile = new File( storageDirectory, LOG_NAME + LOG_COMITTING_POSTFIX );
        File logFile = new File( storageDirectory, LOG_NAME + LOG_OPEN_POSTFIX );

        if ( comittingFile.exists() )
        {
            recoverUncomittedFile( comittingFile, writer );
            comittingFile.delete();
        }
        if ( logFile.exists() )
        {
            recoverUncomittedFile( logFile, writer );
            logFile.delete();
        }
    }


    static Term getPidTerm( String pid )
    {
        Term pidTerm = new Term( PID_FIELD_NAME, pid );
        return pidTerm;
    }


    static int recoverUncomittedFile( File walFile, IndexWriter writer ) throws IOException
    {
        log.warn( "Recovering file {}", walFile );

        RandomAccessFile fileToRecover = new RandomAccessFile( walFile, "r" );

        int count = 0;

        try
        {
            while ( true )
            {
                DocumentData docData = WriteAheadLog.readDocumentData( fileToRecover );
                count++;
                Term pidTerm = getPidTerm( docData.pid );

                if ( docData.docOrNull == null )
                {
                    log.info( "Recovering deleted document for {}", docData.pid );
                    writer.deleteDocuments( pidTerm );
                }
                else
                {
                    log.info( "Recovering updated document for {}", docData.pid );
                    writer.updateDocument( pidTerm, docData.docOrNull );
                }
            }
        }
        catch( IOException ex )
        {
            log.debug( "No more updates found in log file {}", walFile);
        }
        finally
        {
            fileToRecover.close();
            writer.commit();
        }
        log.info( "Recovered {} changes from log file {}", count, walFile);
        return count;
    }


    public void deleteDocument( String pid) throws IOException
    {
        updateOrDeleteDocument( pid, null );
    }

    public void updateDocument( String pid, Document doc ) throws IOException
    {
        updateOrDeleteDocument( pid, doc );
    }

    private void updateOrDeleteDocument( String pid, Document doc ) throws IOException
    {
        File commitFile = null;
        synchronized ( this )
        {
            Term pidTerm = getPidTerm( pid );
            if ( doc == null )
            {
                log.debug( "Deleting document with PID {}", pid );
                this.writer.deleteDocuments( pidTerm);
            }
            else
            {
                log.debug( "Updating document with PID {}", pid );
                this.writer.updateDocument( pidTerm, doc );
            }

            writeDocumentToFile( pid, doc );
            updatedDocuments++;
            uncomittedDocuments++;

            if ( updatedDocuments % commitSize == 0)
            {
                if ( fileAccess != null )
                {
                    fileAccess.close();
                    fileAccess = null;
                }
                commitFile = new File( storageDirectory, LOG_NAME + LOG_COMITTING_POSTFIX );
                currentFile.renameTo( commitFile );
                currentFile = createNewFile();
                log.info( "Comitting {} documents.", uncomittedDocuments );
                // commit
                writer.commit();
                commits++;
                uncomittedDocuments = 0;
            }
        }
        if ( commitFile != null )
        {
            // Erase old file
            commitFile.delete();
        }
    }

    public synchronized void shutdown() throws IOException
    {
        log.info( "Shutting down Write Ahead Log");
        writer.commit();
        commits ++;

        log.info( "Added {} documents. Comitted {} times", updatedDocuments, commits );
        if ( currentFile != null )
        {
            if ( fileAccess != null )
            {
                fileAccess.close();
            }
            fileAccess = null;
            currentFile.delete();
            currentFile = null;
        }
    }

    private File createNewFile() throws IOException
    {
        return new File( storageDirectory, LOG_NAME + LOG_OPEN_POSTFIX );
    }

    private RandomAccessFile getFileAccess() throws IOException
    {
        if ( fileAccess == null )
        {
            fileAccess = new RandomAccessFile( currentFile, "rwd" );
            fileAccess.seek( currentFile.length() );
        }
        return fileAccess;
    }

    private void releaseFileAccess() throws IOException
    {
        if ( ! keepFileOpen )
        {
            fileAccess.close();
            fileAccess = null;
        }
    }

    private void writeDocumentToFile( String pid, Document document ) throws IOException
    {
        try
        {
            writeDocumentData( getFileAccess(), pid, document);
        }
        finally
        {
            releaseFileAccess();
        }
    }

    @Override
    public int getCommitSize()
    {
        return commitSize;
    }


    @Override
    public int getCommits()
    {
        return commits;
    }


    @Override
    public int getUncomittedDocuments()
    {
        return uncomittedDocuments;
    }


    @Override
    public int getUpdatedDocuments()
    {
        return updatedDocuments;
    }

    static void writeDocumentData( RandomAccessFile raf, String pid, Document docOrNull ) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream( bos );
        out.writeObject( pid );
        out.writeObject( docOrNull );
        byte[] recordBytes= bos.toByteArray();

        ByteBuffer rbb = ByteBuffer.wrap(recordBytes);

        raf.getChannel().write( rbb );
    }

    static DocumentData readDocumentData( RandomAccessFile raf ) throws IOException
    {
        try
        {
            ObjectInputStream istr = new ObjectInputStream( Channels.newInputStream( raf.getChannel() ) );

            String pid = ( String ) istr.readObject();
            Document doc = ( Document ) istr.readObject();
            return new DocumentData( pid, doc );
        }
        catch ( ClassNotFoundException ex )
        {
            throw new IOException( ex );
        }
        // Do not shutdown stream or underlying file will be closed
    }


    static class DocumentData
    {
        final String pid;
        final Document docOrNull;

        public DocumentData( String pid, Document docOrNull )
        {
            this.pid = pid;
            this.docOrNull = docOrNull;
        }

        public String toString()
        {
            return "PID '" + pid + "' Document: " + docOrNull;
        }
    }

}