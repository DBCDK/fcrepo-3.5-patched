package dk.dbc.opensearch.fedora.search;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;
import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadLog extends WriteAheadLogStats
{
    private final static String LOG_OPEN_POSTFIX = ".log";
    private final static String LOG_COMITTING_POSTFIX = ".committing";
    private final static String LOG_NAME = "writeaheadlog";

    private final static String PID_FIELD_NAME = "pid";

    private static final Logger log = LoggerFactory.getLogger( WriteAheadLog.class );

    private final IndexWriter writer;

    private final File storageDirectory;

    private final boolean keepFileOpen;

    private boolean isOpen = false;

    private File currentFile;

    RandomAccessFile fileAccess = null;
    private ObjectName jmxObjectName;
    private final static Kryo serializer = new Kryo();

    static
    {
        serializer.addDefaultSerializer( LongField.class, new Serializer<LongField>()
        {
            @Override
            public void write( Kryo kryo, Output output, LongField object )
            {
                output.writeString( object.name() );
                output.writeLong( object.numericValue().longValue() );
                output.writeBoolean( object.fieldType() == LongField.TYPE_STORED );
            }
            @Override
            public LongField read( Kryo kryo, Input input, Class<LongField> type )
            {
                String name = input.readString();
                long value = input.readLong();
                boolean stored = input.readBoolean();
                return new LongField( name, value, stored ? Field.Store.YES : Field.Store.NO );
            }
        });
        serializer.addDefaultSerializer( StringField.class, new Serializer<StringField>()
        {
            @Override
            public void write( Kryo kryo, Output output, StringField object )
            {
                output.writeString( object.name() );
                output.writeString( object.stringValue() );
                output.writeBoolean( object.fieldType() == StringField.TYPE_STORED );
            }
            @Override
            public StringField read( Kryo kryo, Input input, Class<StringField> type )
            {
                String name = input.readString();
                String value = input.readString();
                boolean stored = input.readBoolean();
                return new StringField( name, value, stored ? Field.Store.YES : Field.Store.NO );
            }
        });
        serializer.addDefaultSerializer( TextField.class, new Serializer<TextField>()
        {
            @Override
            public void write( Kryo kryo, Output output, TextField object )
            {
                output.writeString( object.name() );
                output.writeString( object.stringValue() );
                output.writeBoolean( object.fieldType() == TextField.TYPE_STORED );
            }
            @Override
            public TextField read( Kryo kryo, Input input, Class<TextField> type )
            {
                String name = input.readString();
                String value = input.readString();
                boolean stored = input.readBoolean();

                return new TextField( name, value, stored ? Field.Store.YES : Field.Store.NO );
            }
        });
    }

    public WriteAheadLog( IndexWriter writer, File storageDirectory, int commitSize, boolean keepFileOpen ) throws IOException
    {
        super( commitSize );
        log.info( "Creating Write Ahead Log in directory {}, with commit size: {} and keepFileOpen: {}",
                new Object[] { storageDirectory.getAbsolutePath(), commitSize, keepFileOpen} );

        checkParameterForNullLogAndThrow( "writer", writer );
        checkParameterForNullLogAndThrow( "storageDirectory", storageDirectory );


        this.writer = writer;
        this.storageDirectory = storageDirectory;
        this.keepFileOpen = keepFileOpen;

        // Register the JMX monitoring bean
        try
        {
            jmxObjectName = new ObjectName( "FieldSearchLucene:name=WriteAheadLog" );
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.registerMBean( this, jmxObjectName);
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

    public int initialize() throws IOException
    {
        log.debug( "Initializing Write Ahead Log" );
        int count = 0;
        if ( storageDirectory.listFiles().length > 0 )
        {
            count = recoverUncomittedFiles();
        }
        currentFile = createNewFile();
        isOpen = true;
        return count;
    }


    int recoverUncomittedFiles( ) throws IOException
    {
        File comittingFile = new File( storageDirectory, LOG_NAME + LOG_COMITTING_POSTFIX );
        File logFile = new File( storageDirectory, LOG_NAME + LOG_OPEN_POSTFIX );

        int count = 0;
        if ( comittingFile.exists() )
        {
            count += recoverUncomittedFile( comittingFile, writer );
            comittingFile.delete();
        }
        if ( logFile.exists() )
        {
            count += recoverUncomittedFile( logFile, writer );
            logFile.delete();
        }
        return count;
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
        catch( KryoException ex )
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
        tryUpdateOrDeleteDocument( pid, null );
    }

    public void updateDocument( String pid, Document doc ) throws IOException
    {
        tryUpdateOrDeleteDocument( pid, doc );
    }

    private void tryUpdateOrDeleteDocument( String pid, Document docOrNull ) throws IOException
    {
        try
        {
            updateOrDeleteDocument( pid, docOrNull );
        }
        catch ( IOException ex )
        {
            fileAccess = null;
            throw ex;
        }
    }

    private void updateOrDeleteDocument( String pid, Document docOrNull ) throws IOException
    {
        long updateStart = System.nanoTime();

        File commitFile = null;
        numberOfUncomittedDocuments.incrementAndGet();
        int updates = numberOfUpdatedDocuments.incrementAndGet();
        byte[] recordBytes = createDocumentData( pid, docOrNull );

        synchronized ( this )
        {
            if ( !isOpen )
            {
                throw new IOException( "Write Ahead Log is not open");
            }

            writeDocumentToFile( recordBytes );

            if ( updates % commitSize == 0)
            {
                // Time to commit. Close existing log file, rename and create a new log file
                if ( fileAccess != null )
                {
                    try
                    {
                        fileAccess.close();
                    }
                    finally
                    {
                        fileAccess = null;
                    }
                }
                commitFile = new File( storageDirectory, LOG_NAME + LOG_COMITTING_POSTFIX );
                currentFile.renameTo( commitFile );
                currentFile = createNewFile();
            }
        }

        updateInWriter( pid, docOrNull );

        if ( commitFile != null )
        {
            commitWriter();
            // Erase old file
            commitFile.delete();
        }
        long updateEnd = System.nanoTime();

        totalUpdateTimeMicroS.addAndGet( (updateEnd - updateStart)/1000 );
    }


    private void commitWriter() throws IOException
    {
        long commitStart = System.nanoTime();
        log.info( "Comitting {} documents.", numberOfUncomittedDocuments );
        numberOfCommits.incrementAndGet();
        numberOfUncomittedDocuments.set( 0 );
        // commit
        writer.commit();
        long commitEnd = System.nanoTime();
        totalCommitToLuceneTimeMicroS.addAndGet( (commitEnd - commitStart)/1000 );
    }


    private void updateInWriter( String pid, Document doc ) throws IOException
    {
        long updateStart = System.nanoTime();
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
        long updateEnd = System.nanoTime();
        totalUpdateInLuceneTimeMicroS.addAndGet( (updateEnd - updateStart)/1000 );
    }

    public synchronized void flush() throws IOException{
        commitWriter();
        if ( fileAccess != null )
        {
            fileAccess.close();
            fileAccess = null;
        }
        currentFile.delete();
        currentFile = createNewFile();  
        
    }
    public synchronized void shutdown() throws IOException
    {
        log.info( "Shutting down Write Ahead Log");
        isOpen = false;

        commitWriter();

        log.info( "Added {} documents. Comitted {} times", numberOfUpdatedDocuments, numberOfCommits );
        if ( currentFile != null )
        {
            if ( fileAccess != null )
            {
                fileAccess.close();
                fileAccess = null;
            }
            currentFile.delete();
            currentFile = null;
        }
        if ( jmxObjectName != null )
        {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            try
            {
                server.unregisterMBean( jmxObjectName );
            }
            catch( JMException ex )
            {
                log.warn( "Failed to deregister jmx bean", ex);
            }
        }
    }

    private File createNewFile()
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

    private void writeDocumentToFile( byte[] recordBytes ) throws IOException
    {
        long writeStart = System.nanoTime();
        try
        {
            writeDocumentData( getFileAccess(), recordBytes );
        }
        finally
        {
            releaseFileAccess();
        }
        long writeEnd = System.nanoTime();
        totalWriteToFileTimeMicroS.addAndGet( (writeEnd - writeStart)/1000 );
    }

//    private static byte[] createDocumentData( String pid, Document docOrNull ) throws IOException
//    {
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        ObjectOutputStream out = new ObjectOutputStream( bos );
//        out.writeObject( pid );
//        out.writeObject( docOrNull );
//        byte[] recordBytes= bos.toByteArray();
//        out.reset();
//        return recordBytes;
//    }
    private static byte[] createDocumentData( String pid, Document docOrNull ) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputChunked output = new OutputChunked( bos );
        synchronized ( serializer )
        {
            serializer.writeObject( output, pid );
            serializer.writeObjectOrNull( output, docOrNull, Document.class );
        }
        output.endChunks();
        output.close();
        byte[] recordBytes= bos.toByteArray();
        return recordBytes;
    }


    static void writeDocumentData( RandomAccessFile raf, String pid, Document docOrNull ) throws IOException
    {
        byte[] recordBytes = createDocumentData( pid, docOrNull );

        writeDocumentData( raf, recordBytes );
    }

    static void writeDocumentData( RandomAccessFile raf, byte[] recordBytes ) throws IOException
    {
        ByteBuffer rbb = ByteBuffer.wrap(recordBytes);

        raf.getChannel().write( rbb );
    }

    static DocumentData readDocumentData( RandomAccessFile raf ) throws KryoException
    {
        InputChunked input = new InputChunked( Channels.newInputStream( raf.getChannel() ) );

        String pid;
        Document doc;
        synchronized ( serializer )
        {
            pid = serializer.readObject( input, String.class );
            doc = serializer.readObjectOrNull( input, Document.class );
        }
        input.nextChunks();
        return new DocumentData( pid, doc );
    }


    static Term getPidTerm( String pid )
    {
        Term pidTerm = new Term( PID_FIELD_NAME, pid );
        return pidTerm;
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