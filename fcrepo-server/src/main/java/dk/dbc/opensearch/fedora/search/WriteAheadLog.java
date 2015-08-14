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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
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

    private volatile boolean isOpen = false;
    
    private final boolean keepFileOpen;

    private final int numConcurrentTLogs;
    private final BlockingDeque<TLogFile> tLogPool;
    private final AtomicInteger tLogEntryId = new AtomicInteger();
            
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
    

    public WriteAheadLog( IndexWriter writer, File storageDirectory, int commitSize, boolean keepFileOpen, int numConcurrentTLogs ) throws IOException
    {
        super( commitSize );
        log.info( "Creating Write Ahead Log in directory {}, with commit size: {} and keepFileOpen: {}",
                new Object[] { storageDirectory.getAbsolutePath(), commitSize, keepFileOpen} );

        checkParameterForNullLogAndThrow( "writer", writer );
        checkParameterForNullLogAndThrow( "storageDirectory", storageDirectory );


        this.writer = writer;
        this.storageDirectory = storageDirectory;
        this.keepFileOpen = keepFileOpen;
        this.numConcurrentTLogs = numConcurrentTLogs;

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
        
        this.tLogPool = new LinkedBlockingDeque<TLogFile>(numConcurrentTLogs);

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
        for (int i = 0; i < numConcurrentTLogs; i++) {
            this.tLogPool.add(new TLogFile(storageDirectory, i, keepFileOpen));
        }
        isOpen = true;
        return count;
    }
    
    int recoverUncomittedFiles( ) throws IOException
    {
        return recoverUncomittedFiles( LOG_COMITTING_POSTFIX ) + recoverUncomittedFiles( LOG_OPEN_POSTFIX );
    }
    
    int recoverUncomittedFiles( String postFix ) throws IOException
    {
        // Prepare all files in storageDirectory for processing
        List<DocumentDataWrapper> files = new ArrayList<DocumentDataWrapper>();
        for (File f : storageDirectory.listFiles()) {
            if(f.getName().endsWith(postFix)){
                DocumentDataWrapper w = new DocumentDataWrapper(new RandomAccessFile( f, "r" ));
                try {
                    w.doc = WriteAheadLog.readDocumentData(w.raf);
                    files.add(w);
                } catch (KryoException ex) {
                    log.debug("No entries in log file {}", w.raf);
                }
            }
        }
        
        // In each loop step, the "top" unprocessed document from each file is considered.
        // The document with the smallest logEntryId will be processed.
        int count = 0;
        while( true )
        {
            DocumentDataWrapper toBeWritten = null;
            for (DocumentDataWrapper file : files) {
                if(toBeWritten == null || file.doc.logEntryId < toBeWritten.doc.logEntryId){
                    toBeWritten = file;
                }
            }

            if(toBeWritten != null){
                count++;
                Term pidTerm = getPidTerm( toBeWritten.doc.pid );
                if ( toBeWritten.doc.docOrNull == null )
                {
                    log.info( "Recovering deleted document for {}", toBeWritten.doc );
                    writer.deleteDocuments( pidTerm );
                }
                else
                {
                    log.info( "Recovering updated document for {}", toBeWritten.doc.pid );
                    writer.updateDocument( pidTerm, toBeWritten.doc.docOrNull );
                }
                try{ 
                    toBeWritten.doc = WriteAheadLog.readDocumentData(toBeWritten.raf);
                }catch( KryoException ex ){
                    log.info( "No more updates found in log file {}", toBeWritten.raf);
                    files.remove(toBeWritten);
                }
            }else{
                log.info("All files read");
                break;
            }
        }
        writer.commit();
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

    private void updateOrDeleteDocument( String pid, Document docOrNull ) throws IOException
    {
        long updateStart = System.nanoTime();

        numberOfUncomittedDocuments.incrementAndGet();
        int updates = numberOfUpdatedDocuments.incrementAndGet(); 

        if ( !isOpen )
        {
            throw new IOException( "Write Ahead Log is not open");
        }
        
        TLogFile tlog = null;
        try
        {
            tlog = tLogPool.takeLast();
            byte[] recordBytes = createDocumentData( tLogEntryId.getAndIncrement(), pid, docOrNull );
            writeDocumentToFile( recordBytes, tlog.getFileAccess() );
            updateInWriter( pid, docOrNull );
        }
        catch (InterruptedException ex) 
        {
            log.error("Couldn't take tLogFile", ex);
            throw new RuntimeException(ex);
        }        
        finally
        {
            if( tlog != null)
            {
                tlog.releaseFileAccess();
                tLogPool.addLast(tlog);
            }
        }

        
        if (updates % commitSize == 0) 
        {
            // Time to commit.
            commitNow();
        }
        
        long updateEnd = System.nanoTime();

        totalUpdateTimeMicroS.addAndGet( (updateEnd - updateStart)/1000 );
    }

    private void commitNow() throws IOException {
        synchronized(this)
        {
            // Reserve all log files. This will wait for other threads to finish writing.
            List<TLogFile> logFiles = obtainLogFiles();

            // Make commit files
            List<File> commitFiles = new ArrayList<File>();
            for (TLogFile l : logFiles)
            {
                commitFiles.add(l.makeCommitFile());
            }

            // Reset tLogEntryId, and release log files
            tLogEntryId.set(0);
            for (TLogFile l : logFiles)
            {
                tLogPool.addLast(l);
            }

            // Commit and delete commit files
            commitWriter();
            for (File commitFile : commitFiles)
            {
                commitFile.delete();
                log.debug("Deleted commit file {}", commitFile.getAbsolutePath());
            }
        }
    }
    
    private List<TLogFile> obtainLogFiles(){
        long start = System.nanoTime();
        List<TLogFile> result = new ArrayList<TLogFile>();
        while ( result.size() < numConcurrentTLogs ) 
        {
            try {
                result.add(tLogPool.take());
            } catch (InterruptedException ex) {
                
                for (TLogFile l : result) 
                {
                    tLogPool.add(l);
                }
                
                log.error("Couldn't obtain all tLogFiles", ex);
                throw new RuntimeException(ex);
            }
        }
        long end = System.nanoTime();
        numberOfObtainTLogFiles.incrementAndGet();
        totalObtainTLogFilesTimeMicroS.addAndGet((end - start)/1000 );
        return result;
    }
    
    private void deleteLogFiles() throws IOException {
        List<TLogFile> obtainLogFiles = obtainLogFiles();
        for (TLogFile l : obtainLogFiles) {
            l.delete();
        }
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
        isOpen = false;
        commitWriter();
        deleteLogFiles();     
        isOpen = true;
    }
    public synchronized void shutdown() throws IOException
    {
        log.info( "Shutting down Write Ahead Log");
        isOpen = false;

        commitWriter();
        deleteLogFiles();
        
        log.info( "Added {} documents. Comitted {} times", numberOfUpdatedDocuments, numberOfCommits );
        
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

    private void writeDocumentToFile( byte[] recordBytes, RandomAccessFile raf ) throws IOException
    {
        long writeStart = System.nanoTime();
        writeDocumentData( raf, recordBytes );
        long writeEnd = System.nanoTime();
        totalWriteToFileTimeMicroS.addAndGet( (writeEnd - writeStart)/1000 );
    }
    
    @Override
    public int getTLogSize() {
        return tLogPool.size();
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
    private static byte[] createDocumentData( Integer logEntryId, String pid, Document docOrNull ) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputChunked output = new OutputChunked( bos );
        synchronized ( serializer )
        {
            serializer.writeObject( output, logEntryId );
            serializer.writeObject( output, pid );
            serializer.writeObjectOrNull( output, docOrNull, Document.class );
        }
        output.endChunks();
        output.close();
        byte[] recordBytes= bos.toByteArray();
        return recordBytes;
    }

    static void writeDocumentData( RandomAccessFile raf, Integer logEntryId, String pid, Document docOrNull ) throws IOException
    {
        byte[] recordBytes = createDocumentData( logEntryId, pid, docOrNull );

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

        Integer logEntryId;
        String pid;
        Document doc;
        
        synchronized ( serializer )
        {
            logEntryId = serializer.readObject( input, Integer.class );
            pid = serializer.readObject( input, String.class );
            doc = serializer.readObjectOrNull( input, Document.class );
            
        }
        input.nextChunks();
        return new DocumentData( logEntryId, pid, doc );
    }

    static Term getPidTerm( String pid )
    {
        Term pidTerm = new Term( PID_FIELD_NAME, pid );
        return pidTerm;
    }

    static class DocumentDataWrapper
    {
        final RandomAccessFile raf;
        DocumentData doc;
        public DocumentDataWrapper( RandomAccessFile raf ){
            this.raf=raf;
        }
    }
    static class DocumentData
    {
        final Integer logEntryId;
        final String pid;
        final Document docOrNull;

        public DocumentData( Integer logEntryId, String pid, Document docOrNull )
        {
            this.logEntryId = logEntryId;
            this.pid = pid;
            this.docOrNull = docOrNull;   
        }

        public String toString()
        {
            return "PID '" + pid + "' Document: " + docOrNull + " LogEntryID: " + logEntryId;
        }
    }
 
    static class TLogFile
    {
        private final boolean keepFileOpen;
        private final File storageDirectory;
        private final int fileId;
        private File file;
        private RandomAccessFile fileAccess;
        
        public TLogFile( File storageDirectory, int fileId, boolean keepFileOpen )
        {
            this.storageDirectory = storageDirectory;
            this.keepFileOpen = keepFileOpen;
            this.fileId = fileId;
            this.file = initFile();
            log.debug("Created transaction log file {}", file.getAbsolutePath());
        }
        private File initFile(){
            return new File( storageDirectory, fileId+"_"+LOG_NAME + LOG_OPEN_POSTFIX);
        }
        public File getFile(){
            return file;
        }
        public RandomAccessFile getFileAccess() throws IOException 
        {
            if (fileAccess == null) {
                fileAccess = new RandomAccessFile(file, "rwd");
                fileAccess.seek(file.length());
            }
            log.trace("Got file access {}", file.getAbsolutePath());
            return fileAccess;
        }

        public void releaseFileAccess() throws IOException 
        {
            if(fileAccess != null){
                if (!keepFileOpen) {
                    fileAccess.close();
                    fileAccess = null;
                }
            }
            log.trace("Released file access {}", file.getAbsolutePath());
        }
        public void delete() throws IOException{
            if(fileAccess != null)
            {
                fileAccess.close();
                fileAccess = null;
            }
            file.delete();
            log.debug("Deleted tlog-file {}", file.getAbsolutePath());
        }

        private File makeCommitFile() throws IOException {
            if(fileAccess != null)
            {
                fileAccess.close();
                fileAccess = null;
            }
            File commitFile = new File( storageDirectory, fileId+"_"+LOG_NAME + LOG_COMITTING_POSTFIX );
            file.renameTo(commitFile);
            file = initFile();
            log.debug("Created commit file {}", commitFile.getAbsolutePath());
            return commitFile;
        }
    }

}