/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import java.util.HashMap;

import org.fcrepo.server.Context;
import org.fcrepo.server.errors.ObjectIntegrityException;
import org.fcrepo.server.errors.ObjectNotFoundException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.errors.StorageDeviceException;
import org.fcrepo.server.errors.StreamIOException;
import org.fcrepo.server.errors.UnsupportedTranslationException;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A RepositoryReader that uses a directory of serialized objects as its working
 * repository.
 * <p>
 * All files in the directory must be digital object serializations, and none
 * may have the same PID. This is verified upon construction.
 * </p>
 * <p>
 * Note: This implementation does not recognize when files are added to the
 * directory. What is in the directory at construction-time is what is assumed
 * to be the extent of the repository for the life of the object.
 * </p>
 *
 * @author Chris Wilper
 */
public class DirectoryBasedRepositoryReader
        implements RepositoryReader {

    private static final Logger logger =
            LoggerFactory.getLogger(DirectoryBasedRepositoryReader.class);

    private static final String[] EMPTY_STRING_ARRAY =
            new String[0];

    private final DOTranslator m_translator;

    private final String m_exportFormat;

    private final String m_storageFormat;

    private final String m_encoding;

    private final HashMap<String, File> m_files = new HashMap<String, File>();

    /**
     * Initializes the RepositoryReader by looking at all files in the provided
     * directory and ensuring that they're all serialized digital objects and
     * that there are no PID conflicts.
     *
     * @param directory
     *        the directory where this repository is based.
     * @param translator
     *        the serialization/deserialization engine for objects.
     * @param exportFormat
     *        the format to use for export requests.
     * @param storageFormat
     *        the format of the objects on disk.
     * @param encoding
     *        The character encoding used across all formats.
     */
    public DirectoryBasedRepositoryReader(File directory,
                                          DOTranslator translator,
                                          String exportFormat,
                                          String storageFormat,
                                          String encoding)
            throws StorageDeviceException, ObjectIntegrityException,
            StreamIOException, UnsupportedTranslationException, ServerException {
        m_translator = translator;
        m_exportFormat = exportFormat;
        m_storageFormat = storageFormat;
        m_encoding = encoding;
        File[] files = directory.listFiles();
        if (!directory.isDirectory()) {
            throw new StorageDeviceException("Repository storage directory not found.");
        }
        try {
            for (File thisFile : files) {
                try {
                    FileInputStream in = new FileInputStream(thisFile);
                    SimpleDOReader reader =
                            new SimpleDOReader(null,
                                               this,
                                               m_translator,
                                               m_exportFormat,
                                               m_storageFormat,
                                               m_encoding,
                                               in);
                    String pid = reader.GetObjectPID();
                    if (reader.GetObjectPID().length() == 0) {
                        logger.warn("File " + thisFile + " has no pid...skipping");
                    } else {
                        m_files.put(pid, thisFile);
                    }
                } catch (NullPointerException npe) {
                    logger.warn("Error in " + thisFile.getName() + "...skipping");
                }
            }
        } catch (FileNotFoundException fnfe) {
            // impossible
        }
    }

    private InputStream getStoredObjectInputStream(String pid)
            throws ObjectNotFoundException {
        try {
            return new FileInputStream((File) m_files.get(pid));
        } catch (Throwable th) {
            throw new ObjectNotFoundException("The object, " + pid + " was "
                    + "not found in the repository.");
        }
    }

    public DOReader getReader(boolean UseCachedObject,
                              Context context,
                              String pid) throws ObjectIntegrityException,
            ObjectNotFoundException, StreamIOException,
            UnsupportedTranslationException, ServerException {
        return new SimpleDOReader(null,
                                  this,
                                  m_translator,
                                  m_exportFormat,
                                  m_storageFormat,
                                  m_encoding,
                                  getStoredObjectInputStream(pid));
    }

    public ServiceDeploymentReader getServiceDeploymentReader(boolean UseCachedObject,
                                      Context context,
                                      String pid)
            throws ObjectIntegrityException, ObjectNotFoundException,
            StreamIOException, UnsupportedTranslationException, ServerException {
        return new SimpleServiceDeploymentReader(null,
                                     this,
                                     m_translator,
                                     m_exportFormat,
                                     m_storageFormat,
                                     m_encoding,
                                     getStoredObjectInputStream(pid));
    }

    public ServiceDefinitionReader getServiceDefinitionReader(boolean UseCachedObject,
                                    Context context,
                                    String pid)
            throws ObjectIntegrityException, ObjectNotFoundException,
            StreamIOException, UnsupportedTranslationException, ServerException {
        return new SimpleServiceDefinitionReader(null,
                                    this,
                                    m_translator,
                                    m_exportFormat,
                                    m_storageFormat,
                                    m_encoding,
                                    getStoredObjectInputStream(pid));
    }

    public String[] listObjectPIDs(Context context) {
        return m_files.keySet().toArray(EMPTY_STRING_ARRAY);
    }

}
