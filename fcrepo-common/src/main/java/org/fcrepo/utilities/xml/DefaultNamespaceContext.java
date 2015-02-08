package org.fcrepo.utilities.xml;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Default implementation of a {@link NamespaceContext}
 */
public class DefaultNamespaceContext implements NamespaceContext {

    /* URI -> prefixes map */
    private Map<String, Collection<String>> namespace;

    /* prefix -> URI map */
    private Map<String, String> prefixes;

    private String defaultNamespaceURI;


    /**
     * Constructs a NamespaceContext with no default namespace.
     * <p/>
     * Beware that the default namespace can only be set during
     * construction.
     */
    public DefaultNamespaceContext() {
        this(null);
    }

    /**
     * Constructs a NamespaceContext with a default namespace.
     * <p/>
     *
     * @param defaultNamespaceURI , the default namespace for this context.
     */
    public DefaultNamespaceContext(String defaultNamespaceURI) {
        namespace = new HashMap<String, Collection<String>>();
        prefixes = new HashMap<String, String>();
        this.defaultNamespaceURI = defaultNamespaceURI;

        namespace.put(XMLConstants.XMLNS_ATTRIBUTE_NS_URI,
                      Arrays.asList(XMLConstants.XMLNS_ATTRIBUTE));

        prefixes.put(XMLConstants.XMLNS_ATTRIBUTE,
                     XMLConstants.XMLNS_ATTRIBUTE_NS_URI);

        namespace.put(XMLConstants.XML_NS_URI,
                      Arrays.asList(XMLConstants.XML_NS_PREFIX));

        prefixes.put(XMLConstants.XML_NS_PREFIX,
                     XMLConstants.XML_NS_URI);
    }

    public DefaultNamespaceContext(String defaultNamespaceURI,
                                   String... nsContext) {
        this(defaultNamespaceURI);

        if (nsContext.length % 2 != 0) {
            throw new IllegalArgumentException("Odd number of arguments. " +
                                               "Prefix/URI pairs must match up");
        }

        for (int i = 0; i < nsContext.length; i += 2) {
            setNameSpace(nsContext[i + 1], nsContext[i]);
        }
    }

    /**
     * Set or add a namespace to the context and associated it with a prefix.
     * <p/>
     * A given prefix can only be associated with one namespace in the context.
     * A namespace can have multiple prefixes.
     * <p/>
     * The prefixes: {@code xml}, and {@code xmlns} are reserved and
     * predefined in any context.
     *
     * @param namespaceURL the namespace uri
     * @param prefix       the prefix to register with the uri
     * @throws IllegalArgumentException thrown when trying to assign a
     *                                  namespace to a reserved prefix
     */
    public void setNameSpace(String namespaceURL, String prefix)
            throws IllegalArgumentException {
        Collection<String> s = namespace.get(namespaceURL);

        if (s == null) {
            s = new HashSet<String>();
        }

        s.add(prefix);
        namespace.put(namespaceURL, s);
        this.prefixes.put(prefix, namespaceURL);
    }

    @Override
    public String getNamespaceURI(String prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException();
        }

        if (prefix.equals(XMLConstants.DEFAULT_NS_PREFIX)
            && this.defaultNamespaceURI != null) {
            return this.defaultNamespaceURI;
        }


        if (!this.prefixes.containsKey(prefix)) {
            return XMLConstants.NULL_NS_URI;
        }

        return this.prefixes.get(prefix);

    }

    @Override
    public String getPrefix(String namespaceURI) {
        if (namespaceURI == null) {
            throw new IllegalArgumentException();
        }

        if (namespaceURI.equals(defaultNamespaceURI)) {
            return XMLConstants.DEFAULT_NS_PREFIX;
        }

        Collection<String> s = namespace.get(namespaceURI);
        if (s != null && !s.isEmpty()) {
            return s.iterator().next();
        } else {
            return null;
        }
    }

    @Override
    public Iterator<String> getPrefixes(String namespaceURI) {

        if (namespaceURI == null) {
            throw new IllegalArgumentException();
        }

        if (namespaceURI.equals(XMLConstants.XML_NS_URI)) {
            return new OneResultIterator<String>(XMLConstants.XML_NS_PREFIX);
        }
        if (namespaceURI.equals(XMLConstants.XMLNS_ATTRIBUTE_NS_URI)) {
            return new OneResultIterator<String>(XMLConstants.XMLNS_ATTRIBUTE);
        }
        if (namespaceURI.equals(defaultNamespaceURI)) {
            return new OneResultIterator<String>(XMLConstants.DEFAULT_NS_PREFIX);
        }

        Collection<String> s = namespace.get(namespaceURI);
        if (s != null && !namespace.isEmpty()) {
            return new NonModifiableIterator<String>(s.iterator());
        } else {
            return NO_STRINGS;
        }
    }
    
    private static final EmptyIterator<String> NO_STRINGS =
            new EmptyIterator<String>();
    
    static class EmptyIterator<T> implements Iterator<T> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            return null;
        }

        @Override
        public void remove() {
            
        }
        
    }
    
    static class OneResultIterator<T> implements Iterator<T> {
        private T result;
        public OneResultIterator(T oneResult) {
            result = oneResult;
        }
        
        @Override
        public boolean hasNext() {
            return result != null;
        }

        @Override
        public T next() {
            T next = result;
            result = null;
            return next;
        }

        @Override
        public void remove() {
            
        }
        
    }

    /**
     * This Iterator wraps any Iterator and makes the remove() method
     * unsupported.<br>
     *
     * @author Hans Lund, State and University Library, Aarhus Denamrk.
     * @version $Id: DefaultNamespaceContext.java,v 1.5 2007/10/04 13:28:21 te Exp $
     * @param <T>
     * @see javax.xml.namespace.NamespaceContext#getPrefixes(String)
     */
    static class NonModifiableIterator<T> implements Iterator<T> {

        Iterator<T> wrapped;

        NonModifiableIterator(Iterator<T> iter) {
            wrapped = iter;
        }

        /**
         * Returns <tt>true</tt> if the iteration has more elements. (In other
         * words, returns <tt>true</tt> if <tt>next</tt> would return an element
         * rather than throwing an exception.)
         *
         * @return <tt>true</tt> if the iterator has more elements.
         */
        public boolean hasNext() {
            return wrapped.hasNext();
        }

        /**
         * Returns the next element in the iteration.  Calling this method
         * repeatedly until the {@link #hasNext()} method returns false will
         * return each element in the underlying collection exactly once.
         *
         * @return the next element in the iteration.
         * @throws java.util.NoSuchElementException
         *          iteration has no more elements.
         */
        public T next() {
            return wrapped.next();
        }

        /**
         * This method is not supported on this Iterator.
         * <p/>
         * Allways throws UnsupportedOperationException {@link javax.xml.namespace.NamespaceContext#getPrefixes(String)}
         *
         * @throws UnsupportedOperationException if the <tt>remove</tt>
         *                                       operation is not supported by this Iterator.
         */
        public void remove() {
            throw new UnsupportedOperationException("Conform to XML API please");
        }
    }


}
