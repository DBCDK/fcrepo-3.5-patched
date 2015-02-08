/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also 
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.server.storage.types;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Literal;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.openrdf.rio.RDFHandlerException;
import org.trippi.io.transform.Transformer;

import org.fcrepo.common.Constants;
import org.fcrepo.common.rdf.SimpleLiteral;
import org.fcrepo.common.rdf.SimpleTriple;
import org.fcrepo.common.rdf.SimpleURIReference;

/**
 * A data structure for holding relationships.
 * 
 * @author Robert Haschart
 */
public class RelationshipTuple
        implements Constants {
    
    public static final Transformer<RelationshipTuple> TRANSFORMER =
            new TripleTransformer();

    public final String subject;

    public final String predicate;

    public final String object;

    public final boolean isLiteral;

    public final URI datatype;
    
    public final String language;

    public RelationshipTuple(String subject,
                             String predicate,
                             String object,
                             boolean isLiteral,
                             URI datatype) {
        this(subject, predicate, object, isLiteral, datatype, null);
    }
    
    public RelationshipTuple(String subject,
                             String predicate,
                             String object,
                             boolean isLiteral,
                             URI datatype,
                             String language) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.isLiteral = isLiteral;
        this.datatype = datatype;
        this.language = language;
    }

    // TODO: Consider getting rid of this method
    public String getObjectPID() {
        if (object != null && !isLiteral && object.startsWith("info:fedora/")) {
            String PID = object.substring(12);
            return PID;
        }
        return null;
    }

    // TODO: Consider getting rid of this method
    public String getRelationship() {
        String prefixRel = RELS_EXT.uri;
        if (predicate != null && predicate.startsWith(prefixRel)) {
            String rel = "rel:" + predicate.substring(prefixRel.length());
            return rel;
        }
        String prefixModel = MODEL.uri;
        if (predicate != null && predicate.startsWith(prefixModel)) {
            String rel =
                    "fedora-model:" + predicate.substring(prefixModel.length());
            return rel;
        }
        return predicate;
    }

    @Override
    public String toString() {
        String retVal =
                "Sub: " + subject + "  Pred: " + predicate + "  Obj: ["
                        + object + ", " + isLiteral + ", " + datatype + "]";
        return retVal;
    }
    
    @Override
    public int hashCode() {
        return hc(subject)
                + hc(predicate)
                + hc(object)
                + hc(datatype);
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof RelationshipTuple) {
            RelationshipTuple t = (RelationshipTuple) o;
            return eq(subject, t.subject)
                    && eq(predicate, t.predicate)
                    && eq(object, t.object)
                    && eq(datatype, t.datatype)
                    && isLiteral == t.isLiteral;
        } else {
            return false;
        }
    }
    
    public Triple toTriple(Map<String, String> namespaces) throws URISyntaxException {
        return new SimpleTriple(
                                new SimpleURIReference(new URI(subject)),
                                RelationshipTuple.makePredicateResourceFromRel(predicate,
                                                                               namespaces),
                                RelationshipTuple.makeObjectFromURIandLiteral(object,
                                                                              isLiteral,
                                                                              datatype,
                                                                              language));
    }
    
    public static URI makePredicateFromRel(String relationship, Map<String,String> map)
            throws URISyntaxException {
        String predicate = relationship;
        if (map != null) {
            Set<String> keys = map.keySet();
            Iterator<String> iter = keys.iterator();
            while (iter.hasNext()) {
                String key = iter.next();
                if (predicate.startsWith(key)) {
                    int keyLen = key.length();
                    if (predicate.length() != keyLen
                            && predicate.charAt(keyLen) == ':') {
                    predicate =
                            map.get(key).concat(predicate.substring(keyLen + 1));
                    }
                }
            }
        }
        URI retVal = null;
        retVal = new URI(predicate);
        return retVal;
    }

    public static PredicateNode makePredicateResourceFromRel(String predicate,
                                                             Map<String, String> map)
            throws URISyntaxException {
        URI predURI = RelationshipTuple.makePredicateFromRel(predicate, map);
        PredicateNode node = new SimpleURIReference(predURI);
        return node;
    }

    public static ObjectNode makeObjectFromURIandLiteral(String objURI,
                                                         boolean isLiteral,
                                                         URI literalType,
                                                         String language)
            throws URISyntaxException {
        ObjectNode obj = null;
        if (isLiteral) {
            if (literalType != null) {
                obj = new SimpleLiteral(objURI, literalType);
            } else if (language != null){
                obj = new SimpleLiteral(objURI, language);
            } else {
                obj = new SimpleLiteral(objURI);
            }
        } else {
            obj = new SimpleURIReference(new URI(objURI));
        }
        return obj;
    }

    public static RelationshipTuple fromTriple(Triple triple) {
        return fromNodes(triple.getSubject(),
                triple.getPredicate(), triple.getObject());
    }
    
    public static RelationshipTuple fromNodes(
            SubjectNode sNode, PredicateNode pNode, ObjectNode oNode) {
        String subject = sNode.toString();
        String predicate = pNode.toString();
        if (oNode instanceof Literal) {
            return getLiteral(subject, predicate, (Literal)oNode);
        } else {
            return new RelationshipTuple(subject, predicate, oNode.toString(), false, null, null);
        }
    }
    
    public static RelationshipTuple fromNodes(org.openrdf.model.Resource s, org.openrdf.model.URI p,
            org.openrdf.model.Value o, GraphElementFactory u) {
        String subject = s.stringValue();
        String predicate = p.stringValue();
        // the rdf streams parsed by Fedora will only contain URIs and Literals, no BNodes
        if (o instanceof org.openrdf.model.Literal) {
            return getLiteral(subject, predicate, (org.openrdf.model.Literal)o);
        } else {
            return new RelationshipTuple(subject, predicate, o.stringValue(), false, null, null);
        }
    }
    
    private static RelationshipTuple getLiteral(String subject, String predicate, Literal literal) {
        return new RelationshipTuple(subject, predicate, literal.getLexicalForm(), true, literal.getDatatypeURI(), literal.getLanguage());
    }

    private static RelationshipTuple getLiteral(String subject, String predicate, org.openrdf.model.Literal literal) {
        if (literal.getDatatype() != null) {
            return new RelationshipTuple(subject, predicate, literal.stringValue(), true,
                URI.create(literal.getDatatype().stringValue()), null);
        } else {
            return new RelationshipTuple(subject, predicate, literal.stringValue(), true,
                    null, literal.getLanguage());
        }
    }
   
    // test for equality, accounting for null values
    private static boolean eq(Object a, Object b) {
        if (a == null) {
            return b == null;
        } else {
            return b != null && a.equals(b);
        }
    }
    
    // return the hashCode or 0 if null
    private static int hc(Object o) {
        if (o == null) {
            return 0;
        } else {
            return o.hashCode();
        }
    }
    
    public static class TripleTransformer implements Transformer<RelationshipTuple> {

        @Override
        public RelationshipTuple transform(Triple input) {
            return RelationshipTuple.fromTriple(input);
        }

        @Override
        public RelationshipTuple transform(
                SubjectNode sNode, PredicateNode pNode, ObjectNode oNode) {
            return RelationshipTuple.fromNodes(sNode, pNode, oNode);
        }

        @Override
        public RelationshipTuple transform(org.openrdf.model.Statement s,
                GraphElementFactory u) throws RDFHandlerException {
            return RelationshipTuple.fromNodes(s.getSubject(), s.getPredicate(), s.getObject(), u);
        }
        
    }
}
