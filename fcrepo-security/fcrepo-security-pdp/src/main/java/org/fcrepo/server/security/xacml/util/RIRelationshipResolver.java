package org.fcrepo.server.security.xacml.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.fcrepo.common.Constants;
import org.fcrepo.common.MalformedPIDException;
import org.fcrepo.common.PID;
import org.fcrepo.server.Server;
import org.fcrepo.server.resourceIndex.ResourceIndex;
import org.fcrepo.server.security.xacml.MelcoeXacmlException;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.TripleIterator;
import org.trippi.TripleMaker;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public class RIRelationshipResolver
        extends RelationshipResolverBase
        implements RelationshipResolver {

    private static final Logger logger = LoggerFactory.getLogger(RIRelationshipResolver.class);

    private final ResourceIndex RI;

    private boolean spoTriples = false;
    private boolean sparqlTuples = false;
    private boolean itqlTuples = false;

    // Because the FESL beans are created outside the Fedora module context,
    // they can't both share a PDP with the Authorization impl and have a post-init
    // ResourceIndex impl without circular dependencies, so the RI module's config
    // is checked lazily
    protected List<String> tripleLanguages;
    protected List<String> tupleLanguages;

    private static final String SPO = "spo";
    private static final String SPARQL = "sparql";
    private static final String ITQL = "itql";


    public RIRelationshipResolver(Server server, Map<String, String> options) throws MelcoeXacmlException {
        super(options);

        try {
            RI = (ResourceIndex) server.getModule("org.fcrepo.server.resourceIndex.ResourceIndex");
        } catch (Exception e) {
            throw new MelcoeXacmlException("Error getting resource index.", e);
        }
        if (RI == null) {
            throw new MelcoeXacmlException("No Resource Index Module is available to the Server.");
        }

    }

    @Override
    public Map<String, Set<String>> getRelationships(String subject)
            throws MelcoeXacmlException {
        return getRelationships(subject, null);
    }

    @Override
    public Map<String, Set<String>> getRelationships(String subject,
                                                     String relationship)
            throws MelcoeXacmlException {

        Map<String, Set<String>> rels = new HashMap<String, Set<String>>();

        if (subject == null) {
            logger.warn("Subject cannot be null");
            return rels;
        }
        SubjectNode s;
        PredicateNode p;
        try {
                s = TripleMaker.createResource(getFedoraResourceURI(subject));
            if (relationship != null) {
                p = TripleMaker.createResource(relationship);
            } else {
                p = null;
            }

        } catch (TrippiException e) {
            throw new MelcoeXacmlException("Error creating nodes for trippi query " + e.getMessage(), e);
        }

        try {
            TripleIterator it = RI.findTriples(s, p, null, 0);
            while (it.hasNext()) {
                Triple t = it.next();
                String pred = t.getPredicate().toString();

                Set<String> values = rels.get(pred);
                if (values == null) {
                    values = new HashSet<String>();
                }
                values.add(t.getObject().stringValue());
                rels.put(pred, values);
            }

        } catch (TrippiException e) {
            throw new MelcoeXacmlException("Error finding relationships " + e.getMessage(), e);
        }

        return rels;

    }

    protected Map<String, Set<String>> getReverseRelationships(String object)
                      throws MelcoeXacmlException {
        return getReverseRelationships(object, null);
    }

    protected Map<String, Set<String>> getReverseRelationships(String object,
                                                     String relationship)
            throws MelcoeXacmlException {

        Map<String, Set<String>> rels = new HashMap<String, Set<String>>();

        if (object == null) {
            logger.warn("Object cannot be null");
            return rels;
        }
        PredicateNode p;
        ObjectNode o;
        try {
                o = TripleMaker.createResource(getFedoraResourceURI(object));
            if (relationship != null) {
                p = TripleMaker.createResource(relationship);
            } else {
                p = null;
            }

        } catch (TrippiException e) {
            throw new MelcoeXacmlException("Error creating nodes for trippi query " + e.getMessage(), e);
        }

        try {
            TripleIterator it = RI.findTriples(null, p, o, 0);
            while (it.hasNext()) {
                Triple t = it.next();
                String pred = t.getPredicate().toString();

                Set<String> values = rels.get(pred);
                if (values == null) {
                    values = new HashSet<String>();
                }
                values.add(t.getSubject().stringValue());
                rels.put(pred, values);
            }

        } catch (TrippiException e) {
            throw new MelcoeXacmlException("Error finding relationships " + e.getMessage(), e);
        }

        return rels;

    }

    @Override
    public Set<String> getAttributesFromQuery(String query,
                                              String queryLang,
                                              String variable)
            throws MelcoeXacmlException {

        Set<String> res = new HashSet<String>();

        // support tql = itql for the language
        if (queryLang.equals("tql"))
                queryLang = ITQL;

        // tuple queries
        if (queryLang.equals(ITQL) || queryLang.equals(SPARQL)) {
            // check lang supported
            if (!verifyTupleLanguage(queryLang)) {
                logger.warn("RI query language " + queryLang + " is not supported");
                return res;
            }
            // FIXME: should we limit?
            try {
                TupleIterator tuples = RI.findTuples(queryLang, query, 0, true);
                if (tuples != null) {
                    while (tuples.hasNext()) {
                        Map<String, Node> tuple = tuples.next();
                        Node variableValue = tuple.get(variable);
                        if (variableValue != null) {
                            res.add(variableValue.stringValue());
                        } else {
                            logger.error("Attribute query does not contain a result variable " + variable);
                            return res;
                        }
                    }
                }
            } catch (TrippiException e) {
                logger.error("Error running " + queryLang + " query " + query + " : " + e.getMessage(), e);
            }


        } else if (queryLang.equals(SPO)) {
            // triple query
            // check lang supported
            if (!(spoTriples || verifyTripleLanguage(queryLang))) {
                logger.warn("RI query language " + queryLang + " is not supported");
                return res;
            }
            // check variable is s, p, o
            if (variable.length() == 1 && "spo".contains(variable)) {
                try {
                    TripleIterator triples = RI.findTriples(queryLang, query, 0, true);
                    if (triples != null ) {
                        while (triples.hasNext()) {
                            Triple triple = triples.next();
                            switch(variable.charAt(0)) {
                                case 's':
                                    res.add(triple.getSubject().stringValue());
                                    break;
                                case 'p':
                                    res.add(triple.getPredicate().stringValue());
                                    break;
                                case 'o':
                                    res.add(triple.getObject().stringValue());
                                    break;
                            }
                        }
                    }
                } catch (TrippiException e) {
                    logger.error("Error running " + queryLang + " query " + query + " : " + e.getMessage(), e);
                }
            } else {
                logger.error("spo query must specify s, p or o as output variable binding");
            }
        } else {
            logger.error("Query language not supported: " + queryLang);
        }
        return res;
    }


    @Override
    public String buildRESTParentHierarchy(String pid)
            throws MelcoeXacmlException {
                Set<String> parents = getParents(pid);
                if (parents == null || parents.size() == 0) {
                    return "/" + pid;
                }

                String[] parentArray = parents.toArray(new String[parents.size()]);

                // FIXME: always uses the first parent.  If/when we allow multiple hierarchies this needs changing to return all hierarchies
                return buildRESTParentHierarchy(parentArray[0]) + "/" + pid;
            }

    // get parent/child query based on parent and child relationships
    // note: single variable in result, variable must be "parent"
    protected String getTQLQuery(String pidUri) {
        StringBuilder sb = new StringBuilder();

        sb.append("select $parent from <#ri> where ");
        // outward
        sb.append("("); // start outward
        sb.append("<" + pidUri + "> $rel1 $parent ");
        sb.append(" and ("); // start var bindings
        // bind outward relationship variable
        for (int i = 0; i < parentRelationships.size(); i++) {
            if (i > 0) {
                sb.append(" or ");
            }
            sb.append("$rel1 <http://mulgara.org/mulgara#is> <" + parentRelationships.get(i) + "> ");
        }
        sb.append(")"); // end var bindings

        sb.append(")"); // end outward

        // inward
        if (parentRelationships != null && !parentRelationships.isEmpty()) {
            sb.append(" or ("); // start inward
            sb.append("$parent $rel2 <" + pidUri + "> ");
            sb.append(" and ("); // start inward var bindings
            // bind inward relationship variable
            for (int i = 0; i < childRelationships.size(); i++) {
                if (i > 0) {
                    sb.append(" or ");
                }
                sb.append("$rel2 <http://mulgara.org/mulgara#is> <" + childRelationships.get(i) + "> ");
            }
            sb.append(")"); // end inward var bindings
            sb.append(")"); // end inward
        }

        return sb.toString();
    }
    protected String getSPARQLQuery(String pidUri) {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ?parent FROM <#ri> WHERE ");
        sb.append(" { ");
        // outward relationships
        for (int i = 0; i < parentRelationships.size(); i++) {
            if (i > 0) {
                sb.append(" UNION ");
            }
            sb.append(" { ");
            sb.append(" <" + pidUri + "> <" + parentRelationships.get(i) + "> ?parent.");
            sb.append(" } ");
        }
        // inward relationships if present
        if (childRelationships != null && !childRelationships.isEmpty()) {
            for (int i = 0; i < childRelationships.size(); i++) {
                sb.append(" UNION ");
                sb.append(" { ");
                sb.append(" ?parent  <" + parentRelationships.get(i) + "> <" + pidUri + ">.");
                sb.append(" } ");
            }
        }
        sb.append(" } ");

        return sb.toString();
    }

    protected Set<String> getParents(String pid) throws MelcoeXacmlException {
        logger.debug("Obtaining parents for: {}", pid);

        Set<String> parentPIDs = new HashSet<String>();
        if (pid.equalsIgnoreCase(Constants.FEDORA_REPOSITORY_PID.uri)) {
            return parentPIDs;
        }

        // build query using query language in following preferences
        // tuple itql
        // tuple sparql
        // triple SPO

        String pidUri = getFedoraResourceURI(pid);

        // tuple query
        if (verifyTupleLanguage(ITQL) || verifyTupleLanguage(SPARQL)) {
            String query = "";
            String lang = "";
            if (itqlTuples) {
                lang = ITQL;
                query = getTQLQuery(pidUri);
            } else if (sparqlTuples){
                lang = SPARQL;
                query = getSPARQLQuery(pidUri);
            } else {
                throw new MelcoeXacmlException("RI supports no expected query languages for parent queries.");
            }

            logger.debug("{} query: {}", lang, query);

            TupleIterator tuples;
            try {
                tuples = RI.findTuples(lang, query,0, false);
                if (tuples != null) {
                    while (tuples.hasNext()) {
                        Node parent = tuples.next().get("parent");
                        if (parent != null) {
                            if (parent.isURIReference()) {
                                try {
                                    PID parentPID = new PID(parent.stringValue());
                                    logger.debug("Found parent " + parentPID.toString());
                                    parentPIDs.add(parentPID.toString());
                                } catch (MalformedPIDException e) {
                                    logger.warn("parent/child relationship target is not a Fedora object" + parent.stringValue());
                                }
                            } else {
                                logger.warn("parent/child query result is not a Fedora object " + parent.stringValue());
                            }
                        } else {
                            logger.error("parent/child tuple result did not contain parent variable");
                        }
                    }
                    logger.debug("Query result count: " + tuples.count());

                } else {
                    logger.debug("Query returned 0 results");
                }

            } catch (TrippiException e) {
                logger.error("Error running TQL query " + e.getMessage(), e);
                return parentPIDs;
            }


        } else if (spoTriples || verifyTripleLanguage(SPO)) {
            // gets all relationships for pid, then filters results
            // rather than executing separate queries for each relationship

            // parent relationships
            Map<String, Set<String>> pRels = null;
            if (parentRelationships.size() == 1) {
                pRels= getRelationships(pidUri, parentRelationships.get(0));
            } else {
                pRels = getRelationships(pidUri);
            }

           for (String rel : pRels.keySet()) {
               if (parentRelationships.contains(rel)) {
                   for (String parent : pRels.get(rel)) {
                       PID parentPid;
                    try {
                        parentPid = new PID(parent);
                        parentPIDs.add(parentPid.toString());
                    } catch (MalformedPIDException e) {
                        logger.warn("Parent of " + pid + " through relationship " + rel + " is not a Fedora resource");
                    }
                   }
               }
           }
           // child relationships
           if (childRelationships != null && !childRelationships.isEmpty()) {
               Map<String, Set<String>> cRels = null;
               if (childRelationships.size() == 1) {
                   cRels= getReverseRelationships(pidUri, childRelationships.get(0));
               } else {
                   cRels = getReverseRelationships(pidUri);
               }
               for (String rel : cRels.keySet()) {
                   if (childRelationships.contains(rel)) {
                       for (String parent : cRels.get(rel)) {
                           PID parentPid;
                        try {
                            parentPid = new PID(parent);
                            parentPIDs.add(parentPid.toString());
                        } catch (MalformedPIDException e) {
                            logger.warn("Parent of " + pid + " through relationship " + rel + " is not a Fedora resource");
                        }
                       }
                   }
               }
           }

        } else {
            logger.error("Can't get parents: Resource index implementation must support SPARQL tuple queries or SPO triple queries");
            return parentPIDs;
        }

        return parentPIDs;
    }

    private boolean verifyTripleLanguage(String lang) {
        if (tripleLanguages == null){
            tripleLanguages = Arrays.asList(RI.listTripleLanguages());
            spoTriples = tripleLanguages.contains(SPO);
        }
        return tripleLanguages.contains(lang);
    }

    private boolean verifyTupleLanguage(String lang){
        if (tupleLanguages == null){
            tupleLanguages = Arrays.asList(RI.listTupleLanguages());
            sparqlTuples = tupleLanguages.contains(SPARQL);
            itqlTuples = tupleLanguages.contains(ITQL);
        }
        return tupleLanguages.contains(lang);
    }


}
