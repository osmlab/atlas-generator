package org.openstreetmap.atlas.generator.sharding;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.streaming.resource.http.PostResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author jwpgage
 */
public class OverpassClient
{
    private static final Logger logger = LoggerFactory.getLogger(OverpassClient.class);

    private static String BASE_QUERY;
    private static final String END_QUERY = ");out body;";
    private static boolean tooMuchResponseData = false;
    private String server = "-api.de";
    private HttpHost proxy;

    public static String buildCompactQuery(final String type, final Rectangle bounds)
    {
        return type + "(" + constructBoundingBox(bounds) + ");<;";
    }

    public static String buildCompoundQuery(final String type, final String key, final String value,
            final Rectangle bounds)
    {
        return type + "[\"" + key + "\"~\"" + value + "\"](" + constructBoundingBox(bounds)
                + END_QUERY;
    }

    public static String buildQuery(final String type, final Rectangle bounds)
    {
        return type + "(" + constructBoundingBox(bounds) + END_QUERY;
    }

    public static String buildQuery(final String type, final String key, final String value,
            final Rectangle bounds)
    {
        return type + "[\"" + key + "\"=\"" + value + "\"](" + constructBoundingBox(bounds)
                + END_QUERY;
    }

    private static void checkForTooMuchData(final Document document)
    {
        if (document.getDocumentElement().getElementsByTagName("remark").item(0) != null
                && document.getDocumentElement().getElementsByTagName("remark").item(0)
                        .getTextContent().contains("runtime error: Query ran out of memory"))
        {
            tooMuchResponseData = true;
        }
    }

    private static String constructBoundingBox(final Rectangle bounds)
    {
        return bounds.lowerLeft().getLatitude() + "," + bounds.upperLeft().getLongitude() + ","
                + bounds.upperRight().getLatitude() + "," + bounds.lowerRight().getLongitude();
    }

    public OverpassClient()
    {
        initializeBaseQuery();
    }

    public OverpassClient(final String server)
    {
        if (server != null)
        {
            this.server = server;
        }
        initializeBaseQuery();
    }

    public OverpassClient(final String server, final HttpHost proxy)
    {
        if (server != null)
        {
            this.server = server;
        }
        this.proxy = proxy;
        initializeBaseQuery();
    }

    public CloseableHttpResponse getResponse(final String specificQuery)
            throws ParserConfigurationException, UnsupportedOperationException, SAXException,
            IOException
    {
        // post request to handle long overpass queries, get request has limit on URI length
        final PostResource post = new PostResource(BASE_QUERY);
        if (this.proxy != null)
        {
            post.setProxy(this.proxy);
        }
        post.setStringBody("data=" + URLEncoder.encode(specificQuery, "UTF-8"),
                ContentType.APPLICATION_FORM_URLENCODED);
        return post.getResponse();
    }

    public boolean hasTooMuchResponseData()
    {
        return this.tooMuchResponseData;
    }

    /**
     * Makes a way Overpass query, parses the response xml, and returns the identifiers of nodes
     * that make up the ways that match the query
     *
     * @param query
     *            The Overpass query.
     * @return The identifiers of the nodes that make up all ways matching the query.
     * @throws ParserConfigurationException
     *             If a DocumentBuilder cannot be created which satisfies the configuration
     *             requested.
     * @throws UnsupportedOperationException
     *             If any IO errors occur.
     * @throws SAXException
     *             If any parse errors occur.
     * @throws IOException
     *             If is is null.
     */
    public List<String> nodeIdsFromWayQuery(final String query) throws ParserConfigurationException,
            UnsupportedOperationException, SAXException, IOException
    {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder = factory.newDocumentBuilder();
        try (CloseableHttpResponse response = this.getResponse(query))
        {
            final Document doc = builder.parse(response.getEntity().getContent());
            final NodeList nodelist = doc.getElementsByTagName("nd");
            final List<String> nodeIds = new ArrayList<>();
            for (int i = 0; i < nodelist.getLength(); i++)
            {
                nodeIds.add(nodelist.item(i).getAttributes().item(0).getNodeValue());
            }
            return nodeIds;
        }
    }

    /**
     * Makes a node Overpass query, parses the response xml, and returns the nodes that match the
     * query
     *
     * @param query
     *            The node Overpass query.
     * @return The nodes that match the query.
     * @throws UnsupportedOperationException
     *             If any IO errors occur.
     * @throws SAXException
     *             If any parse errors occur.
     * @throws IOException
     *             If is is null.
     * @throws ParserConfigurationException
     *             If a DocumentBuilder cannot be created which satisfies the configuration
     *             requested.
     */
    public List<OverpassOsmNode> nodesFromQuery(final String query)
            throws UnsupportedOperationException, SAXException, IOException,
            ParserConfigurationException
    {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder = factory.newDocumentBuilder();
        try (CloseableHttpResponse response = this.getResponse(query))
        {
            logger.info("Parsing node query response...");
            final Document document = builder.parse(response.getEntity().getContent());
            checkForTooMuchData(document);
            final NodeList nodelist = document.getElementsByTagName("node");
            final int nodeListLength = nodelist.getLength();
            final List<OverpassOsmNode> osmNodes = new ArrayList<>(nodeListLength);
            for (int i = 0; i < nodeListLength; i++)
            {
                final String osmIdentifier = nodelist.item(i).getAttributes().item(0)
                        .getNodeValue();
                final String latitude = nodelist.item(i).getAttributes().item(1).getNodeValue();
                final String longitude = nodelist.item(i).getAttributes().item(2).getNodeValue();
                osmNodes.add(new OverpassOsmNode(osmIdentifier, latitude, longitude));
            }
            logger.info("Parsed all nodes.");
            return osmNodes;
        }
    }

    /**
     * Makes a way Overpass query, parses the response xml, and returns the ways that match the
     * query
     *
     * @param query
     *            The way Overpass query.
     * @return The nodes that match the query
     * @throws ParserConfigurationException
     *             If a DocumentBuilder cannot be created which satisfies the configuration
     *             requested.
     * @throws IOException
     *             If it is null.
     * @throws UnsupportedOperationException
     *             Of any IO errors occur.
     * @throws SAXException
     *             If any parse errors occur.
     */
    public List<OverpassOsmWay> waysFromQuery(final String query)
            throws ParserConfigurationException, IOException, UnsupportedOperationException,
            SAXException
    {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder = factory.newDocumentBuilder();
        try (CloseableHttpResponse response = this.getResponse(query))
        {
            logger.info("Parsing way query response...");
            final Document document = builder.parse(response.getEntity().getContent());
            checkForTooMuchData(document);
            final NodeList wayNodeList = document.getElementsByTagName("way");
            final int wayListLength = wayNodeList.getLength();
            final List<OverpassOsmWay> osmWays = new ArrayList<>(wayListLength);
            for (int i = 0; i < wayListLength; i++)
            {
                final Element wayElement = (Element) wayNodeList.item(i);
                final String wayIdentifier = wayElement.getAttributes().item(0).getNodeValue();
                final NodeList nodeNodeList = wayElement.getElementsByTagName("nd");
                final int nodeListLength = nodeNodeList.getLength();
                final List<String> nodeIdentifiers = new ArrayList<>(nodeListLength);
                for (int j = 0; j < nodeListLength; j++)
                {
                    nodeIdentifiers
                            .add(nodeNodeList.item(j).getAttributes().item(0).getNodeValue());
                }
                final NodeList tagNodeList = wayElement.getElementsByTagName("tag");
                final int tagListLength = tagNodeList.getLength();
                final HashMap<String, String> tags = new HashMap<>(tagListLength);
                for (int j = 0; j < tagListLength; j++)
                {
                    tags.put(tagNodeList.item(j).getAttributes().item(0).getNodeValue(),
                            tagNodeList.item(j).getAttributes().item(1).getNodeValue());
                }
                osmWays.add(new OverpassOsmWay(wayIdentifier, nodeIdentifiers, tags));
            }
            logger.info("Parsed all ways.");
            return osmWays;
        }
    }

    private void initializeBaseQuery()
    {
        BASE_QUERY = "http://overpass" + this.server + "/api/interpreter/";
        logger.info("Overpass Server Queried: " + BASE_QUERY);
    }
}
