package org.openstreetmap.atlas.generator.sharding;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.streaming.resource.http.GetResource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author james-gage
 */
public class OverpassClient
{
    private static final String BASE_QUERY = "http://overpass.kumi.systems/api/interpreter/?data=";
    private static final String END_QUERY = ");out body;";

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

    private static String constructBoundingBox(final Rectangle bounds)
    {
        return bounds.lowerLeft().getLatitude() + "," + bounds.upperLeft().getLongitude() + ","
                + bounds.upperRight().getLatitude() + "," + bounds.lowerRight().getLongitude();
    }

    public CloseableHttpResponse getResponse(final String specificQuery)
            throws UnsupportedEncodingException
    {
        final String query = BASE_QUERY + URLEncoder.encode(specificQuery, "UTF-8");
        return new GetResource(query).getResponse();
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
            final Document doc = builder.parse(response.getEntity().getContent());
            final NodeList nodelist = doc.getElementsByTagName("node");
            final List<OverpassOsmNode> osmNodes = new ArrayList<>();
            for (int i = 0; i < nodelist.getLength(); i++)
            {
                final String osmIdentifier = nodelist.item(i).getAttributes().item(0)
                        .getNodeValue();
                final String latitude = nodelist.item(i).getAttributes().item(1).getNodeValue();
                final String longitude = nodelist.item(i).getAttributes().item(2).getNodeValue();
                osmNodes.add(new OverpassOsmNode(osmIdentifier, latitude, longitude));
            }
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
            final List<OverpassOsmWay> osmWays = new ArrayList<>();
            final Document document = builder.parse(response.getEntity().getContent());
            final NodeList wayNodeList = document.getElementsByTagName("way");
            for (int i = 0; i < wayNodeList.getLength(); i++)
            {
                final Element wayElement = (Element) wayNodeList.item(i);
                final String wayIdentifier = wayElement.getAttributes().item(0).getNodeValue();
                final NodeList nodeNodeList = wayElement.getElementsByTagName("nd");
                final List<String> nodeIdentifiers = new ArrayList<>();
                for (int j = 0; j < nodeNodeList.getLength(); j++)
                {
                    nodeIdentifiers
                            .add(nodeNodeList.item(j).getAttributes().item(0).getNodeValue());
                }
                final NodeList tagNodeList = wayElement.getElementsByTagName("tag");
                final HashMap<String, String> tags = new HashMap<>();
                for (int j = 0; j < tagNodeList.getLength(); j++)
                {
                    tags.put(tagNodeList.item(j).getAttributes().item(0).getNodeValue(),
                            tagNodeList.item(j).getAttributes().item(1).getNodeValue());
                }
                osmWays.add(new OverpassOsmWay(wayIdentifier, nodeIdentifiers, tags));
            }
            return osmWays;
        }
    }
}
