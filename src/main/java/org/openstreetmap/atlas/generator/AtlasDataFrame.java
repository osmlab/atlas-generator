package org.openstreetmap.atlas.generator;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;

import scala.collection.JavaConversions;

/**
 * Methods for converting an {@link Atlas} RDD to {@link Dataset}
 * 
 * @author jamesgage
 */
public final class AtlasDataFrame
{
    public static Dataset<Row> atlasAreasToDataFrame(final JavaRDD<Atlas> atlasRDD,
            final JavaSparkContext javaSparkContext)
    {
        // Generate the schema
        final List<StructField> fields = new ArrayList<>();
        final StructField identifier = DataTypes.createStructField("identifier",
                DataTypes.StringType, true);
        fields.add(identifier);
        final StructField geometry = DataTypes.createStructField("geometry", DataTypes.StringType,
                true);
        fields.add(geometry);
        final StructField bounds = DataTypes.createStructField("bounds", DataTypes.StringType,
                true);
        fields.add(bounds);
        final StructField tags = DataTypes.createStructField("tags",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
        fields.add(tags);
        final StructField relations = DataTypes.createStructField("relations",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(relations);
        final StructType schema = DataTypes.createStructType(fields);

        // Convert entities of atlas to Rows
        final JavaRDD<Row> rowRDD = atlasRDD.flatMap(atlas ->
        {
            final List<Row> rows = new ArrayList<>();
            atlas.areas().forEach(area ->
            {
                final long nodeIdentifier = area.getIdentifier();
                final String idString = String.valueOf(nodeIdentifier);
                final String wktGeometry = area.toWkt();
                final String areaBounds = area.bounds().toWkt();
                final String[] relationArray = area.relations().stream()
                        .map(relation -> String.valueOf(relation.getIdentifier()))
                        .toArray(String[]::new);
                rows.add(RowFactory.create(idString, wktGeometry, areaBounds,
                        JavaConversions.mapAsScalaMap(area.getTags()), relationArray));
            });
            return rows.iterator();
        });

        final SQLContext sqlContext = new SQLContext(javaSparkContext);
        return sqlContext.createDataFrame(rowRDD, schema);
    }

    public static Dataset<Row> atlasEdgesToDataFrame(final JavaRDD<Atlas> atlasRDD,
            final JavaSparkContext javaSparkContext)
    {
        // Generate the schema
        final List<StructField> fields = new ArrayList<>();
        final StructField identifier = DataTypes.createStructField("identifier",
                DataTypes.StringType, true);
        fields.add(identifier);
        final StructField geometry = DataTypes.createStructField("geometry", DataTypes.StringType,
                true);
        fields.add(geometry);
        final StructField start = DataTypes.createStructField("start", DataTypes.StringType, true);
        fields.add(start);
        final StructField end = DataTypes.createStructField("end", DataTypes.StringType, true);
        fields.add(end);
        final StructField inEdges = DataTypes.createStructField("inEdges",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(inEdges);
        final StructField outEdges = DataTypes.createStructField("outEdges",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(outEdges);
        final StructField hasReverse = DataTypes.createStructField("hasReverseEdge",
                DataTypes.BooleanType, true);
        fields.add(hasReverse);
        final StructField isClosed = DataTypes.createStructField("isClosed", DataTypes.BooleanType,
                true);
        fields.add(isClosed);
        final StructField tags = DataTypes.createStructField("tags",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
        fields.add(tags);
        final StructField relations = DataTypes.createStructField("relations",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(relations);
        final StructType schema = DataTypes.createStructType(fields);

        // Convert entities of atlas to Rows
        // final List<Row> rows = new ArrayList<>();
        final JavaRDD<Row> rowRDD = atlasRDD.flatMap(atlas ->
        {
            final List<Row> rows = new ArrayList<>();
            atlas.edges().forEach(edge ->
            {
                final long nodeIdentifier = edge.getIdentifier();
                final String idString = String.valueOf(nodeIdentifier);
                final String wktGeometry = edge.toWkt();
                final String[] inEdgeArray = edge.inEdges().stream()
                        .map(inEdge -> String.valueOf(inEdge.getIdentifier()))
                        .toArray(String[]::new);
                final String[] outEdgeArray = edge.inEdges().stream()
                        .map(outEdge -> String.valueOf(outEdge.getIdentifier()))
                        .toArray(String[]::new);
                final String startNode = String.valueOf(edge.start().getIdentifier());
                final String endNode = String.valueOf(edge.end().getIdentifier());
                final Boolean hasReverseEdge = edge.hasReverseEdge();
                final Boolean isClosedEdge = edge.isClosed();
                final String[] relationArray = edge.relations().stream()
                        .map(relation -> String.valueOf(relation.getIdentifier()))
                        .toArray(String[]::new);
                rows.add(RowFactory.create(idString, wktGeometry, startNode, endNode, inEdgeArray,
                        outEdgeArray, hasReverseEdge, isClosedEdge,
                        JavaConversions.mapAsScalaMap(edge.getTags()), relationArray));
            });
            return rows.iterator();
        });
        final SQLContext sqlContext = new SQLContext(javaSparkContext);

        // Create dataframe from rowRDD
        return sqlContext.createDataFrame(rowRDD, schema);
    }

    public static Dataset<Row> atlasLinesToDataFrame(final JavaRDD<Atlas> atlasRDD,
            final JavaSparkContext javaSparkContext)
    {
        // Generate the schema
        final List<StructField> fields = new ArrayList<>();
        final StructField identifier = DataTypes.createStructField("identifier",
                DataTypes.StringType, true);
        fields.add(identifier);
        final StructField geometry = DataTypes.createStructField("geometry", DataTypes.StringType,
                true);
        fields.add(geometry);
        final StructField bounds = DataTypes.createStructField("bounds", DataTypes.StringType,
                true);
        fields.add(bounds);
        final StructField isClosed = DataTypes.createStructField("isClosed", DataTypes.BooleanType,
                true);
        fields.add(isClosed);
        final StructField tags = DataTypes.createStructField("tags",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
        fields.add(tags);
        final StructField relations = DataTypes.createStructField("relations",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(relations);
        final StructType schema = DataTypes.createStructType(fields);

        // Convert entities of atlas to Rows
        final JavaRDD<Row> rowRDD = atlasRDD.flatMap(atlas ->
        {
            final List<Row> rows = new ArrayList<>();
            atlas.lines().forEach(line ->
            {
                final long nodeIdentifier = line.getIdentifier();
                final String idString = String.valueOf(nodeIdentifier);
                final String wktGeometry = line.toWkt();
                final String lineBounds = line.bounds().toWkt();
                final Boolean isClosedLine = line.isClosed();
                final String[] relationArray = line.relations().stream()
                        .map(relation -> String.valueOf(relation.getIdentifier()))
                        .toArray(String[]::new);
                rows.add(RowFactory.create(idString, wktGeometry, lineBounds, isClosedLine,
                        JavaConversions.mapAsScalaMap(line.getTags()), relationArray));
            });
            return rows.iterator();
        });
        final SQLContext sqlContext = new SQLContext(javaSparkContext);
        // Create dataframe from rowRDD
        return sqlContext.createDataFrame(rowRDD, schema);
    }

    public static Dataset<Row> atlasNodesToDataFrame(final JavaRDD<Atlas> atlasRDD,
            final JavaSparkContext javaSparkContext)
    {
        // Generate the schema
        final List<StructField> fields = new ArrayList<>();
        final StructField identifier = DataTypes.createStructField("identifier",
                DataTypes.StringType, true);
        fields.add(identifier);
        final StructField location = DataTypes.createStructField("location", DataTypes.StringType,
                true);
        fields.add(location);
        final StructField inEdges = DataTypes.createStructField("inEdges",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(inEdges);
        final StructField outEdges = DataTypes.createStructField("outEdges",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(outEdges);
        final StructField tags = DataTypes.createStructField("tags",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
        fields.add(tags);
        final StructField relations = DataTypes.createStructField("relations",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(relations);
        final StructType schema = DataTypes.createStructType(fields);

        // Convert entities of atlas to Rows
        final JavaRDD<Row> rowRDD = atlasRDD.flatMap(atlas ->
        {
            final List<Row> rows = new ArrayList<>();
            atlas.nodes().forEach(node ->
            {
                final long nodeIdentifier = node.getIdentifier();
                final String idString = String.valueOf(nodeIdentifier);
                final String locationString = node.getLocation().toWkt();
                final String[] inEdgeArray = node.inEdges().stream()
                        .map(edge -> String.valueOf(edge.getIdentifier())).toArray(String[]::new);
                final String[] outEdgeArray = node.outEdges().stream()
                        .map(edge -> String.valueOf(edge.getIdentifier())).toArray(String[]::new);
                final String[] relationArray = node.relations().stream()
                        .map(relation -> String.valueOf(relation.getIdentifier()))
                        .toArray(String[]::new);
                rows.add(RowFactory.create(idString, locationString, inEdgeArray, outEdgeArray,
                        JavaConversions.mapAsScalaMap(node.getTags()), relationArray));
            });
            return rows.iterator();
        });
        final SQLContext sqlContext = new SQLContext(javaSparkContext);

        // Create dataframe from rowRDD
        return sqlContext.createDataFrame(rowRDD, schema);
    }

    public static Dataset<Row> atlasPointsToDataFrame(final JavaRDD<Atlas> atlasRDD,
            final JavaSparkContext javaSparkContext)
    {
        // Generate the schema
        final List<StructField> fields = new ArrayList<>();
        final StructField identifier = DataTypes.createStructField("identifier",
                DataTypes.StringType, true);
        fields.add(identifier);
        final StructField location = DataTypes.createStructField("location", DataTypes.StringType,
                true);
        fields.add(location);
        final StructField tags = DataTypes.createStructField("tags",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
        fields.add(tags);
        final StructField relations = DataTypes.createStructField("relations",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(relations);
        final StructType schema = DataTypes.createStructType(fields);

        // Convert entities of atlas to Rows
        final JavaRDD<Row> rowRDD = atlasRDD.flatMap(atlas ->
        {
            final List<Row> rows = new ArrayList<>();
            atlas.points().forEach(point ->
            {
                final long nodeIdentifier = point.getIdentifier();
                final String idString = String.valueOf(nodeIdentifier);
                final String locationString = point.getLocation().toWkt();
                final String[] relationArray = point.relations().stream()
                        .map(relation -> String.valueOf(relation.getIdentifier()))
                        .toArray(String[]::new);
                rows.add(RowFactory.create(idString, locationString,
                        JavaConversions.mapAsScalaMap(point.getTags()), relationArray));
            });
            return rows.iterator();
        });
        final SQLContext sqlContext = new SQLContext(javaSparkContext);

        // Create dataframe from rowRDD
        return sqlContext.createDataFrame(rowRDD, schema);
    }

    public static Dataset<Row> atlasRelationsToDataFrame(final JavaRDD<Atlas> atlasRDD,
            final JavaSparkContext javaSparkContext)
    {
        // Generate the schema
        final List<StructField> fields = new ArrayList<>();
        final StructField identifier = DataTypes.createStructField("identifier",
                DataTypes.StringType, true);
        fields.add(identifier);
        final StructField geometry = DataTypes.createStructField("geometry", DataTypes.StringType,
                true);
        fields.add(geometry);
        final StructField members = DataTypes.createStructField("memberIds",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(members);
        final StructField memberTypes = DataTypes.createStructField("memberTypes",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(memberTypes);
        final StructField memberRoles = DataTypes.createStructField("memberRoles",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(memberRoles);
        final StructField tags = DataTypes.createStructField("tags",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
        fields.add(tags);
        final StructField relations = DataTypes.createStructField("relations",
                DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(relations);
        final StructType schema = DataTypes.createStructType(fields);

        // Convert entities of atlas to Rows
        final JavaRDD<Row> rowRDD = atlasRDD.flatMap(atlas ->
        {
            final List<Row> rows = new ArrayList<>();
            atlas.relations().forEach(relation ->
            {
                final long nodeIdentifier = relation.getIdentifier();
                final String idString = String.valueOf(nodeIdentifier);
                final String wktGeometry = relation.toWkt();
                final String[] relationMemberIdentifiers = relation.members().stream().sorted()
                        .map(member -> String.valueOf(member.getEntity().getIdentifier()))
                        .toArray(String[]::new);
                final String[] relationMemberRoles = relation.members().stream().sorted()
                        .map(member -> String.valueOf(member.getRole())).toArray(String[]::new);
                final String[] relationMemberTypes = relation.members().stream().sorted()
                        .map(member -> String.valueOf(
                                typeValueToString(member.getEntity().getType().getValue())))
                        .toArray(String[]::new);
                final String[] relationArray = relation.relations().stream()
                        .map(parentRrelation -> String.valueOf(parentRrelation.getIdentifier()))
                        .toArray(String[]::new);
                rows.add(RowFactory.create(idString, wktGeometry, relationMemberIdentifiers,
                        relationMemberTypes, relationMemberRoles,
                        JavaConversions.mapAsScalaMap(relation.getTags()), relationArray));
            });
            return rows.iterator();
        });
        final SQLContext sqlContext = new SQLContext(javaSparkContext);
        // Create dataframe from rowRDD
        return sqlContext.createDataFrame(rowRDD, schema);
    }

    private static String typeValueToString(final int value)
    {
        final int node = 0;
        final int edge = 1;
        final int area = 2;
        final int line = 3;
        final int point = 4;
        final int relation = 5;
        switch (value)
        {
            case node:
                return "NODE";
            case edge:
                return "EDGE";
            case area:
                return "AREA";
            case line:
                return "LINE";
            case point:
                return "POINT";
            case relation:
                return "RELATION";
            default:
                throw new CoreException("Invalid type {}", value);
        }
    }

    private AtlasDataFrame()
    {

    }
}
