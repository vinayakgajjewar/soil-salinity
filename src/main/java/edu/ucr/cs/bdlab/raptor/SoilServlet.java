package edu.ucr.cs.bdlab.raptor;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// jackson library to read/write json files
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.beast.JavaSpatialRDDHelper;

import edu.ucr.cs.bdlab.beast.indexing.RTreeFeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import scala.Tuple2;
import scala.Tuple7;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.collection.Iterator;

/**
 * Computes soil statistics for all Farmlands defined in a specific region.
 */
public class SoilServlet extends HttpServlet {
    Configuration hadoopConf = new Configuration();

    static boolean rangeOverlap(String r1, String r2) {
        String[] parts1 = r1.split("-");
        int begin1 = Integer.parseInt(parts1[0]);
        int end1 = Integer.parseInt(parts1[1]);
        String[] parts2 = r2.split("-");
        int begin2 = Integer.parseInt(parts2[0]);
        int end2 = Integer.parseInt(parts2[1]);
        // Treat both ranges as begin-inclusive and end-exclusive
        return !(begin1 >= end2 || begin2 >= end1);
    }

    static final Map<String, String> rasterFiles;
    static {
        rasterFiles = new HashMap<>();
        rasterFiles.put("0-5", "0_5_compressed");
        rasterFiles.put("5-15", "5_15_compressed");
        rasterFiles.put("15-30", "15_30_compressed");
        rasterFiles.put("30-60", "30_60_compressed");
        rasterFiles.put("60-100", "60_100_compressed");
        rasterFiles.put("100-200", "100_200_compressed");
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // time at start of GET request
        long t1 = System.nanoTime();

        // we set content-type as application/geo+json
        // not application/json
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        // Load the Farmland features
        RTreeFeatureReader reader = new RTreeFeatureReader();
        Path indexPath = new Path("data/CA_farmland/CA_farmland.rtree");
        //Path indexPath = new Path("./CA_farmland.rtree");
        FileSystem fs = indexPath.getFileSystem(hadoopConf);
        long fileLength = fs.getFileStatus(indexPath).getLen();
        FileSplit inputFileSplit = new FileSplit(indexPath, 0, fileLength, null);
        BeastOptions opts = new BeastOptions();
        String soilDepth = null;
        String layer = null;
        Envelope mbr = null;
        try {
            // get sidebar select parameters
            soilDepth = request.getParameter("soildepth");
            layer = request.getParameter("layer");

            // get extents parameters
            double minx = Double.parseDouble(request.getParameter("minx"));
            double miny = Double.parseDouble(request.getParameter("miny"));
            double maxx = Double.parseDouble(request.getParameter("maxx"));
            double maxy = Double.parseDouble(request.getParameter("maxy"));
            mbr = new Envelope(minx, maxx, miny, maxy);
            opts.set(SpatialFileRDD.FilterMBR(), String.format("%f,%f,%f,%f", minx, miny, maxx, maxy));
        } catch (java.lang.NullPointerException e) {
            // MBR not passed. Use all farmlands
            if (soilDepth == null || layer == null) {
                PrintWriter writer = response.getWriter();
                writer.printf("{\"error\": \"Error! Both 'soildepth' and 'layer' parameters are required\"}");
            }
        }
        // Initialize the reader that reads the relevant farmlands
        reader.initialize(inputFileSplit, opts);

        // Retrieve in an array to prepare the zonal statistics calculation
        List<IFeature> farmlands = new ArrayList<>();
        for (IFeature farmland : reader)
            farmlands.add(farmland);
        reader.close();
        System.out.printf("Read %d records in %f seconds\n", farmlands.size(), (System.nanoTime() - t1) * 1E-9);

        // load raster data based on selected soil depth and layer
        List<String> matchingRasterFiles = new ArrayList<>();
        for (Map.Entry<String, String> rasterFile : rasterFiles.entrySet()) {
            if (rangeOverlap(rasterFile.getKey(), soilDepth))
                matchingRasterFiles.add(String.format("data/tif/%s/%s.tif", rasterFile.getValue(), layer));
        }

        // Load raster data
        Collector[] finalResults = null;
        GeoTiffReader<Float> rasterReader = new GeoTiffReader<>();
        for (String matchingRasterFile : matchingRasterFiles) {
            rasterReader.initialize(fs, matchingRasterFile, "0", opts, null);
            Collector[] stats = ZonalStatistics.zonalStatsLocal(farmlands.toArray(new IFeature[0]), rasterReader,
                SoilStatistics.class);
            if (finalResults == null) {
                finalResults = stats;
            } else {
                for (int i = 0; i < finalResults.length; i++) {
                    if (finalResults[i] == null)
                        finalResults[i] = stats[i];
                    else if (stats[i] != null)
                        finalResults[i].accumulate(stats[i]);
                }
            }
        }

        // write results to json object
        PrintWriter out = response.getWriter();
        ObjectMapper mapper = new ObjectMapper();

        // create query node
        ObjectNode queryNode = mapper.createObjectNode();
        queryNode.put("soildepth", soilDepth);
        queryNode.put("layer", layer);

        // create mbr node
        // inside query node
        if (mbr != null) {
            ObjectNode mbrNode = mapper.createObjectNode();
            mbrNode.put("minx", mbr.getMinX());
            mbrNode.put("miny", mbr.getMinY());
            mbrNode.put("maxx", mbr.getMaxX());
            mbrNode.put("maxy", mbr.getMaxY());
            queryNode.set("mbr", mbrNode);
        }

        // create results node
        ArrayNode resultsNode = mapper.createArrayNode();

        // populate json object with max vals
        for (int i = 0; i < finalResults.length; i++) {
            SoilStatistics s = (SoilStatistics) finalResults[i];
            if (s != null) {
                ObjectNode resultNode = mapper.createObjectNode();
                resultNode.put("objectid", ((Number) farmlands.get(i).getAs("OBJECTID")).longValue());
                resultNode.put("min", s.getMin());
                resultNode.put("max", s.getMax());
                resultNode.put("average", s.getAverage());
                resultNode.put("count", s.getCount());
                resultNode.put("stdev", s.getStdev());
                //resultNode.put("median", s.getMedian());
                resultsNode.add(resultNode);
            }
        }

        // create root node
        // contains queryNode and resultsNode
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.set("query", queryNode);
        rootNode.set("results", resultsNode);

        // write values to response writer
        String jsonString = mapper.writer().writeValueAsString(rootNode);
        out.print(jsonString);
        out.flush();

        // time at end of GET request
        long t2 = System.nanoTime();

        // print out statistics
        System.out.printf("----Finished SoilServlet in %f seconds\n", (t2 - t1) * 1e-9);
    }
}