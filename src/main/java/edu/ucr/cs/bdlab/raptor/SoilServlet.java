package edu.ucr.cs.bdlab.raptor;

import java.io.IOException;
import java.io.PrintWriter;
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

public class SoilServlet extends HttpServlet {

    // spark context
    private JavaSpatialSparkContext jssc;

    // constructor
    public SoilServlet() {
        System.out.println("----initializing soil servlet");

        // get or create spark context
        SparkConnector sparkconnector = SparkConnector.getInstance();
        jssc = new JavaSpatialSparkContext(sparkconnector.getSC());
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

        // time at start of GET request
        long t1 = System.nanoTime();

        float minx, miny, maxx, maxy;
        String soilDepth = "";
        String layer = "";
        String agg = "";

        // wrap code in try-catch block to handle case where user accesses endpoint directly in browser
        try {

            // get extents parameters
            minx = Float.parseFloat(request.getParameter("minx"));
            miny = Float.parseFloat(request.getParameter("miny"));
            maxx = Float.parseFloat(request.getParameter("maxx"));
            maxy = Float.parseFloat(request.getParameter("maxy"));

            // get sidebar select parameters
            soilDepth = request.getParameter("soildepth");
            layer = request.getParameter("layer");
            agg = request.getParameter("agg");

            // print parameters
            System.out.println("----minx: " + minx);
            System.out.println("----miny: " + miny);
            System.out.println("----maxx: " + maxx);
            System.out.println("----maxy: " + maxy);
            System.out.println("----soildepth: " + soilDepth);
            System.out.println("----layer: " + layer);
            System.out.println("----agg: " + agg);

        } catch (java.lang.NullPointerException e) {

            // extents obj isn't given when accessing endpoint via browser
            // so fill in our own values
            minx = -130;
            miny = 32;
            maxx = -115;
            maxy = 45;
        }

        // set content-type as application/json
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server
        // because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        // load raster data based on selected soil depth and layer
        String rasterPath = "data/tif/";

        // select soil depth
        switch (soilDepth) {
            case "0-5":
                rasterPath = rasterPath.concat("0_5_compressed/");
                break;
            case "5-15":
                rasterPath = rasterPath.concat("5_15_compressed/");
                break;
            case "15-30":
                rasterPath = rasterPath.concat("15_30_compressed/");
                break;
        }
        rasterPath = rasterPath.concat(layer + ".tif");
        System.out.println("----raster path=" + rasterPath);

        // load raster and vector data
        JavaRDD<ITile> raster = jssc.geoTiff(rasterPath, 0, new BeastOptions());
        JavaRDD<IFeature> records = jssc.shapefile("data/shapefile/CA_farmland.zip");

        System.out.println("----done reading records");
        
        // filter by map extents
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry extents = geometryFactory.toGeometry(new Envelope(minx, maxx, miny, maxy));
        // take 1000 records maximum
        JavaRDD<IFeature> filteredRecords = JavaSpatialRDDHelper.rangeQuery(records, extents);

        // run single-machine raptor join operation
        // TODO: get this code working
        //JavaRDD<IFeature> filteredRecords = JavaSpatialRDDHelper.rangeQuery(records, extents);
        //Geometry[] geomArray = filteredRecords.map(x -> x.getGeometry()).take(1000).toArray(new Geometry[1000]);
        //Tuple7<Float, Float, Float, Float, Float, Integer, Float> singleMachineResults = SingleMachineRaptorJoin.join(rasterPath, geomArray);
        //System.out.println("----single machine results");
        //System.out.println("----min: " + singleMachineResults._1());
        //System.out.println("----max: " + singleMachineResults._2());

        // run raptor join operation
        // TODO: convert to single machine join operation
        // TODO: write wrapper function
        JavaRDD<RaptorJoinFeature<Float>> join = JavaSpatialRDDHelper.raptorJoin(filteredRecords.sample(false, 0.001), raster, new BeastOptions());

        // aggregate results RDD
        JavaPairRDD<String, Float> aggResults = null;
        // compute aggregate results based on selected aggregation
        if (agg.equals("Minimum")) {

            // aggregate min results
            aggResults = join.mapToPair(v -> new Tuple2<>(v.feature(), v.m()))
                    .reduceByKey(Float::min)
                    .mapToPair(fv -> new Tuple2<>(Long.toString(fv._1().getAs("OBJECTID")), fv._2()));
        } else if (agg.equals("Maximum")) {

            // aggregate max results
            aggResults = join.mapToPair(v -> new Tuple2<>(v.feature(), v.m()))
                    .reduceByKey(Float::max)
                    .mapToPair(fv -> new Tuple2<>(Long.toString(fv._1().getAs("OBJECTID")), fv._2()));
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
        ObjectNode mbrNode = mapper.createObjectNode();
        mbrNode.put("minx", minx);
        mbrNode.put("miny", miny);
        mbrNode.put("maxx", maxx);
        mbrNode.put("maxy", maxy);
        queryNode.set("mbr", mbrNode);

        // create results node
        ArrayNode resultsNode = mapper.createArrayNode();

        // populate json object with max vals
        System.out.println("County\tMax pH\n");
        assert aggResults != null;
        for (Map.Entry<String, Float> result : aggResults.collectAsMap().entrySet()) {
            //System.out.printf("%s\t%f\n", result.getKey(), result.getValue());
            ObjectNode resultNode = mapper.createObjectNode();
            resultNode.put("objectid", result.getKey());
            resultNode.put("value", result.getValue());
            resultsNode.add(resultNode);
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
        System.out.println("----GET request duration:");
        System.out.println((t2 - t1) * 1e-9);
        System.out.println("----records sent:");
        //System.out.println(filteredRecords.size());
    }
}