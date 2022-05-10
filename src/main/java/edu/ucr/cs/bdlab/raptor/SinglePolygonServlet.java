package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.JavaSpatialRDDHelper;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.raptor.SingleMachineRaptorJoin;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import scala.Tuple2;

public class SinglePolygonServlet extends HttpServlet {

    private SparkConnector sparkconnector;
    private JavaSpatialSparkContext jssc;
    private String vectorPath;

    public SinglePolygonServlet() {
        System.out.println("----initializing single polygon servlet");

        // get or create spark context
        sparkconnector = SparkConnector.getInstance();
        jssc = new JavaSpatialSparkContext(sparkconnector.getSC());

        // set vector data file path
        vectorPath = "data/shapefile/CA_farmland.zip";
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // time at start of GET request
        long t1 = System.nanoTime();

        // sidebar select parameters
        String soilDepth = "";
        String layer = "";

        // try getting parameters from url
        try {
            soilDepth = request.getParameter("soildepth");
            layer = request.getParameter("layer");

            // print parameters
            System.out.println("----soildepth: " + soilDepth);
            System.out.println("----layer: " + layer);
        } catch (java.lang.NullPointerException e) {

            // print error if we can't get parameters
            System.out.println(e);
        }

        // set content-type as application/json
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server
        // because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        // load raster data based on layer and soil depth
        String rasterPath = "data/tif/";
        if (soilDepth.equals("0-5")) {
            rasterPath = rasterPath.concat("0_5_compressed/");
        } else if (soilDepth.equals("5-15")) {
            rasterPath = rasterPath.concat("5_15_compressed/");
        } else if (soilDepth.equals("15-30")) {
            rasterPath = rasterPath.concat("15_30_compressed/");
        }
        rasterPath = rasterPath.concat(layer + ".tif");
        System.out.println("----raster path=" + rasterPath);

        System.out.println("----POST SinglePolygonServlet");
        String encodedData = request.getReader().readLine();
        //System.out.println(encodedData);
        System.out.println("----decoded data:");
        byte[] decodedBytes = Base64.getDecoder().decode(encodedData.getBytes());
        String decodedString = new String(decodedBytes, StandardCharsets.UTF_8);
        //System.out.println(decodedString);

        // get inner geometry object
        String geomString = decodedString.substring(29).split(",\"properties\"")[0];
        System.out.println(geomString);

        // initialize geojson reader
        GeoJsonReader reader = new GeoJsonReader(new GeometryFactory());

        // try reading the geojson string into a geometry object
        Geometry geom = null;
        try {
            geom = reader.read(geomString);
        } catch (ParseException e) {
            System.err.println("----ERROR: could not parse geojson string");
            e.printStackTrace();
        }

        // now that we have a geometry object
        // call single machine raptor join
        Tuple2<Float, Float> singleMachineResults = SingleMachineRaptorJoin.join(vectorPath, rasterPath, geom);
        System.out.println("----single machine results");
        System.out.println("----min: " + singleMachineResults._1);
        System.out.println("----max: " + singleMachineResults._2);

        // write the geometry string to a temp file
        //PrintWriter out = new PrintWriter("temp.geojson");
        //out.println(geomString);
        //out.close();

        // read temp file into list of ifeatures
        //List<IFeature> singleIFeature = SpatialReader.readInput(jssc, new BeastOptions(), "temp.geojson", "geojson").collect();

        // run distributed raptor join operation
        //JavaRDD<ITile> raster = jssc.geoTiff("data/tif/0_5_compressed/lambda.tif", 0, new BeastOptions());
        //JavaRDD<RaptorJoinFeature<Float>> join = JavaSpatialRDDHelper.raptorJoin(jssc.parallelize(singleIFeature), raster, new BeastOptions());

        // aggregate min results
        //JavaPairRDD<String, Float> aggResults = null;
        //aggResults = join.mapToPair(v -> new Tuple2<>(v.feature(), v.m()))
        //        .reduceByKey(Float::min)
        //        .mapToPair(fv -> {
        //            String name = fv._1().getAs("County");
        //            float val = fv._2();
        //            return new Tuple2<>(name, val);
        //        });

        // write result to json object
        PrintWriter resWriter = response.getWriter();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.put("min", singleMachineResults._1);
        rootNode.put("max", singleMachineResults._2);
        String jsonString = mapper.writer().writeValueAsString(rootNode);
        resWriter.print(jsonString);
        resWriter.flush();

        // time at end of GET request
        long t2 = System.nanoTime();

        // print out statistics
        System.out.println("----request duration:");
        System.out.println((t2 - t1) * 1e-9);
    }
}