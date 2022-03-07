package edu.ucr.cs.bdlab.raptor;

import java.io.IOException;

import org.json.JSONObject;

// import java.util.*; // maps
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.beast.JavaSpatialRDDHelper;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import scala.Tuple2;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class SoilServlet extends HttpServlet {

    protected SparkConnector sparkconnector;

    protected JavaSpatialSparkContext jssc;

    public SoilServlet() {
        System.out.println("----initializing soil servlet");

        // get or create spark context
        sparkconnector = SparkConnector.getInstance();
        jssc = new JavaSpatialSparkContext(sparkconnector.getSC());
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        float minx, miny, maxx, maxy;

        // time at start of GET request
        long t1 = System.nanoTime();

        // wrap code in try-catch block to handle case where user accesses endpoint directly in browser
        try {

            minx = Float.parseFloat(request.getParameter("minx"));
            miny = Float.parseFloat(request.getParameter("miny"));
            maxx = Float.parseFloat(request.getParameter("maxx"));
            maxy = Float.parseFloat(request.getParameter("maxy"));

            System.out.println("----minx: " + Float.toString(minx));
            System.out.println("----miny: " + Float.toString(miny));
            System.out.println("----maxx: " + Float.toString(maxx));
            System.out.println("----maxy: " + Float.toString(maxy));

        } catch (java.lang.NullPointerException e) {

            // extents obj isn't given when accessing endpoint via browser
            // so fill in our own values
            minx = -130;
            miny = 32;
            maxx = -115;
            maxy = 45;
        }

        //dbr.read();

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.setStatus(HttpServletResponse.SC_OK);

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server
        // because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        // load raster data
        JavaRDD<ITile> raster = jssc.geoTiff("data/tif/0_5_compressed/ph.tif", 0, new BeastOptions());

        JavaRDD<IFeature> records = jssc.shapefile("data/shapefile/CA_farmland.zip");
        
        // filter by map extents
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry extents = geometryFactory.toGeometry(new Envelope(minx, maxx, miny, maxy));
        // take 1000 records maximum
        List<IFeature> filteredRecords = JavaSpatialRDDHelper.rangeQuery(records, extents).take(1000);

        System.out.println("----done reading records");

        // run raptor join operation
        JavaRDD<RaptorJoinFeature<Float>> join = JavaSpatialRDDHelper.raptorJoin(jssc.parallelize(filteredRecords), raster, new BeastOptions());

        // aggregate min results
        JavaPairRDD<String, Float> aggMinResults = join.mapToPair(v -> new Tuple2<>(v.feature(), v.m()))
            .reduceByKey(Float::min)
            .mapToPair(fv -> {
                String name = fv._1().getAs("County");
                float val = fv._2();
                return new Tuple2<>(name, val);
            });

        // aggregate max results
        JavaPairRDD<String, Float> aggMaxResults = join.mapToPair(v -> new Tuple2<>(v.feature(), v.m()))
                .reduceByKey(Float::max)
                .mapToPair(fv -> {
                    String name = fv._1().getAs("County");
                    float val = fv._2();
                    return new Tuple2<>(name, val);
                });
        
        JSONObject responseJSON = new JSONObject();
        
        // write output
        System.out.println("County\tMin pH\n");
        for (Map.Entry<String, Float> result : aggMinResults.collectAsMap().entrySet()) {
            System.out.printf("%s\t%f\n", result.getKey(), result.getValue());
        }

        System.out.println("County\tMax pH\n");
        for (Map.Entry<String, Float> result : aggMaxResults.collectAsMap().entrySet()) {
            System.out.printf("%s\t%f\n", result.getKey(), result.getValue());
            responseJSON.put(result.getKey(), result.getValue());
        }

        PrintWriter pw = response.getWriter();
        pw.print(responseJSON.toString());
        
    }
}