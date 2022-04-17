package edu.ucr.cs.bdlab.raptor;

//import edu.ucr.cs.bdlab.raptor.SingleMachineRaptorJoin;

import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import java.io.PrintWriter;
import java.util.List; // lists
import java.util.Map;

// jackson library to read/write json files
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
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

public class SinglePolygonServlet extends HttpServlet {

    private SparkConnector sparkconnector;

    private JavaSpatialSparkContext jssc;

    public SinglePolygonServlet() {
        System.out.println("----initializing single polygon servlet");

        // get or create spark context
        sparkconnector = SparkConnector.getInstance();
        jssc = new JavaSpatialSparkContext(sparkconnector.getSC());
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // time at start of GET request
        long t1 = System.nanoTime();

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server
        // because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        System.out.println("----POST SinglePolygonServlet");
        String encodedData = request.getReader().readLine();
        //System.out.println(encodedData);
        System.out.println("----decoded data:");
        byte[] decodedBytes = Base64.getDecoder().decode(encodedData.getBytes());
        String decodedString = new String(decodedBytes, StandardCharsets.UTF_8);
        System.out.println(decodedString);

        // time at end of GET request
        long t2 = System.nanoTime();

        // print out statistics
        System.out.println("----request duration:");
        System.out.println((t2 - t1) * 1e-9);
    }
}