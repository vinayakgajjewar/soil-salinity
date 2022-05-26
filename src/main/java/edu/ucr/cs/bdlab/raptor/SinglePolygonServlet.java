package edu.ucr.cs.bdlab.raptor;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.geojson.GeoJsonReader;

import scala.Tuple7;

public class SinglePolygonServlet extends HttpServlet {

    // constructor
    public SinglePolygonServlet() {
        System.out.println("----initializing single polygon servlet");
    }

    // post method
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

        // time at start of POST request
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
            e.printStackTrace();
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

        Geometry[] geomArray = {geom};

        // now that we have a geometry object
        // call single machine raptor join
        Tuple7<Float, Float, Float, Float, Float, Integer, Float> singleMachineResults = SingleMachineRaptorJoin.join(rasterPath, geomArray);
        System.out.println("----single machine results");
        System.out.println("----min: " + singleMachineResults._1());
        System.out.println("----max: " + singleMachineResults._2());

        // write result to json object
        PrintWriter resWriter = response.getWriter();
        ObjectMapper mapper = new ObjectMapper();

        // create query node
        ObjectNode queryNode = mapper.createObjectNode();
        queryNode.put("soildepth", soilDepth);
        queryNode.put("layer", layer);

        // create results node
        ObjectNode resultsNode = mapper.createObjectNode();
        resultsNode.put("max", singleMachineResults._1());
        resultsNode.put("min", singleMachineResults._2());
        resultsNode.put("sum", singleMachineResults._3());
        resultsNode.put("median", singleMachineResults._4());
        resultsNode.put("stddev", singleMachineResults._5());
        resultsNode.put("count", singleMachineResults._6());
        resultsNode.put("mean", singleMachineResults._7());

        // create root node
        // contains queryNode and resultsNode
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.set("query", queryNode);
        rootNode.set("results", resultsNode);

        // write values to response writer
        String jsonString = mapper.writer().writeValueAsString(rootNode);
        resWriter.print(jsonString);
        resWriter.flush();

        // time at end of POST request
        long t2 = System.nanoTime();

        // print out statistics
        System.out.println("----request duration:");
        System.out.println((t2 - t1) * 1e-9);
    }
}