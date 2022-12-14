package edu.ucr.cs.bdlab.raptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import scala.Tuple7;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Computes soil statistics for a single polygon defined in GeoJSON format.
 */
public class SinglePolygonServlet extends HttpServlet {

    // post method
    // An alias to the get method for browsers and SDKs that do not support a payload in the GET request, e.g., iPhone
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        doGet(request, response);
    }

    // get method
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

        // time at start of POST request
        long t1 = System.nanoTime();

        // sidebar select parameters
        String soilDepth = "";
        String layer = "";

        // try getting parameters from url
        try {
            soilDepth = request.getParameter("soildepth");
            layer = request.getParameter("layer");
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

        // load raster data based on selected soil depth and layer
        List<String> matchingRasterFiles = new ArrayList<>();
        for (Map.Entry<String, String> rasterFile : SoilServlet.rasterFiles.entrySet()) {
            if (SoilServlet.rangeOverlap(rasterFile.getKey(), soilDepth))
                matchingRasterFiles.add(String.format("data/tif/%s/%s.tif", rasterFile.getValue(), layer));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ServletInputStream input = request.getInputStream();
        IOUtils.copy(input, baos);
        input.close();
        baos.close();

        // initialize geojson reader
        GeoJsonReader reader = new GeoJsonReader(new GeometryFactory());

        // try reading the geojson string into a geometry object
        Geometry geom = null;
        String geometryGeoJSON = baos.toString();
        try {
            geom = reader.read(geometryGeoJSON);
            geom.setSRID(4326);
        } catch (ParseException e) {
            System.err.println("----ERROR: could not parse geojson string "+geometryGeoJSON);
            e.printStackTrace();
        }

        Geometry[] geomArray = {geom};

        // now that we have a geometry object
        // call single machine raptor join
        Tuple7<Float, Float, Float, Float, Float, Integer, Float> singleMachineResults =
            SingleMachineRaptorJoin.join(matchingRasterFiles.toArray(new String[0]), geomArray);

        // write result to json object
        PrintWriter resWriter = response.getWriter();
        ObjectMapper mapper = new ObjectMapper();

        // create query node
        ObjectNode queryNode = mapper.createObjectNode();
        queryNode.put("soildepth", soilDepth);
        queryNode.put("layer", layer);

        // create results node
        ObjectNode resultsNode = mapper.createObjectNode();
        if (singleMachineResults != null) {
            resultsNode.put("max", singleMachineResults._1());
            resultsNode.put("min", singleMachineResults._2());
            resultsNode.put("sum", singleMachineResults._3());
            resultsNode.put("median", singleMachineResults._4());
            resultsNode.put("stddev", singleMachineResults._5());
            resultsNode.put("count", singleMachineResults._6());
            resultsNode.put("mean", singleMachineResults._7());
        }

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