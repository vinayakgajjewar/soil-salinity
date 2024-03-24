package edu.ucr.cs.bdlab.raptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.Files;

/**
 * Retrieves information about all the available features products.
 * The information is retrieved from a JSON file with a format similar to the following:
 *
 * <pre>
 *   [
 *     {
 *       "id": "farmland",
 *       "title": "California Farmlands",
 *       "description": "All farmlands in California",
 *       "extents": [-124.4096, 32.5343, -114.1312, 42.0095],
 *       "index_path": "data/CA_farmland/index.rtree",
 *       "viz_path": "data/CA_farmland/plot.zip"
 *     }, {
 *       ...
 *     }
 *   ]
 * </pre>
 */
public class VectorInfoServlet extends HttpServlet {

    public static final File VectorIndexFile = new File("data/vectors.json");

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");
        if (!VectorIndexFile.exists()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            PrintWriter writer = response.getWriter();
            writer.printf("{'message': 'File %s not found'}", VectorIndexFile);
            writer.close();
        } else {
            response.setStatus(HttpServletResponse.SC_OK);

            try (InputStream input = new BufferedInputStream(Files.newInputStream(VectorIndexFile.toPath()))) {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode rootNode = objectMapper.readTree(input);

                // Create a new ObjectNode and put the read JsonNode under the "vectors" attribute
                ObjectNode wrapperNode = objectMapper.createObjectNode();
                wrapperNode.set("vectors", rootNode);

                // Write the wrapper ObjectNode to the output stream
                OutputStream out = response.getOutputStream();
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(out, wrapperNode);
                out.close();
            }
        }
    }
}