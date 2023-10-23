package edu.ucr.cs.bdlab.raptor;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.indexing.RTreeFeatureReader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class VectorInfoServlet extends HttpServlet {
    Configuration hadoopConf = new Configuration();
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // time at start of GET request
        long t1 = System.nanoTime();

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");
        

        FileSystem fs;
        try {
            fs = FileSystem.get(hadoopConf);

            String hdfsDirectoryPath = "hdfs:///tmp";
            RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(new Path(hdfsDirectoryPath), true);

            while (fileStatusIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusIterator.next();
                if (fileStatus.isFile()) {
                    final Path filePath = fileStatus.getPath();
            RTreeFeatureReader reader = new RTreeFeatureReader();
            Path indexPath = new Path(filePath);
            FileSystem fs = indexPath.getFileSystem(hadoopConf);
            long fileLength = fs.getFileStatus(indexPath).getLen();
            FileSplit inputFileSplit = new FileSplit(indexPath, 0, fileLength, null);
            BeastOptions opts = new BeastOptions();
            PrintWriter out = response.getWriter();
            ObjectMapper mapper = new ObjectMapper();        
            reader.close();
                }
            }


        // Load the Vector-layer features
    }   


}