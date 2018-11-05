package com.ice2systems.voice.servlet;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;

import java.util.logging.Logger;

@WebServlet("/resource")
@MultipartConfig(	fileSizeThreshold=1024*1024*2, // 2MB
									maxFileSize=1024*1024*10,      // 10MB
									maxRequestSize=1024*1024*50) 

public class ResourceControllerServlet extends HttpServlet {
	private static Logger myLogger = Logger.getLogger(ResourceControllerServlet.class.getName());
	
  private static final long serialVersionUID = 1L;
	private static final String bucketOut = "<voice output bucket>";
	private static final String bucketIn = "<SRT input bucket>";
  String descriptorName = "voice.ini";
	private AmazonS3 s3;
	
  public void init() {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials("<access key>", "<secret key>");
		s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();  
  }
  
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) 
        throws ServletException, IOException {
  	
  	String title = request.getParameter("title");
  	String resourceType = request.getParameter("resourceType");
  	
  	String keyName = null;
  			 
  	switch(ResourceType.valueOf(resourceType)) {
  		case srt:
  			keyName = String.format("%s/%s.json", title, title);
  			response.setContentType("application/json");
  			break;
  		case descriptor:
  			keyName = String.format("%s/%s", title, descriptorName);
  			response.setContentType("application/json");
  			break;  			
  		case media:
  	  	String name = request.getParameter("name");
  			keyName = String.format("%s/%s", title, name);
  			response.setContentType("audio/pcm");//"audio/mpeg"
  			break;
  		default:
  			return;
  	}
     
    S3Object s3Obj = s3.getObject(new GetObjectRequest(bucketOut, keyName));
    final BufferedInputStream inputStream = new BufferedInputStream(s3Obj.getObjectContent());
     
    OutputStream out = response.getOutputStream();
    byte[] buffer = new byte[4096];
    int length;
    while ((length = inputStream.read(buffer)) > 0){
       out.write(buffer, 0, length);
    }
    inputStream.close();
    out.flush(); 
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    String description = request.getParameter("description"); 
    String username = request.getParameter("username");

    boolean replace = StringUtils.isNullOrEmpty(request.getParameter("replace")) ? false : Boolean.parseBoolean(request.getParameter("replace"));
    
    myLogger.info(String.format("description=%s username=%s", description, username));

    String result = "{\"status\":\"OK\"}";
    
    try {   
	    if(StringUtils.isNullOrEmpty(username) || StringUtils.isNullOrEmpty(description)) {
	    	throw new RuntimeException("mandatory parameters username/description not provided");
	    }
	    
	    boolean processed = false;
	    
	    for (Part part : request.getParts()) {
	      String fileName = extractFileName(part);
	      
	      if(!fileName.endsWith(".srt")) {
	      	throw new RuntimeException("file has to have the \"srt\" extension");
	      }
	      
	      if(!StringUtils.isNullOrEmpty(fileName)) {
	      	myLogger.info(String.format("fileName=%s size=%d", fileName, part.getSize()));
	      	InputStream inputStream = part.getInputStream();
	    		
	    		processed = processFile(inputStream, part.getSize(), fileName, username, description, replace);
	    		break;
	      }
	    }
	    
	    if(!processed) {
	    	throw new RuntimeException("file is not provided");
	    }
    }
    catch(Exception e) {
    	result = String.format("{\"status\":\"FAILED\", \"reason\":\"%s\"}", e.getMessage());
    }
    
    PrintWriter out = response.getWriter();
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    out.print(result);
    out.flush();
  }  
  
  private boolean processFile(final InputStream inputStream, final long size, final String fileName, final String username, final String description, final boolean replace) {
  	if( s3.doesObjectExist(bucketIn, fileName) ) {
  		
  		if(!replace) {
  			throw new RuntimeException(String.format("file %s is already uploaded", fileName));
  		}
  		
  		ObjectListing objectListing = s3.listObjects(bucketOut, fileName.split(".srt")[0] + "/");
  		
  		while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        	myLogger.info(String.format("deleting %s",objectSummary.getKey()));
          s3.deleteObject(bucketOut, objectSummary.getKey());
        }
        if (objectListing.isTruncated()) {
            objectListing = s3.listNextBatchOfObjects(objectListing);
        } else {
            break;
        }
  		}
  	}
  	
  	ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(size);
		metadata.addUserMetadata("username", username);
		
		if(!StringUtils.isNullOrEmpty(description)) {
			metadata.addUserMetadata("description", description);
		}
		
		s3.putObject(new PutObjectRequest(bucketIn, fileName, inputStream, metadata));	
		
  	return true;
  }
  
  private String extractFileName(Part part) {
    String contentDisp = part.getHeader("content-disposition");
    String[] items = contentDisp.split(";");
    for (String s : items) {
        if (s.trim().startsWith("filename")) {
            return s.substring(s.indexOf("=") + 2, s.length()-1);
        }
    }
    return null;
}  
}
