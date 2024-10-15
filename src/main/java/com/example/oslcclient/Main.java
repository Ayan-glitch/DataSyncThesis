package com.example.oslcclient;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.vocabulary.RDF;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URI;
import java.io.OutputStream;
import java.util.Base64;

public class Main {

    private static String arcadeDBUrl = "http://localhost:2480/api/v1/command/test-db";
    private static String username = "root"; 
    private static String password = "playwithdata";  

    public static void main(String[] args) {

        String mockServerUrl = "https://a06218af-9740-4e68-aa6a-d0d35a857859.mock.pstmn.io/requirements";

        // Create HTTP client to retrieve RDF data
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

            // Create HTTP GET request to fetch the RDF data
            HttpGet request = new HttpGet(mockServerUrl);
            request.setHeader("Accept", "application/rdf+xml");

            // Execute the HTTP request and get the response
            HttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode == 200) {
                // Get RDF content from the response
                InputStream content = response.getEntity().getContent();

                // Parse RDF using Jena
                Model model = ModelFactory.createDefaultModel();
                model.read(content, null);

                // Define properties to extract
                Property identifierProperty = model.getProperty("http://purl.org/dc/terms/identifier");
                Property titleProperty = model.getProperty("http://purl.org/dc/terms/title");
                Property createdProperty = model.getProperty("http://purl.org/dc/terms/created");
                Property modifiedProperty = model.getProperty("http://purl.org/dc/terms/modified");
                Property statusProperty = model.getProperty("http://localhost:8080/cb/api/oslc/ns/rm#status");

                // Iterate through the RDF data and extract each Requirement
                StmtIterator iter = model.listStatements(null, RDF.type, model.getResource("http://open-services.net/ns/rm#Requirement"));

                while (iter.hasNext()) {
                    Statement stmt = iter.next();
                    Resource requirementResource = stmt.getSubject();  // The Requirement resource

                    // Extract the required properties from each Requirement
                    String identifier = getPropertyStringValue(requirementResource, identifierProperty);
                    String title = getPropertyStringValue(requirementResource, titleProperty);
                    String created = getPropertyStringValue(requirementResource, createdProperty);
                    String modified = getPropertyStringValue(requirementResource, modifiedProperty);
                    String status = getPropertyStringValue(requirementResource, statusProperty);

                    // Log or print the extracted values
                    System.out.println("Identifier: " + identifier);
                    System.out.println("Title: " + title);
                    System.out.println("Created: " + created);
                    System.out.println("Modified: " + modified);
                    System.out.println("Status: " + status);
                    System.out.println("----------------------");

                    // Insert the extracted Requirement as a vertex in ArcadeDB
                    String createVertexQuery = String.format(
                        "CREATE VERTEX Requirement SET identifier='%s', title='%s', created='%s', modified='%s', status='%s'",
                        identifier, title, created, modified, status
                    );
                    executeArcadeDBCommand(createVertexQuery);
                }

                content.close();
            } else {
                System.err.println("Failed to retrieve RDF data. HTTP Status Code: " + statusCode);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Function to get the string value of a property from a Resource
    private static String getPropertyStringValue(Resource resource, Property property) {
        Statement stmt = resource.getProperty(property);
        if (stmt != null && stmt.getObject().isLiteral()) {
            return stmt.getString();
        }
        return "";
    }
 // Function to send an HTTP POST request to ArcadeDB to execute an SQL command
    private static void executeArcadeDBCommand(String command) {
        try {
            // First, ensure the 'Requirement' vertex type exists
            createVertexTypeIfNotExists("Requirement");

            // Then, execute the original command
            URI uri = new URI(arcadeDBUrl);
            URL url = uri.toURL();  // Convert URI to URL
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);

            // Add Basic Authentication header
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            String authHeaderValue = "Basic " + encodedAuth;
            conn.setRequestProperty("Authorization", authHeaderValue);

            // Format command into JSON body
            String jsonInputString = "{\"command\": \"" + command + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                System.out.println("Command executed: " + command);
            } else {
                System.out.println("Failed to execute command: " + command);
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Create the vertex type if it doesn't already exist
    private static void createVertexTypeIfNotExists(String vertexType) {
        try {
            String checkTypeCommand = "CREATE VERTEX TYPE IF NOT EXISTS " + vertexType;
            URI uri = new URI(arcadeDBUrl);
            URL url = uri.toURL();  // Convert URI to URL
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);

            // Add Basic Authentication header
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            String authHeaderValue = "Basic " + encodedAuth;
            conn.setRequestProperty("Authorization", authHeaderValue);

            // Format command into JSON body
            String jsonInputString = "{\"command\": \"" + checkTypeCommand + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            conn.getResponseCode(); // Force execution
            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
