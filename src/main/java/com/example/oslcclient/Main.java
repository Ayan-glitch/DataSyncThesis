package com.example.oslcclient;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URI;
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

                    // Check if the requirement exists in ArcadeDB
                    String existingVertexQuery = String.format("SELECT FROM Requirement WHERE identifier='%s'", identifier);
                    boolean exists = checkVertexExists(existingVertexQuery);

                    if (exists) {
                        // Compare modified timestamp to see if the data has changed
                        String existingModified = getExistingVertexProperty("modified", identifier);

                        if (!existingModified.equals(modified)) {
                            // Create a new vertex for the updated data
                            String newVertexQuery = String.format(
                                "CREATE VERTEX Requirement SET identifier='%s', title='%s', created='%s', modified='%s', status='Active'",
                                identifier, title, created, modified
                            );
                            executeArcadeDBCommand(newVertexQuery);

                            // Fetch the @rid for the old and new vertices
                            String oldVertexRid = getExistingVertexRid("identifier", identifier, "modified", existingModified);
                            String newVertexRid = getExistingVertexRid("identifier", identifier, "modified", modified);

                            // Create an edge linking the new vertex to the old one with the label "updated from"
                            if (!oldVertexRid.isEmpty() && !newVertexRid.isEmpty()) {
                                String linkVerticesQuery = String.format(
                                    "CREATE EDGE `updated_from` FROM %s TO %s",  newVertexRid, oldVertexRid
                                );
                                executeArcadeDBCommand(linkVerticesQuery);
                            }

                            // Mark the old vertex as "End of life"
                            String updateOldVertexQuery = String.format(
                                "UPDATE Requirement SET status='End of life' WHERE identifier='%s' AND modified='%s'",
                                identifier, existingModified
                            );
                            executeArcadeDBCommand(updateOldVertexQuery);
                        }
                    } else {
                        // Insert a new vertex as this requirement does not exist
                        String createVertexQuery = String.format(
                            "CREATE VERTEX Requirement SET identifier='%s', title='%s', created='%s', modified='%s', status='%s'",
                            identifier, title, created, modified, status
                        );
                        executeArcadeDBCommand(createVertexQuery);
                    }
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

    // Check if a vertex with a given identifier already exists in ArcadeDB
    private static boolean checkVertexExists(String query) {
        try {
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

            // Format query into JSON body
            String jsonInputString = "{\"command\": \"" + query + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                
                // Check if the result contains any vertices
                return content.toString().contains("@rid");
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }
    
    // Get the @rid of a vertex based on identifier and modified values
    private static String getExistingVertexRid(String identifierProperty, String identifierValue, String modifiedProperty, String modifiedValue) {
        String query = String.format("SELECT @rid FROM Requirement WHERE %s='%s' AND %s='%s'", identifierProperty, identifierValue, modifiedProperty, modifiedValue);
        try {
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

            // Format query into JSON body
            String jsonInputString = "{\"command\": \"" + query + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                // Extract the @rid from the JSON result
                return extractRidFromResult(content.toString());
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    // Helper function to extract @rid from ArcadeDB JSON response
    private static String extractRidFromResult(String result) {
        String ridPattern = "\"@rid\":\"";
        int startIndex = result.indexOf(ridPattern);
        if (startIndex != -1) {
            startIndex += ridPattern.length();
            int endIndex = result.indexOf("\"", startIndex);
            return result.substring(startIndex, endIndex);
        }
        return "";
    }


    // Get an existing vertex property (e.g., modified date) for a given identifier
    private static String getExistingVertexProperty(String property, String identifier) {
        String query = String.format("SELECT %s FROM Requirement WHERE identifier='%s'", property, identifier);
        try {
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

            // Format query into JSON body
            String jsonInputString = "{\"command\": \"" + query + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                // Extract the "modified" field value from the JSON result
                return extractFieldValueFromResult(content.toString(), property);
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    // A helper method to extract the field value from the JSON result
    private static String extractFieldValueFromResult(String result, String fieldName) {
        String fieldPattern = "\"" + fieldName + "\":\"";
        int startIndex = result.indexOf(fieldPattern);
        if (startIndex != -1) {
            startIndex += fieldPattern.length();
            int endIndex = result.indexOf("\"", startIndex);
            return result.substring(startIndex, endIndex);
        }
        return "";
    }
}
