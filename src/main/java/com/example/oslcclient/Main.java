package com.example.oslcclient;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class responsible for fetching RDF data from CodeBeamer Mock server, parsing
 * it, and updating a database (ArcadeDB) accordingly with a scheduled task.
 */
public class Main {

	 // Logger for logging events
    private static final Logger logger = Logger.getLogger(MainNew.class.getName());
    
    // Scheduler for running tasks periodically
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    // ArcadeDB endpoint and credentials
    private static final String arcadeDBUrl = "http://localhost:2480/api/v1/command/test-db";
    private static final String username = "root";
    private static final String password = "playwithdata";

    /**
     * Main method to set up and start the scheduled task for fetching and processing RDF data.
     */    
    public static void main(String[] args) {
        // Start scheduled task to run every 5 minutes
        logger.info("Starting scheduled task...");
        scheduler.scheduleAtFixedRate(() -> {
            try {
                runTask();
                logger.info("Task completed successfully.");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error in scheduled task execution", e);
            }
        }, 0, 3, TimeUnit.MINUTES);

        // A shutdown hook to cleanly stop the scheduler
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown initiated. Stopping scheduled tasks...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warning("Scheduler did not terminate within the specified time.");
                    scheduler.shutdownNow();
                }
                logger.info("Scheduler stopped successfully.");
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "Scheduler termination interrupted", e);
                Thread.currentThread().interrupt();
            }
        }));

        logger.info("Scheduled task setup complete. Task will run every 5 minutes.");
    }

    /**
     * Runs the primary task of fetching RDF data, parsing it, and updating ArcadeDB.
     */    
    private static void runTask() {
        String mockServerUrl = "https://a06218af-9740-4e68-aa6a-d0d35a857859.mock.pstmn.io/requirements";
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(mockServerUrl);
            request.setHeader("Accept", "application/rdf+xml");

            HttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode == 200) {
                // Read and parse RDF content
                InputStream content = response.getEntity().getContent();
                Model model = ModelFactory.createDefaultModel();
                model.read(content, null);

                // Define RDF properties for parsing
                Property identifierProperty = model.getProperty("http://purl.org/dc/terms/identifier");
                Property titleProperty = model.getProperty("http://purl.org/dc/terms/title");
                Property createdProperty = model.getProperty("http://purl.org/dc/terms/created");
                Property modifiedProperty = model.getProperty("http://purl.org/dc/terms/modified");
                Property statusProperty = model.getProperty("http://localhost:8080/cb/api/oslc/ns/rm#status");

                // Iterate over RDF requirements and process them
                StmtIterator iter = model.listStatements(null, RDF.type, model.getResource("http://open-services.net/ns/rm#Requirement"));

                while (iter.hasNext()) {
                    Statement stmt = iter.next();
                    Resource requirementResource = stmt.getSubject();

                    String identifier = getPropertyStringValue(requirementResource, identifierProperty);
                    String title = getPropertyStringValue(requirementResource, titleProperty);
                    String created = getPropertyStringValue(requirementResource, createdProperty);
                    String modified = getPropertyStringValue(requirementResource, modifiedProperty);
                    String status = getPropertyStringValue(requirementResource, statusProperty);

                    // Check for existing vertex in ArcadeDB with the same identifier and status
                    String existingVertexQuery = String.format("SELECT * FROM Requirement WHERE identifier='%s' AND status='New'", identifier);
                    JSONObject existingNode = getExistingVertexData(existingVertexQuery);

                    // Check if data has changed compared to the stored vertex
                    if (existingNode != null) {
                        String storedTitle = existingNode.optString("title");
                        boolean hasChanges = !normalizeText(storedTitle).equals(normalizeText(title)) ||
                                             !existingNode.optString("created").equals(created) ||
                                             !existingNode.optString("modified").equals(modified) ||
                                             !existingNode.optString("status").equals(status);

                        // Update database with new vertex and link edges if changes detected
                        if (hasChanges) {
                            String newVertexQuery = String.format(
                                "CREATE VERTEX Requirement SET identifier='%s', title='%s', created='%s', modified='%s', status='New'",
                                identifier, escapeForSql(title), created, modified
                            );
                            executeArcadeDBCommand(newVertexQuery);

                            String oldVertexRid = existingNode.getString("@rid");
                            String newVertexRid = getExistingVertexRid("identifier", identifier, "modified", modified);

                            if (!oldVertexRid.isEmpty() && !newVertexRid.isEmpty() && !newVertexRid.equals(oldVertexRid)) {
                                String linkVerticesQuery = String.format(
                                    "CREATE EDGE `updated_from` FROM %s TO %s", newVertexRid, oldVertexRid
                                );
                                executeArcadeDBCommand(linkVerticesQuery);
                                copyEdges(oldVertexRid, newVertexRid);
                            }

                            String updateOldVertexQuery = String.format(
                                "UPDATE Requirement SET status='End of life', endOfLife='%s' WHERE @rid='%s'",
                                modified, oldVertexRid
                            );
                            executeArcadeDBCommand(updateOldVertexQuery);
                        }
                    } else {
                        String createVertexQuery = String.format(
                        	// Create a new vertex if no matching record exists	
                            "CREATE VERTEX Requirement SET identifier='%s', title='%s', created='%s', modified='%s', status='%s'",
                            identifier, escapeForSql(title), created, modified, status
                        );
                        executeArcadeDBCommand(createVertexQuery);
                    }
                }

                content.close();
            } else {
                logger.warning("Failed to retrieve RDF data. HTTP Status Code: " + statusCode);
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error occurred while fetching RDF data", e);
        }
    }

    /**
     * Retrieves the string value of a given RDF property from a resource.
     */
    private static String getPropertyStringValue(Resource resource, Property property) {
        Statement stmt = resource.getProperty(property);
        if (stmt != null && stmt.getObject().isLiteral()) {
            return stmt.getString();
        }
        return "";
    }

    /**
     * Escapes text to make it SQL-safe.
     */
    private static String escapeForSql(String input) {
        return input.replace("'", "\\'");
    }

    /**
     * Normalizes text by removing HTML escape sequences and trimming whitespace.
     */
    private static String normalizeText(String text) {
        if (text == null) return "";
        return StringEscapeUtils.unescapeHtml4(text).trim();
    }

    /**
     * Executes a command in ArcadeDB using HTTP POST request.
     */
    private static void executeArcadeDBCommand(String command) {
        try {
            URI uri = new URI(arcadeDBUrl);
            URL url = uri.toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);

            // Set authorization header
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            String authHeaderValue = "Basic " + encodedAuth;
            conn.setRequestProperty("Authorization", authHeaderValue);

            // Prepare and send JSON payload
            String jsonInputString = "{\"command\": \"" + command + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            // Check response for command execution status
            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                logger.info("Command executed successfully: " + command);
            } else {
                logger.warning("Failed to execute command: " + command);
            }

            conn.disconnect();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error executing ArcadeDB command", e);
        }
    }

    /**
     * Fetches existing vertex data from ArcadeDB using a SQL query.
     */
    private static JSONObject getExistingVertexData(String query) {
        try {
            URI uri = new URI(arcadeDBUrl);
            URL url = uri.toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);

            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            String authHeaderValue = "Basic " + encodedAuth;
            conn.setRequestProperty("Authorization", authHeaderValue);

            String jsonInputString = "{\"command\": \"" + query + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder content = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                JSONObject jsonResponse = new JSONObject(content.toString());
                JSONArray resultArray = jsonResponse.optJSONArray("result");
                if (resultArray != null && resultArray.length() > 0) {
                    return resultArray.getJSONObject(0);
                }
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Copies edges between vertices in ArcadeDB to maintain relationships.
     */
    private static void copyEdges(String oldVertexRid, String newVertexRid) {
        try {
            String getOutgoingEdgesQuery = String.format(
                "SELECT @rid, @type, @out, @in FROM (SELECT expand(outE()) FROM %s) WHERE @type != 'updated_from'", oldVertexRid
            );
            String[][] outgoingEdges = getEdgesFromQuery(getOutgoingEdgesQuery);

            for (String[] edgeData : outgoingEdges) {
                String edgeType = edgeData[1];
                String targetVertexRid = edgeData[3];
                String createOutgoingEdgeQuery = String.format(
                    "CREATE EDGE %s FROM %s TO %s IF NOT EXISTS", edgeType, newVertexRid, targetVertexRid
                );
                executeArcadeDBCommand(createOutgoingEdgeQuery);
            }

            String getIncomingEdgesQuery = String.format(
                "SELECT @rid, @type, @out, @in FROM (SELECT expand(inE()) FROM %s) WHERE @type != 'updated_from'", oldVertexRid
            );
            String[][] incomingEdges = getEdgesFromQuery(getIncomingEdgesQuery);

            for (String[] edgeData : incomingEdges) {
                String edgeType = edgeData[1];
                String sourceVertexRid = edgeData[2];
                String createIncomingEdgeQuery = String.format(
                    "CREATE EDGE %s FROM %s TO %s IF NOT EXISTS", edgeType, sourceVertexRid, newVertexRid
                );
                executeArcadeDBCommand(createIncomingEdgeQuery);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes a query to retrieve edge data in ArcadeDB.
     */
    private static String[][] getEdgesFromQuery(String query) {
        List<String[]> edges = new ArrayList<>();
        try {
            URI uri = new URI(arcadeDBUrl);
            URL url = uri.toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);

            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            String authHeaderValue = "Basic " + encodedAuth;
            conn.setRequestProperty("Authorization", authHeaderValue);

            String jsonInputString = "{\"command\": \"" + query + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder content = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                edges = extractEdgeDataFromResult(content.toString());
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return edges.toArray(new String[0][0]);
    }

    /**
     * Parses and extracts edge information from a JSON response string.
     */
    private static List<String[]> extractEdgeDataFromResult(String result) {
        List<String[]> edges = new ArrayList<>();

        try {
            JSONObject jsonResponse = new JSONObject(result);
            JSONArray resultArray = jsonResponse.optJSONArray("result");

            if (resultArray == null || resultArray.length() == 0) {
                System.out.println("Result array is empty; no edges to process.");
                return edges;
            }

            for (int i = 0; i < resultArray.length(); i++) {
                JSONObject resultObject = resultArray.getJSONObject(i);
                if (resultObject.has("@rid") && resultObject.has("@type") &&
                    resultObject.has("@out") && resultObject.has("@in")) {
                    String edgeRid = resultObject.getString("@rid");
                    String edgeType = resultObject.getString("@type");
                    String outVertexRid = resultObject.getString("@out");
                    String inVertexRid = resultObject.getString("@in");
                    edges.add(new String[]{edgeRid, edgeType, outVertexRid, inVertexRid});
                }
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return edges;
    }

    /**
     * Retrieves the RID of an existing vertex using identifier and modification properties.
     */
    private static String getExistingVertexRid(String identifierProperty, String identifierValue, String modifiedProperty, String modifiedValue) {
        String query = String.format("SELECT @rid FROM Requirement WHERE %s='%s' AND %s='%s'", identifierProperty, identifierValue, modifiedProperty, modifiedValue);
        try {
            URI uri = new URI(arcadeDBUrl);
            URL url = uri.toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);

            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            String authHeaderValue = "Basic " + encodedAuth;
            conn.setRequestProperty("Authorization", authHeaderValue);

            String jsonInputString = "{\"command\": \"" + query + "\", \"language\": \"sql\"}";

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder content = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                return extractRidFromResult(content.toString());
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    /**
     * Extracts the RID (record ID) from a JSON-formatted result string.
     */
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
}
