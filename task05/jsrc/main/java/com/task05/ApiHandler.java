package com.task05;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.RetentionSetting;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Collections;

@LambdaHandler(
    lambdaName = "api_handler",
	roleName = "api_handler-role",
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DependsOn(name = "Events", resourceType = com.syndicate.deployment.model.ResourceType.DYNAMODB_TABLE)
@EnvironmentVariables(value = {
    @EnvironmentVariable(key = "target_table", value = "${target_table}")
})


public class ApiHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
 	private final DynamoDB dynamoDB;
    private final ObjectMapper objectMapper;

    public ApiHandler() {
        this.dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
     
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        String tableName = System.getenv("target_table");
    
        try {
            // Parse the incoming request body
            Map<String, Object> requestBody = objectMapper.readValue(request.getBody(), Map.class);
    
            // Generate attributes
            String id = UUID.randomUUID().toString();
            int principalId = 10; // Change to int to match expected number type
            String createdAt = java.time.Instant.now().toString();
            Map<String, Object> body = requestBody;
    
            // Create a new item for the DynamoDB table
            Table table = dynamoDB.getTable(tableName);
            Item newItem = new Item()
                .withPrimaryKey("id", id)  // Ensure key matches schema
                .withNumber("principalId", principalId)  // Use withNumber for numerical value
                .withString("createdAt", createdAt)
                .withMap("body", body);
    
            // Save the item to the DynamoDB table
            table.putItem(newItem);
    
          
    
    
            // Prepare the response body with statusCode
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("statusCode", 201);  // Add statusCode here
            responseBody.put("id", id);
    
            // Convert the response body to JSON
            String bodyJson = convertObjectToJson(responseBody);
    
            // Set response details
            response.setStatusCode(201);
            response.setBody(bodyJson);
            response.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
        } catch (Exception e) {
            context.getLogger().log("Error saving event: " + e.getMessage());
            response.setStatusCode(500);
            response.setBody("{\"message\": \"Failed to process the request\"}");
        }
    
        return response;
    }

    private String convertObjectToJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Object cannot be converted to JSON: " + object);
        }
    }
}
