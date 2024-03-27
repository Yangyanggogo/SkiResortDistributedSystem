import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public class CreateDynamoDBTable {
  public static void main(String[] args) {
    DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
        .region(Region.US_WEST_2)
        .build();

    String tableName = "SkierActivities";

    try {
      CreateTableRequest createTableRequest = CreateTableRequest.builder()
          .tableName(tableName)
          .keySchema(
              KeySchemaElement.builder()
                  .attributeName("skierID")
                  .keyType(KeyType.HASH)
                  .build(),
              KeySchemaElement.builder()
                  .attributeName("compositeKey")
                  .keyType(KeyType.RANGE)
                  .build()
          )
          .attributeDefinitions(
              AttributeDefinition.builder()
                  .attributeName("skierID")
                  .attributeType(ScalarAttributeType.N)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("compositeKey")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("resortID")
                  .attributeType(ScalarAttributeType.N)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("dayID")
                  .attributeType(ScalarAttributeType.N)
                  .build()
          )
          .provisionedThroughput(
              ProvisionedThroughput.builder()
                  .readCapacityUnits(5L)
                  .writeCapacityUnits(50L)
                  .build()
          )
          .globalSecondaryIndexes(
              GlobalSecondaryIndex.builder()
                  .indexName("ResortDayIndex")
                  .keySchema(
                      KeySchemaElement.builder()
                          .attributeName("resortID")
                          .keyType(KeyType.HASH)
                          .build(),
                      KeySchemaElement.builder()
                          .attributeName("dayID")
                          .keyType(KeyType.RANGE)
                          .build()
                  )
                  .projection(Projection.builder()
                      .projectionType(ProjectionType.INCLUDE)
                      .nonKeyAttributes("seasonID", "skierID")
                      .build())
                  .provisionedThroughput(
                      ProvisionedThroughput.builder()
                          .readCapacityUnits(5L)
                          .writeCapacityUnits(50L)
                          .build()
                  )
                  .build()
          )
          .build();

      dynamoDbClient.createTable(createTableRequest);
      System.out.println("Table created successfully: " + tableName);
    } catch (DynamoDbException e) {
      System.err.println("Error creating table: " + e.getMessage());
      e.printStackTrace();
    }
  }
}

