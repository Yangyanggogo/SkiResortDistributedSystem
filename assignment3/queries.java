// 1. For skier N, how many days have they skied this season?

String skierId = "N"; // Replace N with the actual skier ID
String seasonId = "some_season_id"; // Replace with the actual season ID

Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
expressionAttributeValues.put(":skierId", AttributeValue.builder().n(skierId).build());
expressionAttributeValues.put(":seasonId", AttributeValue.builder().s(seasonId + "#").build()); // Assuming seasonID is the start of the compositeKey

QueryRequest queryRequest = QueryRequest.builder()
        .tableName("SkierActivities")
        .keyConditionExpression("skierID = :skierId and begins_with(compositeKey, :seasonId)")
        .expressionAttributeValues(expressionAttributeValues)
        .build();

QueryResponse response = dynamoDbClient.query(queryRequest);
Set<String> uniqueDays = response.items().stream()
        .map(item -> item.get("compositeKey").s().split("#")[1]) // Assuming dayID is the second part of the compositeKey
        .collect(Collectors.toSet());

int daysSkied = uniqueDays.size();


// 2. For skier N, what are the vertical totals for each ski day? (calculate vertical as liftID*10)

String skierId = "N"; // Replace N with the actual skier ID

Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
expressionAttributeValues.put(":skierId", AttributeValue.builder().n(skierId).build());

QueryRequest queryRequest = QueryRequest.builder()
        .tableName("SkierActivities")
        .keyConditionExpression("skierID = :skierId")
        .expressionAttributeValues(expressionAttributeValues)
        .build();

QueryResponse response = dynamoDbClient.query(queryRequest);
Map<String, Integer> verticalTotalsPerDay = new HashMap<>();

response.items().forEach(item -> {
    String[] parts = item.get("compositeKey").s().split("#");
    String dayId = parts[1];
    int liftId = Integer.parseInt(parts[3]); // Assuming liftID is the fourth part of the compositeKey
    int vertical = liftId * 10;
    
    verticalTotalsPerDay.merge(dayId, vertical, Integer::sum);
});


// 3. For skier N, show me the lifts they rode on each ski day

// Reuse the queryRequest and response from the above example

Map<String, Set<String>> liftsPerDay = new HashMap<>();

response.items().forEach(item -> {
    String[] parts = item.get("compositeKey").s().split("#");
    String dayId = parts[1];
    String liftId = parts[3]; // Assuming liftID is the fourth part of the compositeKey
    
    liftsPerDay.computeIfAbsent(dayId, k -> new HashSet<>()).add(liftId);
});


// 4. How many unique skiers visited resort X on day N?

String resortId = "X"; // Replace X with the actual resort ID
String dayId = "N"; // Replace N with the actual day ID

Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
expressionAttributeValues.put(":resortId", AttributeValue.builder().n(resortId).build());
expressionAttributeValues.put(":dayId", AttributeValue.builder().s(dayId).build());

QueryRequest queryRequest = QueryRequest.builder()
        .tableName("SkierActivities")
        .indexName("ResortDayIndex")
        .keyConditionExpression("resortID = :resortId and dayID = :dayId")
        .expressionAttributeValues(expressionAttributeValues)
        .build();

QueryResponse response = dynamoDbClient.query(queryRequest);
Set<String> uniqueSkiers = response.items().stream()
        .map(item -> item.get("skierID").n())
        .collect(Collectors.toSet());

int uniqueSkierCount = uniqueSkiers.size();

