# Code Review Standards

Core expectations for code quality, organization, and maintainability in the OpenSearch SQL project.

## Clean Code Principles

### Self-Documenting Code
- Write code that explains itself through clear naming and structure
- Avoid redundant comments that merely restate what code does
- Use comments only to explain WHY, not WHAT
- Remove obvious comments that add no value

**Bad:**
```java
// Get the user name
String name = user.getName();

// Loop through items
for (Item item : items) {
    // Process the item
    processItem(item);
}
```

**Good:**
```java
String userName = user.getName();

for (Item item : items) {
    processItem(item);
}
```

### Meaningful Names
- Use descriptive variable and method names that express intent
- Prefer `calculateTotalPrice()` over `calc()` with comments
- Avoid abbreviations unless universally understood
- Use domain-specific terminology consistently

### Method Size and Responsibility
- Keep methods concise (typically under 20 lines)
- Single responsibility per method
- Extract complex logic into well-named helper methods
- Avoid deep nesting (max 3 levels)

### Dead Code Elimination
- Remove unused imports
- Delete commented-out code
- Remove unused variables and methods
- Clean up temporary debugging code

## Code Organization

### Class Structure
- Single responsibility per class
- Logical grouping of related functionality
- Clear separation of concerns
- Appropriate use of access modifiers

### Package Organization
Follow project structure:
- `core/` - Core SQL/PPL functionality and shared components
- `ppl/` - PPL (Piped Processing Language) implementation
- `sql/` - SQL specific implementation
- `opensearch/` - OpenSearch storage engine integration
- `integ-test/` - Integration tests
- `common/` - Shared utilities

### Dependency Management
- Prefer composition over inheritance
- Use interfaces to define contracts
- Minimize coupling between modules
- Avoid circular dependencies

## Documentation Requirements

### JavaDoc Standards
All public classes and methods must have JavaDoc:

```java
/**
 * Executes a PPL query against the OpenSearch index.
 *
 * @param query the PPL query string to execute
 * @param context the execution context containing index metadata
 * @return query execution result
 * @throws QueryExecutionException if query execution fails
 */
public QueryResult executeQuery(String query, ExecutionContext context) {
    // implementation
}
```

Required elements:
- Brief description of purpose
- `@param` for all parameters
- `@return` if method returns a value
- `@throws` for checked exceptions

### Complex Logic Documentation
- Document non-obvious algorithms
- Explain business logic decisions
- Reference related code or documentation
- Include examples for complex APIs

## Performance Considerations

### Object Creation
- Avoid creating unnecessary objects in loops
- Use StringBuilder for string concatenation in loops
- Consider object pooling for frequently created objects
- Lazy initialization for expensive operations

### Data Structures
- Choose appropriate collections (ArrayList vs LinkedList)
- Consider memory vs speed tradeoffs
- Use primitive types when possible
- Profile before optimizing

### Resource Management
- Use try-with-resources for AutoCloseable resources
- Close streams and connections explicitly
- Avoid resource leaks in error paths
- Consider connection pooling

## Error Handling

### Exception Usage
- Use specific exception types, not generic Exception
- Provide meaningful error messages
- Include context in exception messages
- Handle null values explicitly

**Bad:**
```java
try {
    processData(data);
} catch (Exception e) {
    throw new Exception("Error");
}
```

**Good:**
```java
try {
    processData(data);
} catch (DataProcessingException e) {
    throw new QueryExecutionException(
        "Failed to process query data for index: " + indexName, e);
}
```

### Null Handling
- Prefer Optional<T> for nullable returns
- Validate inputs at API boundaries
- Use Objects.requireNonNull() for required parameters
- Document null behavior in JavaDoc
