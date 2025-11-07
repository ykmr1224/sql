# Test Requirements

Testing standards and expectations for the OpenSearch SQL project.

## Test Coverage Standards

### Unit Tests
- Required for all new business logic
- Test individual methods and classes in isolation
- Mock external dependencies

### Integration Tests
- Required for end-to-end scenarios
- Test cross-module functionality
- Use actual OpenSearch instances
- Verify complete workflows

### Test Organization
- Unit tests in `src/test/java` directory
- Integration tests in `integ-test/` module
- Test utilities in appropriate packages
- Follow existing test structure

## Test File Naming

### Integration Tests
- **Must end with `IT.java`** (e.g., `CalcitePPLAggregationIT.java`)
- Located in `integ-test/` module

### Unit Tests
- **Must end with `Test.java`** (e.g., `QueryExecutorTest.java`)
- Located in module `src/test/java/`

**Critical:** Files ending with `IT` require OpenSearch cluster setup and must be in `integ-test/` module.

## Test Data Management

### Test Data Files
- Located in `integ-test/src/test/resources/`
- Use JSON format for index data
- Use meaningful, realistic data

### Data Cleanup
- Clean up test resources after execution
- Avoid dependencies between test cases
- Use `@AfterEach` or `@AfterAll` for cleanup
- Don't leave test indices

### Good Test Characteristics
- **Independent**: Tests don't depend on each other
- **Repeatable**: Same results every run
- **Fast**: Quick execution (especially unit tests)
- **Clear**: Easy to understand what's being tested
- **Focused**: Test one thing at a time
