# Java Patterns and Standards

Java-specific guidelines for the OpenSearch SQL project.

## JDK Requirements

### Version Requirements
- **Development**: JDK 21 required
- **Runtime**: JDK 21 required
- **Compatibility**: Maintain compatibility with Java 11 where possible for OpenSearch 2.x

## Naming Conventions

### Classes
- **PascalCase** for class names
- Descriptive, noun-based names
- Examples: `QueryExecutor`, `PPLParser`, `CalciteRelNodeVisitor`

### Methods and Variables
- **camelCase** for methods and variables
- Verb-based method names
- Examples: `executeQuery()`, `parseExpression()`, `buildRelNode()`

### Constants
- **UPPER_SNAKE_CASE** for constants
- Examples: `MAX_RETRY_COUNT`, `DEFAULT_TIMEOUT_MS`

### Packages
- **lowercase** with dots
- Examples: `org.opensearch.sql.ppl`, `org.opensearch.sql.calcite`

## Project Structure Patterns

### Module Organization
```
core/          - Core SQL/PPL functionality, shared components
ppl/           - PPL (Piped Processing Language) implementation
sql/           - SQL specific implementation
opensearch/    - OpenSearch storage engine integration
integ-test/    - Integration tests
common/        - Shared utilities
```

## Code Style
Code style is verified by spotless and no need to be reviewed.
