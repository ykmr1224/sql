# Amazon Q Developer Code Review

This directory contains project-specific rules and guidelines for Amazon Q Developer code reviews.

## Overview

Amazon Q Developer provides AI-powered code reviews that analyze:
- **Security vulnerabilities** (SAST scanning)
- **Secrets detection** (hardcoded credentials, API keys)
- **Code quality** (performance, maintainability, best practices)
- **Infrastructure as Code** (security and compliance)
- **Third-party dependencies** (vulnerability scanning)

## How to Use

### Requesting a Code Review

In your GitHub Pull Request, comment:
```
/q review
```

Amazon Q will analyze your PR changes and provide feedback as comments.

### Review Scope Options

- `/q review` - Review all changes in the PR
- `/q review path/to/file.java` - Review specific file
- `/q review --full` - Full project scan (use sparingly)

### Interpreting Results

Amazon Q provides findings with:
- **Severity levels**: Critical, High, Medium, Low, Info
- **Issue type**: Security, Quality, Best Practice, etc.
- **Remediation guidance**: Specific suggestions to fix issues
- **Code references**: Exact locations of issues

## Project Rules

Our project-specific rules are located in the `rules/` directory:

### [code-review-standards.md](rules/code-review-standards.md)
Core code review expectations including:
- Clean code principles
- Code organization patterns
- Performance considerations
- Documentation requirements

### [java-patterns.md](rules/java-patterns.md)
Java-specific guidelines including:
- JDK 21 requirements
- Naming conventions
- OpenSearch project patterns
- Module organization

### [security-guidelines.md](rules/security-guidelines.md)
Security review focus areas including:
- Input validation
- Injection prevention
- Secrets management
- Dependency security

### [test-requirements.md](rules/test-requirements.md)
Testing standards including:
- Test coverage expectations
- Integration test patterns
- Test naming conventions
- Test execution commands

### [response-format.md](rules/response-format.md)
Amazon Q response format:
- Review summary format
- Feedback collection via emoji reactions

## Best Practices

### When to Request Reviews

- **Before requesting PR review**: Catch issues early
- **After addressing feedback**: Verify fixes are correct
- **For security-sensitive changes**: Extra scrutiny on auth, data handling
- **For complex refactoring**: Ensure quality is maintained

### Addressing Findings

1. **Review all findings**: Don't dismiss without understanding
2. **Prioritize by severity**: Address Critical/High issues first
3. **Ask questions**: Use PR comments if guidance is unclear
4. **Document decisions**: If you disagree with a finding, explain why
5. **Re-review after fixes**: Verify issues are resolved

### Common Issues to Watch For

Based on our project standards:
- Redundant comments (code should be self-documenting)
- Missing JavaDoc on public methods
- Inconsistent naming conventions
- Missing input validation
- Hardcoded credentials or secrets
- Unused imports or dead code
- Missing test coverage
- Integration tests not following `*IT.java` naming

## Integration with Existing Tools

Amazon Q complements our existing tools:
- **CodeQL**: Continues to run for security scanning
- **Gradle**: Build and test execution remains unchanged
- **Pre-commit hooks**: Still enforced
- **Manual reviews**: Human review is still essential

## Limitations

- **Context window**: Large PRs may need to be reviewed in parts
- **False positives**: AI may flag valid patterns; use judgment
- **Project-specific patterns**: May not catch all custom conventions
- **Language support**: Best for Java; limited for other languages

## Feedback and Improvements

If you encounter:
- **False positives**: Document in PR comments
- **Missed issues**: Share examples for rule refinement
- **Unclear guidance**: Request clarification in PR
- **Rule suggestions**: Propose updates to rules/ files

## Resources

- [Amazon Q Code Reviews Documentation](https://docs.aws.amazon.com/amazonq/latest/qdeveloper-ug/code-reviews.html)
- [Amazon Q Detector Library](https://docs.aws.amazon.com/codeguru/detector-library/)
- [Project .clinerules/](../.clinerules/) - Development standards
- [Feature Documentation](../memory-bank/features/amazon-q-code-review-setup.md)

## Support

For questions or issues:
1. Check this README and rule files
2. Review Amazon Q documentation
3. Ask in team chat or PR comments
4. Escalate to project maintainers if needed
