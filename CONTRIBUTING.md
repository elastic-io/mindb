# Contributing Guide

Thank you for your interest in contributing to MinDB! This document provides guidelines and information for contributors.

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

### Our Pledge

In the interest of fostering an open and welcoming environment, we as contributors and maintainers pledge to:

- Use welcoming and inclusive language
- Be respectful of differing viewpoints and experiences
- Gracefully accept constructive criticism
- Focus on what is best for the community
- Show empathy towards other community members

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When creating a bug report, include:

- **Clear title and description**
- **Steps to reproduce** the issue
- **Expected vs actual behavior**
- **Environment details** (OS, Go version, etc.)
- **Code samples** or error messages
- **Possible solutions** if you have ideas

#### Bug Report Template

```markdown
**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Actual behavior**
A clear and concise description of what actually happened.

**Environment**
- OS: [e.g. Ubuntu 20.04, macOS 12.0, Windows 11]
- Go version: [e.g. 1.21.0]
- MinDB version: [e.g. 1.0.0]

**Additional context**
Add any other context about the problem here.
```

### Suggesting Enhancements

Enhancement suggestions are welcome! Please provide:

- **Clear use case description**
- **Detailed feature specification**
- **Possible implementation approaches**
- **Impact on existing features**
- **Alternative solutions considered**

### Code Contributions

We welcome code contributions! Please follow this process:

#### Development Environment Setup

1. **Fork the repository**
   ```bash
   # Fork the repository on GitHub, then clone your fork
   git clone https://github.com/elastic-io/mindb.git
   cd mindb
   ```

2. **Set up development environment**
   ```bash
   # Ensure you have Go 1.21 or higher
   go version
   
   # Install dependencies
   go mod download
   
   # Run smoke tests to ensure everything works
   make test-smoke
   ```

3. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or for bug fixes
   git checkout -b fix/issue-description
   ```

#### Coding Standards

- **Go Code Style**: Follow standard Go code style
  ```bash
  # Format code
  go fmt ./...
  
  # Run linter
  golangci-lint run
  
  # Check with vet
  go vet ./...
  ```

- **Naming Conventions**:
  - Use camelCase for naming
  - Exported functions and types start with uppercase
  - Private functions and variables start with lowercase
  - Interface names typically end with -er

- **Comments**:
  - All exported functions, types, and variables must have comments
  - Comments should start with the name of the item being commented
  - Complex logic should have inline comments

- **Error Handling**:
  - Always check and handle errors
  - Use meaningful error messages
  - Wrap errors with context where appropriate

#### Testing Requirements

We have a comprehensive test suite to ensure code quality. All contributions must include appropriate tests.

##### Test Types

1. **Unit Tests**: Test individual functions and methods
2. **Integration Tests**: Test interactions between components
3. **Functional Tests**: Test complete feature workflows
4. **Performance Tests**: Benchmarks and performance validation
5. **Stress Tests**: High-load and boundary condition testing
6. **Stability Tests**: Long-running reliability tests

##### Running Tests

We provide a Makefile to simplify the testing process:

```bash
# View all available test commands
make help

# Quick smoke tests (recommended for development)
make test-smoke

# Full functional tests (quiet mode)
make test

# Verbose mode tests (for debugging)
make test-verbose

# Performance benchmarks
make benchmark

# Stress tests (high-load testing)
make stress

# Stability tests (long-running)
make stability

# Complete test suite
make full-test
```

##### Performance Profiling

For performance-related changes, run profiling:

```bash
# Memory profiling
make memory-profile
# View results: go tool pprof mem.prof

# CPU profiling  
make cpu-profile
# View results: go tool pprof cpu.prof
```

##### Test Coverage

We aim to maintain 85%+ test coverage:

```bash
# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

# View coverage statistics
go tool cover -func=coverage.out
```

##### Testing Best Practices

1. **Test Naming**: Use descriptive test names
   ```go
   func TestGetObject_ValidObject_ReturnsCorrectData(t *testing.T) { ... }
   func TestGetObject_NonExistentObject_ReturnsError(t *testing.T) { ... }
   ```

2. **Table-Driven Tests**: For multiple test cases
   ```go
   func TestValidateInput(t *testing.T) {
       tests := []struct {
           name    string
           input   string
           wantErr bool
       }{
           {"valid input", "test", false},
           {"empty input", "", true},
           {"invalid chars", "test@#$", true},
       }
       
       for _, tt := range tests {
           t.Run(tt.name, func(t *testing.T) {
               err := ValidateInput(tt.input)
               if (err != nil) != tt.wantErr {
                   t.Errorf("ValidateInput() error = %v, wantErr %v", err, tt.wantErr)
               }
           })
       }
   }
   ```

3. **Subtests**: Organize related tests
   ```go
   func TestStorageOperations(t *testing.T) {
       storage := setupTestStorage(t)
       defer storage.Close()
       
       t.Run("CreateBucket", func(t *testing.T) {
           // Test bucket creation
       })
       
       t.Run("PutObject", func(t *testing.T) {
           // Test object storage
       })
   }
   ```

4. **Test Helpers**: Reduce code duplication
   ```go
   func setupTestStorage(t *testing.T) (*DB, string) {
       tmpDir, err := os.MkdirTemp("", "mindb_test_*")
       require.NoError(t, err)
       
       storage, err := New(tmpDir, NewLogger())
       require.NoError(t, err)
       
       t.Cleanup(func() {
           storage.Close()
           os.RemoveAll(tmpDir)
       })
       
       return storage, tmpDir
   }
   ```

5. **Concurrent Testing**: Use race detection
   ```go
   func TestConcurrentOperations(t *testing.T) {
       // This test will automatically run with -race flag
       var wg sync.WaitGroup
       for i := 0; i < 100; i++ {
           wg.Add(1)
           go func(id int) {
               defer wg.Done()
               // Concurrent operations
           }(i)
       }
       wg.Wait()
   }
   ```

##### Benchmark Tests

Add benchmark tests for performance-critical code:

```go
func BenchmarkPutObject(b *testing.B) {
    storage := setupBenchmarkStorage(b)
    defer storage.Close()
    
    data := make([]byte, 1024) // 1KB test data
    
    b.ResetTimer()
    b.SetBytes(1024)
    
    for i := 0; i < b.N; i++ {
        obj := &ObjectData{
            Key:  fmt.Sprintf("test-object-%d", i),
            Data: data,
        }
        
        if err := storage.PutObject("test-bucket", obj); err != nil {
            b.Fatal(err)
        }
    }
}
```

##### Test Environment Cleanup

Ensure resources are cleaned up after tests:

```bash
# Clean up test files and profiling results
make clean
```

#### Pre-Commit Checklist

Before submitting code, ensure:

- [ ] Code is formatted (`go fmt ./...`)
- [ ] Passes static checks (`go vet ./...`, `golangci-lint run`)
- [ ] Smoke tests pass (`make test-smoke`)
- [ ] Full tests pass (`make test`)
- [ ] Benchmarks show no performance regression (`make benchmark`)
- [ ] Added appropriate test coverage
- [ ] Updated relevant documentation
- [ ] Commit messages are clear and descriptive

#### Continuous Integration

Our CI pipeline automatically runs:

1. **Code Quality Checks**
   - Format validation
   - Static analysis
   - Security scanning

2. **Test Suite**
   - Unit tests
   - Integration tests
   - Race detection

3. **Performance Validation**
   - Benchmark tests
   - Performance regression detection

4. **Compatibility Testing**
   - Multiple Go versions
   - Different operating systems

#### Debugging Tests

If tests fail, use these debugging methods:

```bash
# Run specific test
go test -run TestSpecificFunction -v

# Run flaky test multiple times
go test -run TestFlakyTest -count=10

# Enable verbose logging
mindb_LOG_LEVEL=debug make test-verbose

# Generate test binary for debugging
go test -c
./mindb.test -test.run TestSpecificFunction -test.v
```

#### Performance Testing Guidelines

For performance-related changes:

1. **Establish Baseline**: Run benchmarks before changes
2. **Measure Impact**: Compare performance before and after
3. **Document Results**: Include performance data in PR
4. **Consider Trade-offs**: Explain performance vs other factors

```bash
# Establish performance baseline
git checkout main
make benchmark > baseline.txt

# Test your changes
git checkout your-branch
make benchmark > changes.txt

# Compare results
benchcmp baseline.txt changes.txt
```

#### Commit Message Guidelines

Use clear, descriptive commit messages:

```
type(scope): brief description

Detailed description (if needed)

- List important changes
- Reference related issues #123
```

**Commit Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code formatting changes
- `refactor`: Code refactoring
- `test`: Adding or modifying tests
- `chore`: Build process or auxiliary tool changes

**Examples**:
```
feat(storage): add object range request support

Implement GetObjectRange method to support HTTP Range requests.
This allows clients to request specific byte ranges of objects,
useful for partial downloads of large files.

- Add GetObjectRange method
- Support start and end parameters
- Add corresponding unit tests
- Update API documentation

Fixes #45
```

#### Pull Request Process

1. **Ensure tests pass**
   ```bash
   make test
   make benchmark
   ```

2. **Update documentation** (if needed)
   - Update README.md
   - Update API documentation
   - Add code examples

3. **Create Pull Request**
   - Use clear title and description
   - Reference related issues
   - Include test results
   - Add screenshots (if applicable)

4. **Code Review**
   - Respond to review comments
   - Make necessary changes
   - Keep discussions constructive

#### Pull Request Template

```markdown
## Description
Brief description of the changes in this PR.

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Testing
Describe the tests you ran to verify your changes.

- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Benchmarks (if applicable)

## Checklist
- [ ] My code follows the style guidelines of this project
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes
```

## Code Review Process

### Pull Request Requirements

1. **Complete Test Coverage**
   - New features must have unit tests
   - Complex features need integration tests
   - Performance-critical code needs benchmarks

2. **Documentation Updates**
   - Update API documentation
   - Add usage examples
   - Update CHANGELOG.md

3. **Performance Validation**
   - Run benchmark tests
   - Ensure no performance regression
   - Include performance data

### Review Standards

Code reviews will check for:

- **Functional Correctness**: Does the code work as expected
- **Test Quality**: Are tests adequate and meaningful
- **Performance Impact**: Any performance regressions
- **Code Quality**: Following best practices
- **Documentation Completeness**: Appropriate documentation