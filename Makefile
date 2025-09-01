# Makefile
.PHONY: test benchmark stress clean setup test-verbose test-quiet

# 设置测试环境
setup:
	@echo "Setting up test environment..."

# 安静模式测试（减少日志输出）
test: setup
	@echo "Running functional tests (quiet mode)..."
	@go test -v -race -timeout=10m . 2>/dev/null || go test -v -race -timeout=10m .

# 详细模式测试
test-verbose: setup
	@echo "Running functional tests (verbose mode)..."
	@go test -v -race -timeout=10m .

# 快速测试（跳过长时间运行的测试）
test-smoke: setup
	@echo "Running smoke tests..."
	@go test -short -v -race -timeout=5m .

# 压力测试（单独运行）
stress: setup
	@echo "Running stress tests..."
	@go test -v -run=TestStress -timeout=30m

# 稳定性测试
stability: setup
	@echo "Running stability tests..."
	@go test -v -run=TestLongRunning -timeout=1h

# 性能基准测试
benchmark: setup
	@echo "Running benchmark tests..."
	@go test -bench=. -benchmem -benchtime=3s -timeout=20m

# 内存分析
memory-profile: setup
	@echo "Running memory profiling..."
	@go test -bench=BenchmarkMemoryUsage -benchmem -memprofile=mem.prof -benchtime=5s
	@echo "Memory profile: tests/mem.prof"

# CPU分析
cpu-profile: setup
	@echo "Running CPU profiling..."
	@go test -bench=BenchmarkPutObject -cpuprofile=cpu.prof -benchtime=10s
	@echo "CPU profile: tests/cpu.prof"

# 完整测试套件
full-test: test benchmark

# 清理
clean:
	@echo "Cleaning up..."
	@rm -rf /tmp/mindb_test_*
	@rm -rf /tmp/mindb_benchmark_*
	@rm -f tests/*.prof
	@rm -f tests/*.log

# 帮助
help:
	@echo "Available targets:"
	@echo "  test          - Run functional tests (quiet)"
	@echo "  test-verbose  - Run functional tests (verbose)"
	@echo "  test-smoke    - Run smoke tests only"
	@echo "  benchmark     - Run benchmark tests"
	@echo "  stress        - Run stress tests"
	@echo "  stability     - Run stability tests"
	@echo "  memory-profile- Generate memory profile"
	@echo "  cpu-profile   - Generate CPU profile"
	@echo "  full-test     - Run complete test suite"
	@echo "  clean         - Clean up test files"