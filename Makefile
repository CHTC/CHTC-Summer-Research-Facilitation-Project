# HTCondor Cluster Analysis Tools - Demo Makefile
# Usage: make <target>
# Example: make demo CLUSTER=12345

.PHONY: help demo fetch health analytics histogram dashboard hold_bucket summarise clean all

# Default cluster ID (override with: make demo CLUSTER=your_id)
CLUSTER ?= 12345

# Color output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
CYAN=\033[0;36m
BOLD=\033[1m
NC=\033[0m # No Color

# Help target
help:
	@echo ""
	@echo "$(CYAN)$(BOLD)HTCondor Cluster Analysis Tools$(NC)"
	@echo "$(CYAN)=================================$(NC)"
	@echo ""
	@echo "$(BOLD)Usage:$(NC)"
	@echo "  make <target> CLUSTER=<cluster_id>"
	@echo ""
	@echo "$(BOLD)Available Targets:$(NC)"
	@echo "  $(GREEN)help$(NC)           - Show this help message"
	@echo "  $(GREEN)demo$(NC)           - Run full demo of all tools (requires CLUSTER)"
	@echo "  $(GREEN)fetch$(NC)          - Fetch cluster data from HTCondor"
	@echo "  $(GREEN)health$(NC)         - Run cluster health check"
	@echo "  $(GREEN)analytics$(NC)      - Run detailed resource analysis"
	@echo "  $(GREEN)histogram$(NC)      - Show runtime distribution"
	@echo "  $(GREEN)dashboard$(NC)      - Display live job status"
	@echo "  $(GREEN)hold_bucket$(NC)    - Analyze held jobs"
	@echo "  $(GREEN)summarise$(NC)      - Show job parameter summary"
	@echo "  $(GREEN)clean$(NC)          - Remove generated data files"
	@echo "  $(GREEN)all$(NC)            - Run all analysis tools (after fetch)"
	@echo ""
	@echo "$(BOLD)Examples:$(NC)"
	@echo "  make demo CLUSTER=12345"
	@echo "  make fetch CLUSTER=12345"
	@echo "  make health CLUSTER=12345"
	@echo "  make analytics CLUSTER=12345"
	@echo ""
	@echo "$(BOLD)Workflow:$(NC)"
	@echo "  1. $(CYAN)make fetch CLUSTER=xxx$(NC)   - Get data from HTCondor"
	@echo "  2. $(CYAN)make health CLUSTER=xxx$(NC)  - Quick health overview"
	@echo "  3. $(CYAN)make analytics CLUSTER=xxx$(NC) - Deep dive into issues"
	@echo ""

# Full demo - runs all tools in sequence
demo:
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════════$(NC)"
	@echo "$(CYAN)$(BOLD)  HTCondor Cluster Analysis Tools - Full Demo$(NC)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(YELLOW)Cluster ID: $(CLUSTER)$(NC)"
	@echo ""
	@echo "$(BOLD)Step 1/6: Fetching cluster data...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python fetch_cluster_data.py $(CLUSTER)
	@echo ""
	@echo "$(GREEN)✓ Data fetch complete!$(NC)"
	@echo ""
	@read -p "Press Enter to continue to health check..." dummy
	@echo ""
	@echo "$(BOLD)Step 2/6: Running health check...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python cluster_health.py $(CLUSTER)
	@echo ""
	@read -p "Press Enter to continue to detailed analytics..." dummy
	@echo ""
	@echo "$(BOLD)Step 3/6: Running detailed resource analytics...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python analytics.py $(CLUSTER)
	@echo ""
	@read -p "Press Enter to continue to runtime histogram..." dummy
	@echo ""
	@echo "$(BOLD)Step 4/6: Generating runtime histogram...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python histogram.py $(CLUSTER)
	@echo ""
	@read -p "Press Enter to continue to job dashboard..." dummy
	@echo ""
	@echo "$(BOLD)Step 5/6: Displaying job status dashboard...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python dashboard.py $(CLUSTER)
	@echo ""
	@read -p "Press Enter to continue to held jobs analysis..." dummy
	@echo ""
	@echo "$(BOLD)Step 6/6: Analyzing held jobs...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python hold_bucket.py $(CLUSTER)
	@echo ""
	@echo "$(GREEN)$(BOLD)✓ Demo complete!$(NC)"
	@echo ""
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""

# Individual tool targets
fetch:
	@echo "$(CYAN)Fetching cluster data for Cluster $(CLUSTER)...$(NC)"
	@python fetch_cluster_data.py $(CLUSTER)
	@echo "$(GREEN)✓ Done!$(NC)"

health:
	@echo "$(CYAN)Running health check for Cluster $(CLUSTER)...$(NC)"
	@python cluster_health.py $(CLUSTER)

analytics:
	@echo "$(CYAN)Running detailed analytics for Cluster $(CLUSTER)...$(NC)"
	@python analytics.py $(CLUSTER)

histogram:
	@echo "$(CYAN)Generating runtime histogram for Cluster $(CLUSTER)...$(NC)"
	@python histogram.py $(CLUSTER)

dashboard:
	@echo "$(CYAN)Displaying dashboard for Cluster $(CLUSTER)...$(NC)"
	@python dashboard.py $(CLUSTER)

hold_bucket:
	@echo "$(CYAN)Analyzing held jobs for Cluster $(CLUSTER)...$(NC)"
	@python hold_bucket.py $(CLUSTER)

summarise:
	@echo "$(CYAN)Showing job summary for Cluster $(CLUSTER)...$(NC)"
	@python summarise.py $(CLUSTER)

# Quick workflow - just health + analytics
quick:
	@echo "$(CYAN)$(BOLD)Quick Analysis for Cluster $(CLUSTER)$(NC)"
	@echo ""
	@make fetch CLUSTER=$(CLUSTER)
	@echo ""
	@make health CLUSTER=$(CLUSTER)
	@echo ""
	@echo "$(YELLOW)Run 'make analytics CLUSTER=$(CLUSTER)' for detailed analysis$(NC)"

# Run all analysis tools (assumes data already fetched)
all:
	@echo "$(CYAN)$(BOLD)Running all analysis tools for Cluster $(CLUSTER)$(NC)"
	@echo ""
	@make health CLUSTER=$(CLUSTER)
	@echo ""
	@make analytics CLUSTER=$(CLUSTER)
	@echo ""
	@make histogram CLUSTER=$(CLUSTER)
	@echo ""
	@make dashboard CLUSTER=$(CLUSTER)
	@echo ""
	@make hold_bucket CLUSTER=$(CLUSTER)

# Clean up generated data
clean:
	@echo "$(YELLOW)Cleaning cluster data files...$(NC)"
	@rm -rf cluster_data/cluster_*.csv
	@echo "$(GREEN)✓ Clean complete!$(NC)"

# Show available clusters (if data exists)
list:
	@echo "$(CYAN)Available cluster data:$(NC)"
	@if [ -d "cluster_data" ]; then \
		ls -1 cluster_data/cluster_*.csv 2>/dev/null | sed 's/cluster_data\/cluster_//g' | sed 's/_jobs.csv//g' || echo "$(YELLOW)No cluster data found$(NC)"; \
	else \
		echo "$(YELLOW)cluster_data directory not found$(NC)"; \
	fi

# Check if required Python packages are installed
check:
	@echo "$(CYAN)Checking Python dependencies...$(NC)"
	@python -c "import pandas" 2>/dev/null && echo "$(GREEN)✓ pandas$(NC)" || echo "$(RED)✗ pandas (missing)$(NC)"
	@python -c "import numpy" 2>/dev/null && echo "$(GREEN)✓ numpy$(NC)" || echo "$(RED)✗ numpy (missing)$(NC)"
	@python -c "import tabulate" 2>/dev/null && echo "$(GREEN)✓ tabulate$(NC)" || echo "$(RED)✗ tabulate (missing)$(NC)"
	@python -c "import htcondor2" 2>/dev/null && echo "$(GREEN)✓ htcondor2$(NC)" || echo "$(RED)✗ htcondor2 (missing)$(NC)"
	@python -c "import classad" 2>/dev/null && echo "$(GREEN)✓ classad$(NC)" || echo "$(RED)✗ classad (missing)$(NC)"
	@echo ""
	@echo "$(YELLOW)If any packages are missing, run: pip install -r requirements.txt$(NC)"

# Install dependencies
install:
	@echo "$(CYAN)Installing Python dependencies...$(NC)"
	@pip install -r requirements.txt
	@echo "$(GREEN)✓ Installation complete!$(NC)"

# Advanced: Compare two clusters
compare:
	@if [ -z "$(CLUSTER2)" ]; then \
		echo "$(RED)Error: CLUSTER2 not specified$(NC)"; \
		echo "Usage: make compare CLUSTER=12345 CLUSTER2=67890"; \
		exit 1; \
	fi
	@echo "$(CYAN)$(BOLD)Comparing Clusters $(CLUSTER) and $(CLUSTER2)$(NC)"
	@echo ""
	@echo "$(BOLD)Cluster $(CLUSTER):$(NC)"
	@make health CLUSTER=$(CLUSTER)
	@echo ""
	@echo "$(BOLD)Cluster $(CLUSTER2):$(NC)"
	@make health CLUSTER=$(CLUSTER2)

# Show detailed help for a specific tool
help-analytics:
	@echo "$(CYAN)$(BOLD)Analytics Tool$(NC)"
	@echo "$(CYAN)══════════════$(NC)"
	@python analytics.py --help 2>/dev/null || echo "Provides detailed resource analysis including efficiency, waste, and recommendations"

help-histogram:
	@echo "$(CYAN)$(BOLD)Histogram Tool$(NC)"
	@echo "$(CYAN)══════════════$(NC)"
	@python histogram.py --help 2>/dev/null || echo "Displays runtime distribution and detects fast jobs (<10min)"

help-hold:
	@echo "$(CYAN)$(BOLD)Hold Bucket Tool$(NC)"
	@echo "$(CYAN)════════════════$(NC)"
	@python hold_bucket.py --help 2>/dev/null || echo "Analyzes and categorizes held jobs with detailed reasons"
