# HTCondor Cluster Analysis Tools
# Usage: make <target> CLUSTER=<cluster_id>
# Example: make demo CLUSTER=12345

.PHONY: help demo fetch fetch-es status runtimes resources held summary clean all quick list check install compare

# Default cluster ID (override with: make demo CLUSTER=your_id)
CLUSTER ?= 12345

# Color output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
CYAN=\033[0;36m
BOLD=\033[1m
NC=\033[0m

# ─────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────

help:
	@echo ""
	@echo "$(CYAN)$(BOLD)HTCondor Cluster Analysis Tools$(NC)"
	@echo "$(CYAN)=================================$(NC)"
	@echo ""
	@echo "$(BOLD)Usage:$(NC)"
	@echo "  make <target> CLUSTER=<cluster_id>"
	@echo ""
	@echo "$(BOLD)Data Fetch:$(NC)"
	@echo "  $(GREEN)fetch$(NC)          - Fetch cluster data from HTCondor schedd"
	@echo "  $(GREEN)fetch-es$(NC)       - Fetch cluster data from Elasticsearch"
	@echo ""
	@echo "$(BOLD)Analysis:$(NC)"
	@echo "  $(GREEN)summary$(NC)        - Aggregated cluster health report (good starting point)"
	@echo "  $(GREEN)resources$(NC)      - Detailed CPU / memory / disk utilisation"
	@echo "  $(GREEN)runtimes$(NC)       - Runtime distribution histogram and scatter plot"
	@echo "  $(GREEN)status$(NC)         - Live job status bar chart"
	@echo "  $(GREEN)held$(NC)           - Held job analysis and bucketing"
	@echo ""
	@echo "$(BOLD)Workflows:$(NC)"
	@echo "  $(GREEN)demo$(NC)           - Run all tools in sequence with prompts"
	@echo "  $(GREEN)quick$(NC)          - Fetch + summary only"
	@echo "  $(GREEN)all$(NC)            - Run all analysis tools (assumes data already fetched)"
	@echo "  $(GREEN)compare$(NC)        - Compare two clusters (requires CLUSTER and CLUSTER2)"
	@echo ""
	@echo "$(BOLD)Utilities:$(NC)"
	@echo "  $(GREEN)list$(NC)           - Show locally cached cluster IDs"
	@echo "  $(GREEN)check$(NC)          - Verify Python dependencies are installed"
	@echo "  $(GREEN)install$(NC)        - Install dependencies from requirements.txt"
	@echo "  $(GREEN)clean$(NC)          - Remove cached cluster CSV files"
	@echo ""
	@echo "$(BOLD)Examples:$(NC)"
	@echo "  make demo CLUSTER=12345"
	@echo "  make fetch CLUSTER=12345"
	@echo "  make summary CLUSTER=12345"
	@echo "  make resources CLUSTER=12345"
	@echo "  make compare CLUSTER=12345 CLUSTER2=67890"
	@echo ""
	@echo "$(BOLD)Typical workflow:$(NC)"
	@echo "  1. $(CYAN)make fetch CLUSTER=xxx$(NC)     - Fetch and cache data"
	@echo "  2. $(CYAN)make summary CLUSTER=xxx$(NC)   - Quick health overview"
	@echo "  3. $(CYAN)make resources CLUSTER=xxx$(NC) - Deep dive into issues"
	@echo ""

# ─────────────────────────────────────────────
# Data fetch
# ─────────────────────────────────────────────

fetch:
	@echo "$(CYAN)Fetching cluster data for Cluster $(CLUSTER) from HTCondor...$(NC)"
	@python fetch_cluster_data.py $(CLUSTER)
	@echo "$(GREEN)✓ Done!$(NC)"

fetch-es:
	@echo "$(CYAN)Fetching cluster data for Cluster $(CLUSTER) from Elasticsearch...$(NC)"
	@python query.py $(CLUSTER)
	@echo "$(GREEN)✓ Done!$(NC)"

# ─────────────────────────────────────────────
# Analysis subcommands
# ─────────────────────────────────────────────

summary:
	@echo "$(CYAN)Running health summary for Cluster $(CLUSTER)...$(NC)"
	@python main.py summary $(CLUSTER)

resources:
	@echo "$(CYAN)Running resource analysis for Cluster $(CLUSTER)...$(NC)"
	@python main.py resources $(CLUSTER)

runtimes:
	@echo "$(CYAN)Generating runtime histogram for Cluster $(CLUSTER)...$(NC)"
	@python main.py runtimes $(CLUSTER)

status:
	@echo "$(CYAN)Displaying job status for Cluster $(CLUSTER)...$(NC)"
	@python main.py status $(CLUSTER)

held:
	@echo "$(CYAN)Analyzing held jobs for Cluster $(CLUSTER)...$(NC)"
	@python main.py held $(CLUSTER)

# ─────────────────────────────────────────────
# Workflows
# ─────────────────────────────────────────────

demo:
	@echo ""
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════════$(NC)"
	@echo "$(CYAN)$(BOLD)  HTCondor Cluster Analysis Tools - Full Demo$(NC)"
	@echo "$(CYAN)$(BOLD)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(YELLOW)Cluster ID: $(CLUSTER)$(NC)"
	@echo ""
	@echo "$(BOLD)Step 1/5: Fetching cluster data...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python fetch_cluster_data.py $(CLUSTER)
	@echo ""
	@echo "$(GREEN)✓ Data fetch complete!$(NC)"
	@echo ""
	@read -p "Press Enter to continue to summary..." dummy
	@echo ""
	@echo "$(BOLD)Step 2/5: Running health summary...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python main.py summary $(CLUSTER)
	@echo ""
	@read -p "Press Enter to continue to resource analysis..." dummy
	@echo ""
	@echo "$(BOLD)Step 3/5: Running resource analysis...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python main.py resources $(CLUSTER)
	@echo ""
	@read -p "Press Enter to continue to runtime histogram..." dummy
	@echo ""
	@echo "$(BOLD)Step 4/5: Generating runtime histogram...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python main.py runtimes $(CLUSTER)
	@echo ""
	@read -p "Press Enter to continue to job status..." dummy
	@echo ""
	@echo "$(BOLD)Step 5/5: Displaying job status...$(NC)"
	@echo "$(CYAN)────────────────────────────────────────────────────────────────$(NC)"
	@python main.py status $(CLUSTER)
	@echo ""
	@echo "$(GREEN)$(BOLD)✓ Demo complete!$(NC)"
	@echo ""
	@echo "$(YELLOW)Tip: run 'make held CLUSTER=$(CLUSTER)' to analyse any held jobs.$(NC)"
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""

quick:
	@echo "$(CYAN)$(BOLD)Quick Analysis for Cluster $(CLUSTER)$(NC)"
	@echo ""
	@make fetch CLUSTER=$(CLUSTER)
	@echo ""
	@make summary CLUSTER=$(CLUSTER)
	@echo ""
	@echo "$(YELLOW)Run 'make resources CLUSTER=$(CLUSTER)' for detailed analysis.$(NC)"

all:
	@echo "$(CYAN)$(BOLD)Running all analysis tools for Cluster $(CLUSTER)$(NC)"
	@echo ""
	@make summary CLUSTER=$(CLUSTER)
	@echo ""
	@make resources CLUSTER=$(CLUSTER)
	@echo ""
	@make runtimes CLUSTER=$(CLUSTER)
	@echo ""
	@make status CLUSTER=$(CLUSTER)
	@echo ""
	@make held CLUSTER=$(CLUSTER)

compare:
	@if [ -z "$(CLUSTER2)" ]; then \
		echo "$(RED)Error: CLUSTER2 not specified$(NC)"; \
		echo "Usage: make compare CLUSTER=12345 CLUSTER2=67890"; \
		exit 1; \
	fi
	@echo "$(CYAN)$(BOLD)Comparing Clusters $(CLUSTER) and $(CLUSTER2)$(NC)"
	@echo ""
	@echo "$(BOLD)Cluster $(CLUSTER):$(NC)"
	@make summary CLUSTER=$(CLUSTER)
	@echo ""
	@echo "$(BOLD)Cluster $(CLUSTER2):$(NC)"
	@make summary CLUSTER=$(CLUSTER2)

# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────

list:
	@echo "$(CYAN)Locally cached clusters:$(NC)"
	@if [ -d "cluster_data" ]; then \
		ls -1 cluster_data/cluster_*.csv 2>/dev/null \
			| sed 's|cluster_data/cluster_||g' \
			| sed 's/_jobs\.csv//g' \
			|| echo "$(YELLOW)No cluster data found.$(NC)"; \
	else \
		echo "$(YELLOW)cluster_data/ directory not found.$(NC)"; \
	fi

clean:
	@echo "$(YELLOW)Removing cached cluster data...$(NC)"
	@rm -rf cluster_data/cluster_*.csv
	@echo "$(GREEN)✓ Clean complete!$(NC)"

check:
	@echo "$(CYAN)Checking Python dependencies...$(NC)"
	@python -c "import numpy"     2>/dev/null && echo "$(GREEN)✓ numpy$(NC)"        || echo "$(RED)✗ numpy (missing)$(NC)"
	@python -c "import tabulate"  2>/dev/null && echo "$(GREEN)✓ tabulate$(NC)"     || echo "$(RED)✗ tabulate (missing)$(NC)"
	@python -c "import htcondor2" 2>/dev/null && echo "$(GREEN)✓ htcondor2$(NC)"    || echo "$(RED)✗ htcondor2 (missing — usually provided by system HTCondor installation)$(NC)"
	@python -c "import elasticsearch" 2>/dev/null && echo "$(GREEN)✓ elasticsearch$(NC)" || echo "$(RED)✗ elasticsearch (only needed for fetch-es / query.py)$(NC)"
	@echo ""
	@echo "$(YELLOW)To install missing packages: make install$(NC)"

install:
	@echo "$(CYAN)Installing Python dependencies...$(NC)"
	@pip install -r requirements.txt
	@echo "$(GREEN)✓ Installation complete!$(NC)"