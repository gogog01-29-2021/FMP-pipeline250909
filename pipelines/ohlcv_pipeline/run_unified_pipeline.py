#!/usr/bin/env python3
"""
Unified Data Pipeline Orchestrator
Master script to run the complete ETL pipeline:
1. Create QuestDB schema
2. Crawl data from APIs
3. Load data into QuestDB
4. Verify results
"""

import sys
import io
import logging
from datetime import datetime
from pathlib import Path

# Fix Windows encoding
if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except (ValueError, AttributeError):
        pass  # Already wrapped or not available

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

# Import our modules
sys.path.append('src')
from create_unified_schema import UnifiedSchemaCreator
from unified_data_crawler import UnifiedDataCrawler
from load_to_questdb import QuestDBLoader

class UnifiedPipelineOrchestrator:
    """Orchestrates the complete ETL pipeline"""

    def __init__(self, override_years=None):
        self.schema_creator = UnifiedSchemaCreator()
        self.crawler = UnifiedDataCrawler(override_years=override_years)
        self.loader = QuestDBLoader()
        self.override_years = override_years

    def run_full_pipeline(self, crawl_priority: int = 1, crawl_limit: int = None,
                          skip_schema: bool = False, skip_crawl: bool = False,
                          skip_load: bool = False):
        """
        Run the complete pipeline

        Args:
            crawl_priority: Only crawl symbols with priority <= N
            crawl_limit: Limit number of symbols to crawl
            skip_schema: Skip schema creation (if already exists)
            skip_crawl: Skip data crawling (if files already exist)
            skip_load: Skip loading to QuestDB
        """
        logging.info("\n" + "="*80)
        logging.info("🚀 UNIFIED DATA PIPELINE - STARTING")
        logging.info("="*80)
        logging.info(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("="*80)

        pipeline_start = datetime.now()

        # STEP 1: Create Schema
        if not skip_schema:
            logging.info("\n" + "="*80)
            logging.info("STEP 1: CREATE QUESTDB SCHEMA")
            logging.info("="*80)

            try:
                self.schema_creator.create_all_tables()
                logging.info("✅ Schema creation complete")
            except Exception as e:
                logging.error(f"❌ Schema creation failed: {e}")
                return False
        else:
            logging.info("\n⏭️  STEP 1: SKIPPED (Schema creation)")

        # STEP 2: Crawl Data
        if not skip_crawl:
            logging.info("\n" + "="*80)
            logging.info("STEP 2: CRAWL DATA FROM APIS")
            logging.info("="*80)

            try:
                self.crawler.crawl_all(priority=crawl_priority, limit=crawl_limit)
                logging.info("✅ Data crawling complete")
            except Exception as e:
                logging.error(f"❌ Data crawling failed: {e}")
                return False
        else:
            logging.info("\n⏭️  STEP 2: SKIPPED (Data crawling)")

        # STEP 3: Load to QuestDB
        if not skip_load:
            logging.info("\n" + "="*80)
            logging.info("STEP 3: LOAD DATA TO QUESTDB")
            logging.info("="*80)

            try:
                # Load CSV files
                self.loader.load_all_files(data_dir='data/csv', file_type='csv')
                logging.info("✅ Data loading complete")
            except Exception as e:
                logging.error(f"❌ Data loading failed: {e}")
                return False
        else:
            logging.info("\n⏭️  STEP 3: SKIPPED (Data loading)")

        # STEP 4: Verify Results
        logging.info("\n" + "="*80)
        logging.info("STEP 4: VERIFY RESULTS")
        logging.info("="*80)

        try:
            self.loader.verify_data()
            logging.info("✅ Data verification complete")
        except Exception as e:
            logging.error(f"❌ Data verification failed: {e}")

        # Final Summary
        pipeline_end = datetime.now()
        duration = pipeline_end - pipeline_start

        logging.info("\n" + "="*80)
        logging.info("✅ UNIFIED DATA PIPELINE - COMPLETE")
        logging.info("="*80)
        logging.info(f"⏰ End Time: {pipeline_end.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"⏱️  Duration: {duration}")
        logging.info("="*80)

        logging.info("\n📝 Next Steps:")
        logging.info("1. Access QuestDB Web Console: http://localhost:9000")
        logging.info("2. Run sample queries:")
        logging.info("   - SELECT * FROM ohlcv_unified LIMIT 10;")
        logging.info("   - SELECT symbol, COUNT(*) FROM ohlcv_unified GROUP BY symbol;")
        logging.info("3. Use Python/pandas for analysis:")
        logging.info("   - import pandas as pd")
        logging.info("   - from sqlalchemy import create_engine")
        logging.info("   - engine = create_engine('postgresql://admin:quest@localhost:8812/qdb')")
        logging.info("   - df = pd.read_sql('SELECT * FROM ohlcv_unified LIMIT 1000', engine)")

        return True

    def run_quick_test(self):
        """Run a quick test with limited data"""
        logging.info("\n🧪 RUNNING QUICK TEST (Priority 1, Limit 5 symbols)")

        return self.run_full_pipeline(
            crawl_priority=1,
            crawl_limit=5,
            skip_schema=False,
            skip_crawl=False,
            skip_load=False
        )

    def run_priority_1(self):
        """Run pipeline for priority 1 symbols only (most important)"""
        logging.info("\n⭐ RUNNING PRIORITY 1 PIPELINE (Critical symbols)")

        return self.run_full_pipeline(
            crawl_priority=1,
            crawl_limit=None,
            skip_schema=True,  # Assume schema exists
            skip_crawl=False,
            skip_load=False
        )

    def run_full_production(self):
        """Run complete production pipeline (all symbols)"""
        logging.info("\n🏭 RUNNING FULL PRODUCTION PIPELINE (All symbols)")

        return self.run_full_pipeline(
            crawl_priority=None,  # All priorities
            crawl_limit=None,     # No limit
            skip_schema=True,     # Assume schema exists
            skip_crawl=False,
            skip_load=False
        )

    def reload_only(self):
        """Only reload existing CSV files to QuestDB"""
        logging.info("\n♻️  RELOADING EXISTING DATA TO QUESTDB")

        return self.run_full_pipeline(
            skip_schema=True,
            skip_crawl=True,
            skip_load=False
        )

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Unified Data Pipeline Orchestrator')
    parser.add_argument('--mode', choices=['test', 'priority1', 'full', 'reload'],
                        default='test', help='Pipeline execution mode')
    parser.add_argument('--priority', type=int, help='Crawl priority (1-3)')
    parser.add_argument('--limit', type=int, help='Limit number of symbols')
    parser.add_argument('--years', type=int, help='Override years of data (e.g., 1, 5, 10, 20)')
    parser.add_argument('--skip-schema', action='store_true', help='Skip schema creation')
    parser.add_argument('--skip-crawl', action='store_true', help='Skip data crawling')
    parser.add_argument('--skip-load', action='store_true', help='Skip loading to QuestDB')

    args = parser.parse_args()

    orchestrator = UnifiedPipelineOrchestrator(override_years=args.years)

    # Run based on mode
    if args.mode == 'test':
        orchestrator.run_quick_test()
    elif args.mode == 'priority1':
        orchestrator.run_priority_1()
    elif args.mode == 'full':
        orchestrator.run_full_production()
    elif args.mode == 'reload':
        orchestrator.reload_only()
    else:
        # Custom run
        orchestrator.run_full_pipeline(
            crawl_priority=args.priority,
            crawl_limit=args.limit,
            skip_schema=args.skip_schema,
            skip_crawl=args.skip_crawl,
            skip_load=args.skip_load
        )

if __name__ == "__main__":
    main()
