from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))
import ingestion2
import metadata_upload
import transformation2
import datamart2

def run_pipeline():
    try:
        print(f"\n{'='*50}")
        print(f"[{datetime.now()}] Pipeline starts...")
        print(f"{'='*50}")

        print("\n--- Job: Ingestion ---")
        ingestion2.run()

        print("\n--- Activity: Upload Metadata ---")
        metadata_upload.run()

        print("\n--- Job: Transformation ---")
        transformation2.run_all()

        print("\n--- Job: Datamart ---")
        datamart2.run()
    except Exception as e:
        print(f"An error occured: {e}")

print("=== Running pipeline===")
run_pipeline()

scheduler = BlockingScheduler()
scheduler.add_job(run_pipeline, 'cron', hour=8, minute=0)

print("Scheduler is running...")
scheduler.start()