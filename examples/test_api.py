"""
Test script for FastAPI backend
Tests both unified and staged execution modes
"""
import requests
import time
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("APITest")

BASE_URL = "http://localhost:8000"


def test_health():
    """Test health check endpoint"""
    logger.info("Testing health endpoint...")
    response = requests.get(f"{BASE_URL}/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    logger.info(f"✅ Health check passed: {data}")


def test_unified_mode():
    """Test unified pipeline execution"""
    logger.info("\n" + "="*60)
    logger.info("Testing Unified Mode")
    logger.info("="*60)

    # Pipeline configuration
    config = {
        "name": "test_unified_api",
        "mode": "unified",
        "source": {
            "type": "csv",
            "path": "./data/sample.csv"
        },
        "transformers": [
            {
                "type": "null_remover",
                "config": {}
            }
        ],
        "destination": {
            "type": "sqlite",
            "path": "./output/api_test_unified.db",
            "table_name": "test_data"
        },
        "storage": {
            "type": "file",
            "path": "./.state/api_test"
        }
    }

    # Execute pipeline
    logger.info("Sending unified pipeline request...")
    response = requests.post(f"{BASE_URL}/api/pipeline/unified", json=config)

    assert response.status_code == 200, f"Failed: {response.text}"

    data = response.json()
    logger.info(f"✅ Unified pipeline completed")
    logger.info(f"   Pipeline ID: {data['pipeline_id']}")
    logger.info(f"   Status: {data['status']}")
    logger.info(f"   Message: {data['message']}")

    if data.get("stages"):
        for stage in data["stages"]:
            logger.info(f"   {stage['stage'].upper()}: {stage['records_out']} records, {stage['duration_seconds']:.2f}s")

    return data["pipeline_id"]


def test_staged_mode():
    """Test staged pipeline execution"""
    logger.info("\n" + "="*60)
    logger.info("Testing Staged Mode")
    logger.info("="*60)

    # Pipeline configuration
    config = {
        "name": "test_staged_api",
        "mode": "staged",
        "source": {
            "type": "csv",
            "path": "./data/sample.csv"
        },
        "transformers": [
            {
                "type": "null_remover",
                "config": {}
            }
        ],
        "destination": {
            "type": "sqlite",
            "path": "./output/api_test_staged.db",
            "table_name": "test_data"
        },
        "storage": {
            "type": "file",
            "path": "./.state/api_test"
        }
    }

    # Step 1: Initialize
    logger.info("\n1️⃣  Initializing staged pipeline...")
    response = requests.post(f"{BASE_URL}/api/pipeline/staged/init", json=config)
    assert response.status_code == 200

    data = response.json()
    pipeline_id = data["pipeline_id"]
    logger.info(f"✅ Pipeline initialized: {pipeline_id}")

    # Step 2: Extract
    logger.info("\n2️⃣  Running Extract stage...")
    response = requests.post(f"{BASE_URL}/api/pipeline/staged/{pipeline_id}/extract")
    assert response.status_code == 200

    data = response.json()
    logger.info(f"✅ Extract completed: {data['records']} records, {data['duration_seconds']:.2f}s")

    # Step 3: Check status
    logger.info("\n📊 Checking pipeline status...")
    response = requests.get(f"{BASE_URL}/api/pipeline/{pipeline_id}/status")
    assert response.status_code == 200

    status = response.json()
    logger.info(f"   Extract: {status['extract_status']} ({status['extract_records']} records)")
    logger.info(f"   Transform: {status['transform_status']}")
    logger.info(f"   Load: {status['load_status']}")

    # Step 4: Preview data
    logger.info("\n👀 Previewing extracted data...")
    response = requests.get(f"{BASE_URL}/api/pipeline/{pipeline_id}/data/preview?stage=extracted&limit=3")
    assert response.status_code == 200

    preview = response.json()
    logger.info(f"   Found {preview['count']} records")
    if preview["records"]:
        logger.info(f"   Sample: {json.dumps(preview['records'][0], indent=2)}")

    # Step 5: Transform
    logger.info("\n3️⃣  Running Transform stage...")
    response = requests.post(f"{BASE_URL}/api/pipeline/staged/{pipeline_id}/transform")
    assert response.status_code == 200

    data = response.json()
    logger.info(f"✅ Transform completed: {data['records']} records, {data['duration_seconds']:.2f}s")

    # Step 6: Load
    logger.info("\n4️⃣  Running Load stage...")
    response = requests.post(f"{BASE_URL}/api/pipeline/staged/{pipeline_id}/load")
    assert response.status_code == 200

    data = response.json()
    logger.info(f"✅ Load completed: {data['records']} records, {data['duration_seconds']:.2f}s")

    # Step 7: Final status
    logger.info("\n📊 Final pipeline status...")
    response = requests.get(f"{BASE_URL}/api/pipeline/{pipeline_id}/status")
    status = response.json()

    logger.info(f"   Overall: {status['overall_status']}")
    logger.info(f"   Extract: {status['extract_status']} ({status['extract_records']} records)")
    logger.info(f"   Transform: {status['transform_status']} ({status['transform_records']} records)")
    logger.info(f"   Load: {status['load_status']} ({status['load_records']} records)")

    return pipeline_id


def test_list_pipelines():
    """Test listing pipelines"""
    logger.info("\n" + "="*60)
    logger.info("Testing List Pipelines")
    logger.info("="*60)

    response = requests.get(f"{BASE_URL}/api/pipelines")
    assert response.status_code == 200

    pipelines = response.json()
    logger.info(f"✅ Found {len(pipelines)} pipeline(s)")

    for p in pipelines:
        logger.info(f"   - {p['name']} ({p['mode']}) - {p['overall_status']}")


def main():
    """Run all tests"""
    logger.info("="*60)
    logger.info("AI ETL Framework API Test Suite")
    logger.info("="*60)
    logger.info(f"Base URL: {BASE_URL}")
    logger.info("")

    try:
        # Health check
        test_health()

        # Test unified mode
        test_unified_mode()

        # Test staged mode
        test_staged_mode()

        # List all pipelines
        test_list_pipelines()

        logger.info("\n" + "="*60)
        logger.info("✅ All API tests passed!")
        logger.info("="*60)

    except AssertionError as e:
        logger.error(f"\n❌ Test failed: {e}")
        raise

    except requests.exceptions.ConnectionError:
        logger.error(f"\n❌ Could not connect to API at {BASE_URL}")
        logger.error("Make sure the API server is running:")
        logger.error("  python -m uvicorn src.api.main:app --reload")
        raise

    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
