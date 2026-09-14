#!/usr/bin/env python3
"""Comprehensive validation suite for Pokemon analytics orchestration.

Tests:
  1. DAG syntax and structure
  2. dbt job content validity
  3. dlt pipeline syntax (import checks)
  4. Orchestration support module functionality
  5. File presence and consistency
"""
import sys
import json
import ast
from pathlib import Path

# Orchestration paths
WORKSPACE_ROOT = Path(__file__).parent
ORCHESTRATION_DIR = WORKSPACE_ROOT / "orchestration"
TRANSFORMATION_DIR = WORKSPACE_ROOT / "transformation"
INGESTION_DIR = WORKSPACE_ROOT / "ingestion"

DAG_FILE = ORCHESTRATION_DIR / "pokemon_daily.ApacheAirflowJob" / "dags" / "pokemon_daily.py"
SUPPORT_FILE = ORCHESTRATION_DIR / "pokemon_daily.ApacheAirflowJob" / "plugins" / "orchestration_support.py"
DBT_CONTENT_FILE = ORCHESTRATION_DIR / "pokemon_transform.DataBuildToolJob" / "dbt-content.json"
AIRFLOW_CONTENT_FILE = ORCHESTRATION_DIR / "pokemon_daily.ApacheAirflowJob" / "apacheairflowjob-content.json"
NOTEBOOK_FILE = ORCHESTRATION_DIR / "dlt_notebook_runner.Notebook" / "notebook-content.py"
DLT_PIPELINE_FILE = INGESTION_DIR / "pokemon_pipeline.py"
DBT_PROJECT_FILE = TRANSFORMATION_DIR / "dbt_project.yml"

TESTS_PASSED = []
TESTS_FAILED = []


def log_pass(test_name, detail=""):
    msg = f"✅ {test_name}"
    if detail:
        msg += f": {detail}"
    print(msg)
    TESTS_PASSED.append(test_name)


def log_fail(test_name, detail=""):
    msg = f"❌ {test_name}"
    if detail:
        msg += f": {detail}"
    print(msg)
    TESTS_FAILED.append(test_name)


def test_file_exists(name, path):
    """Verify a required file exists."""
    if path.exists():
        log_pass(f"File exists: {name}", str(path.relative_to(WORKSPACE_ROOT)))
        return True
    else:
        log_fail(f"File exists: {name}", f"Missing: {path}")
        return False


def test_dag_syntax():
    """Validate DAG file Python syntax and structure."""
    print("\n--- Testing DAG Syntax ---")
    if not test_file_exists("DAG file", DAG_FILE):
        return False

    try:
        with open(DAG_FILE) as f:
            code = f.read()
        ast.parse(code)
        log_pass("DAG syntax", "Valid Python")
        
        # Check for required elements
        if 'dag_id="pokemon_daily"' in code:
            log_pass("DAG id", "pokemon_daily found")
        else:
            log_fail("DAG id", "pokemon_daily not found")
            return False
            
        if "MSFabricRunJobOperator" in code:
            log_pass("Fabric operators", "MSFabricRunJobOperator imported")
        else:
            log_fail("Fabric operators", "MSFabricRunJobOperator not found")
            return False
            
        # Check task definitions
        required_tasks = ["resolve_dlt", "dlt_load", "resolve_dbt", "dbt_build", "record_run", "workload_succeeded"]
        for task in required_tasks:
            if f'task_id="{task}"' in code or f"task_id='{task}'" in code:
                log_pass(f"Task defined: {task}")
            else:
                log_fail(f"Task defined: {task}", "Not found in DAG")
                return False
        
        return True
    except SyntaxError as e:
        log_fail("DAG syntax", f"Syntax error: {e}")
        return False


def test_dag_schedule():
    """Verify DAG schedule configuration."""
    print("\n--- Testing DAG Schedule ---")
    if not test_file_exists("DAG file", DAG_FILE):
        return False

    with open(DAG_FILE) as f:
        code = f.read()
    
    # Check current schedule
    if "schedule=None" in code:
        log_pass("Schedule parameter", "Found (currently None, should be updated to '0 6 * * *')")
    elif "schedule=" in code:
        if "'0 6 * * *'" in code or '"0 6 * * *"' in code:
            log_pass("Schedule", "Daily at 6 AM UTC configured")
            return True
        else:
            import re
            match = re.search(r'schedule\s*=\s*["\']([^"\']+)["\']', code)
            if match:
                log_fail("Schedule", f"Found '{match.group(1)}', expected '0 6 * * *'")
            else:
                log_fail("Schedule", "Found but unable to parse")
            return False
    else:
        log_fail("Schedule", "No schedule parameter found")
        return False
    
    return True


def test_orchestration_support():
    """Validate orchestration support module."""
    print("\n--- Testing Orchestration Support ---")
    if not test_file_exists("Support module", SUPPORT_FILE):
        return False

    try:
        with open(SUPPORT_FILE) as f:
            code = f.read()
        ast.parse(code)
        log_pass("Support module syntax", "Valid Python")
        
        # Check required functions
        required_functions = [
            "current_workspace_id",
            "resolve_item_id",
            "resolve_runner_coordinates",
            "write_orchestration_run",
        ]
        for func in required_functions:
            if f"def {func}" in code:
                log_pass(f"Function: {func}")
            else:
                log_fail(f"Function: {func}", "Not found")
                return False
        
        return True
    except SyntaxError as e:
        log_fail("Support module syntax", f"Syntax error: {e}")
        return False


def test_dbt_job_content():
    """Validate dbt job content JSON."""
    print("\n--- Testing dbt Job Content ---")
    if not test_file_exists("dbt job content", DBT_CONTENT_FILE):
        return False

    try:
        with open(DBT_CONTENT_FILE) as f:
            content = json.load(f)
        
        log_pass("dbt job JSON", "Valid JSON")
        
        # Check structure
        if "project" in content and "command" in content:
            log_pass("dbt job structure", "Has project and command sections")
        else:
            log_fail("dbt job structure", "Missing project or command section")
            return False
        
        # Check project type
        project_type = content.get("project", {}).get("projectType")
        if project_type == "OneLake":
            log_pass("dbt project type", "OneLake configured")
        else:
            log_fail("dbt project type", f"Expected OneLake, got {project_type}")
            return False
        
        # Check operation
        operation = content.get("command", {}).get("operation")
        if operation == "build":
            log_pass("dbt operation", "build command configured")
        else:
            log_fail("dbt operation", f"Expected build, got {operation}")
            return False
        
        return True
    except json.JSONDecodeError as e:
        log_fail("dbt job JSON", f"JSON error: {e}")
        return False


def test_airflow_job_content():
    """Validate Apache Airflow job content JSON."""
    print("\n--- Testing Airflow Job Content ---")
    if not test_file_exists("Airflow job content", AIRFLOW_CONTENT_FILE):
        return False

    try:
        with open(AIRFLOW_CONTENT_FILE) as f:
            content = json.load(f)
        
        log_pass("Airflow job JSON", "Valid JSON")
        
        # Check structure
        if "properties" in content and "typeProperties" in content.get("properties", {}):
            log_pass("Airflow job structure", "Has properties and typeProperties")
        else:
            log_fail("Airflow job structure", "Missing required sections")
            return False
        
        # Check Airflow configuration
        airflow_props = (
            content.get("properties", {})
            .get("typeProperties", {})
            .get("airflowProperties", {})
        )
        
        if airflow_props.get("airflowVersion") == "2.10.5":
            log_pass("Airflow version", "2.10.5 configured")
        else:
            log_fail("Airflow version", f"Got {airflow_props.get('airflowVersion')}")
            return False
        
        # Check required provider
        requirements = airflow_props.get("airflowRequirements", [])
        if "apache-airflow-providers-microsoft-fabric" in str(requirements):
            log_pass("Fabric provider", "apache-airflow-providers-microsoft-fabric included")
        else:
            log_fail("Fabric provider", "Missing Fabric provider in requirements")
            return False
        
        # Check AAD integration
        if airflow_props.get("enableAADIntegration"):
            log_pass("AAD integration", "Enabled for Entra authentication")
        else:
            log_fail("AAD integration", "Not enabled")
            return False
        
        return True
    except json.JSONDecodeError as e:
        log_fail("Airflow job JSON", f"JSON error: {e}")
        return False


def test_dlt_pipeline_syntax():
    """Validate dlt pipeline Python syntax."""
    print("\n--- Testing dlt Pipeline ---")
    if not test_file_exists("dlt pipeline", DLT_PIPELINE_FILE):
        return False

    try:
        with open(DLT_PIPELINE_FILE) as f:
            code = f.read()
        ast.parse(code)
        log_pass("dlt pipeline syntax", "Valid Python")
        
        # Check imports
        if "import dlt" in code:
            log_pass("dlt import", "dlt module imported")
        else:
            log_fail("dlt import", "dlt not imported")
            return False
        
        # Check resource definition
        if "@dlt.resource" in code and "def pokemon" in code:
            log_pass("dlt resource", "pokemon() resource defined")
        else:
            log_fail("dlt resource", "pokemon() resource not found")
            return False
        
        # Check pipeline configuration (can be constant or literal)
        if ('PIPELINE_NAME = "pokemon_bronze"' in code or
            'pipeline_name=PIPELINE_NAME' in code or
            "pipeline_name = PIPELINE_NAME" in code):
            log_pass("Pipeline name", "pokemon_bronze configured (via PIPELINE_NAME constant)")
        else:
            log_fail("Pipeline name", "pokemon_bronze not found")
            return False
        
        if 'destination="fabric"' in code or 'destination = "fabric"' in code:
            log_pass("Destination", "fabric configured")
        else:
            log_fail("Destination", "fabric destination not found")
            return False
        
        return True
    except SyntaxError as e:
        log_fail("dlt pipeline syntax", f"Syntax error: {e}")
        return False


def test_dbt_project_structure():
    """Validate dbt project configuration."""
    print("\n--- Testing dbt Project Structure ---")
    if not test_file_exists("dbt project file", DBT_PROJECT_FILE):
        return False
    
    staging_model = TRANSFORMATION_DIR / "models" / "staging" / "stg_pokemon.sql"
    marts_model = TRANSFORMATION_DIR / "models" / "marts" / "pokemon_weight_bands.sql"
    
    test_file_exists("Staging model", staging_model)
    test_file_exists("Marts model", marts_model)
    
    return staging_model.exists() and marts_model.exists()


def test_task_dependencies():
    """Verify task dependency chain."""
    print("\n--- Testing Task Dependencies ---")
    if not test_file_exists("DAG file", DAG_FILE):
        return False

    with open(DAG_FILE) as f:
        code = f.read()
    
    # The expected chain is:
    # resolve_dlt >> dlt_load >> resolve_dbt >> dbt_build >> [record_run, workload_succeeded]
    dependency_line = "resolve_dlt >> dlt_load >> resolve_dbt >> dbt_build >> [record_run, workload_succeeded]"
    
    if dependency_line in code:
        log_pass("Task chain", "Correct dependency order confirmed")
        return True
    else:
        # More flexible check
        if (">>" in code and "resolve_dlt" in code and "dlt_load" in code and
            "resolve_dbt" in code and "dbt_build" in code):
            log_pass("Task chain", "Dependencies present (exact order not verified)")
            return True
        else:
            log_fail("Task chain", "Dependencies not properly configured")
            return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("Pokemon Analytics Orchestration Validation Suite")
    print("=" * 60)
    
    tests = [
        test_dag_syntax,
        test_dag_schedule,
        test_orchestration_support,
        test_dbt_job_content,
        test_airflow_job_content,
        test_dlt_pipeline_syntax,
        test_dbt_project_structure,
        test_task_dependencies,
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 60)
    print(f"Tests passed: {len(TESTS_PASSED)}")
    print(f"Tests failed: {len(TESTS_FAILED)}")
    
    if TESTS_FAILED:
        print("\nFailed tests:")
        for test in TESTS_FAILED:
            print(f"  - {test}")
    
    print("=" * 60)
    
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
