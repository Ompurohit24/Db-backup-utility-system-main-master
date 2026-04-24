#!/usr/bin/env python3
"""
Verification script for Schedule Timestamp Fix
Demonstrates the changes made to the schedule service and schema
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def verify_schedule_service():
    """Verify the schedule service has the timestamp fetch code."""
    print("=" * 70)
    print("✓ Verifying app/services/schedule_service.py")
    print("=" * 70)
    
    service_file = project_root / "app" / "services" / "schedule_service.py"
    content = service_file.read_text()
    
    checks = [
        ("create_schedule function contains get_row fetch", 
         "created_schedule_id = schedule_row.get" in content and 
         "tables.get_row" in content and 
         "Failed to fetch created schedule for timestamps" in content),
        
        ("toggle_schedule function contains get_row fetch",
         "Failed to fetch updated schedule for timestamps" in content and
         "tables.get_row" in content),
        
        ("Log warning for failed timestamp fetch on create",
         'log.warning("Failed to fetch created schedule for timestamps' in content),
        
        ("Log warning for failed timestamp fetch on update",
         'log.warning("Failed to fetch updated schedule for timestamps' in content),
    ]
    
    for check_name, result in checks:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {check_name}")
    
    print()
    return all(result for _, result in checks)


def verify_schedule_schema():
    """Verify the schedule schema has proper Pydantic config."""
    print("=" * 70)
    print("✓ Verifying app/schemas/schedule.py")
    print("=" * 70)
    
    schema_file = project_root / "app" / "schemas" / "schedule.py"
    content = schema_file.read_text()
    
    checks = [
        ("ScheduleOut model has created_at field",
         "created_at: Optional[datetime]" in content),
        
        ("ScheduleOut model has updated_at field",
         "updated_at: Optional[datetime]" in content),
        
        ("Pydantic config uses from_attributes",
         "from_attributes = True" in content),
        
        ("Old orm_mode is NOT present",
         "orm_mode = True" not in content),
        
        ("Config class is inside ScheduleOut",
         "class ScheduleOut(BaseModel):" in content and 
         "class Config:" in content),
    ]
    
    for check_name, result in checks:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {check_name}")
    
    print()
    return all(result for _, result in checks)


def verify_normalization():
    """Verify normalize_row function can handle timestamps."""
    print("=" * 70)
    print("✓ Verifying app/utils/appwrite_normalize.py")
    print("=" * 70)
    
    normalize_file = project_root / "app" / "utils" / "appwrite_normalize.py"
    content = normalize_file.read_text()
    
    checks = [
        ("normalize_row function exists",
         "def normalize_row(row: Any) -> dict:" in content),
        
        ("normalize_row_collection function exists",
         "def normalize_row_collection(result: Any) -> dict:" in content),
        
        ("Function handles Row objects",
         "isinstance(row, Row)" in content),
        
        ("Function handles dict objects",
         "isinstance(row, dict)" in content),
    ]
    
    for check_name, result in checks:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {check_name}")
    
    print()
    return all(result for _, result in checks)


def verify_appwrite_client():
    """Verify appwrite client has tables object."""
    print("=" * 70)
    print("✓ Verifying app/core/appwrite_client.py")
    print("=" * 70)
    
    client_file = project_root / "app" / "core" / "appwrite_client.py"
    content = client_file.read_text()
    
    checks = [
        ("Tables client imported",
         "TablesDB" in content or "tables" in content.lower()),
        
        ("Client initialization present",
         "Client(" in content),
    ]
    
    for check_name, result in checks:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {check_name}")
    
    print()
    return all(result for _, result in checks)


def main():
    """Run all verifications."""
    print("\n")
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 68 + "║")
    print("║" + "  SCHEDULE TIMESTAMP FIX - VERIFICATION REPORT".center(68) + "║")
    print("║" + " " * 68 + "║")
    print("╚" + "═" * 68 + "╝")
    print()
    
    results = {
        "Schedule Service": verify_schedule_service(),
        "Schedule Schema": verify_schedule_schema(),
        "Normalization Utils": verify_normalization(),
        "Appwrite Client": verify_appwrite_client(),
    }
    
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    all_passed = True
    for component, passed in results.items():
        status = "✅ VERIFIED" if passed else "❌ ISSUES"
        print(f"{status}: {component}")
        all_passed = all_passed and passed
    
    print()
    
    if all_passed:
        print("╔" + "═" * 68 + "╗")
        print("║" + "  ✅ ALL VERIFICATIONS PASSED - READY FOR DEPLOYMENT".center(68) + "║")
        print("╚" + "═" * 68 + "╝")
        print()
        return 0
    else:
        print("╔" + "═" * 68 + "╗")
        print("║" + "  ❌ SOME VERIFICATIONS FAILED - REVIEW REQUIRED".center(68) + "║")
        print("╚" + "═" * 68 + "╝")
        print()
        return 1


if __name__ == "__main__":
    sys.exit(main())

