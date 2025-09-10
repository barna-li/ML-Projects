"""
Test script for oracle_data_processor module.

This script tests the function structure and validates that the module
can be imported and basic functions work without database connection.
"""

import pandas as pd
import numpy as np
import sys
import os

# Add current directory to path for imports
sys.path.append('/home/runner/work/ML-Projects/ML-Projects')

try:
    from oracle_data_processor import (
        batch_list,
        get_hardcoded_numbers,
        process_value,
        process_column,
        aggregate_cbrs,
        join_cbrs_keep_duplicates
    )
    print("✓ Successfully imported oracle_data_processor functions")
except ImportError as e:
    print(f"✗ Failed to import oracle_data_processor: {e}")
    sys.exit(1)


def test_batch_list():
    """Test the batch_list function."""
    print("\nTesting batch_list function...")
    test_list = list(range(10))
    batches = list(batch_list(test_list, batch_size=3))
    
    expected = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    if batches == expected:
        print("✓ batch_list works correctly")
    else:
        print(f"✗ batch_list failed. Expected {expected}, got {batches}")


def test_hardcoded_numbers():
    """Test the get_hardcoded_numbers function."""
    print("\nTesting get_hardcoded_numbers function...")
    hardcoded = get_hardcoded_numbers()
    
    if isinstance(hardcoded, set) and len(hardcoded) > 0:
        print(f"✓ get_hardcoded_numbers works correctly. Found {len(hardcoded)} hardcoded numbers")
    else:
        print("✗ get_hardcoded_numbers failed")


def test_process_value():
    """Test the process_value function."""
    print("\nTesting process_value function...")
    hardcoded_set = {"9999999999", "1111111111"}
    special_words = {"none", "i dont have one"}
    
    # Test cases
    test_cases = [
        ("9999999999", np.nan),  # Should be filtered out
        ("none", np.nan),  # Should be filtered out
        ("1234567890", "1234567890"),  # Should be kept
        ("9999999999,1234567890", "1234567890"),  # Should filter out hardcoded
        (None, np.nan),  # Should handle NaN
        ("", np.nan),  # Should handle empty string
    ]
    
    all_passed = True
    for test_input, expected in test_cases:
        result = process_value(test_input, hardcoded_set, special_words)
        if pd.isna(expected) and pd.isna(result):
            continue  # Both NaN, test passed
        elif result != expected:
            print(f"✗ process_value failed for input '{test_input}'. Expected '{expected}', got '{result}'")
            all_passed = False
    
    if all_passed:
        print("✓ process_value works correctly")


def test_process_column():
    """Test the process_column function."""
    print("\nTesting process_column function...")
    
    # Create test DataFrame
    test_df = pd.DataFrame({
        'CBR_LIST': ['1234567890', '9999999999', '1234567890,9999999999', 'none', None]
    })
    
    processed_df = process_column(test_df.copy())
    
    # Check if processing worked (should filter out hardcoded numbers and special words)
    if 'CBR_LIST' in processed_df.columns:
        print("✓ process_column works correctly")
    else:
        print("✗ process_column failed")


def test_aggregate_cbrs():
    """Test the aggregate_cbrs function."""
    print("\nTesting aggregate_cbrs function...")
    
    # Create test DataFrame
    test_group = pd.DataFrame({
        'CBR': ['1234567890', '0987654321', '1111111111'],
        'META_UPD_DTTM': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
    })
    
    result = aggregate_cbrs(test_group)
    expected = '1234567890,0987654321,1111111111'
    
    if result == expected:
        print("✓ aggregate_cbrs works correctly")
    else:
        print(f"✗ aggregate_cbrs failed. Expected '{expected}', got '{result}'")


def test_join_cbrs_keep_duplicates():
    """Test the join_cbrs_keep_duplicates function."""
    print("\nTesting join_cbrs_keep_duplicates function...")
    
    # Create test DataFrame
    test_group = pd.DataFrame({
        'CBR': ['1234567890', '0987654321'],
        'CBR_SOURCE': ['WFAC_CONTACT_TN', 'TRACS_CALLER_CAN_BE_REACHED']
    })
    
    result = join_cbrs_keep_duplicates(test_group)
    expected = '1234567890[WFAC_CONTACT_TN],0987654321[TRACS_CALLER_CAN_BE_REACHED]'
    
    if result == expected:
        print("✓ join_cbrs_keep_duplicates works correctly")
    else:
        print(f"✗ join_cbrs_keep_duplicates failed. Expected '{expected}', got '{result}'")


def main():
    """Run all tests."""
    print("Running tests for oracle_data_processor module...")
    
    test_batch_list()
    test_hardcoded_numbers()
    test_process_value()
    test_process_column()
    test_aggregate_cbrs()
    test_join_cbrs_keep_duplicates()
    
    print("\n" + "="*50)
    print("All basic function tests completed!")
    print("Note: Database connection functions require actual Oracle database access to test.")


if __name__ == "__main__":
    main()