"""
Data Profiling Script
Purpose: Profile all CSV tables to understand data quality and structure
Author: Data Engineer Intern
"""

import pandas as pd
import os
from datetime import datetime
import sys

print("Script started...")
print(f"Python version: {sys.version}")
print(f"Pandas version: {pd.__version__}")
print(f"Current working directory: {os.getcwd()}")
print()

# Define data directory
DATA_DIR = 'data'
OUTPUT_DIR = 'output'

print(f"Looking for data in: {os.path.abspath(DATA_DIR)}")
print(f"Output will be saved to: {os.path.abspath(OUTPUT_DIR)}")
print()

# Create output directory if it doesn't exist
try:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"✓ Output directory ready: {OUTPUT_DIR}")
except Exception as e:
    print(f"✗ Error creating output directory: {e}")
    sys.exit(1)

# Check if data directory exists
if not os.path.exists(DATA_DIR):
    print(f"✗ ERROR: Data directory not found: {DATA_DIR}")
    print(f"Please create '{DATA_DIR}' folder and place CSV files there.")
    sys.exit(1)
else:
    print(f"✓ Data directory found: {DATA_DIR}")
    print(f"  Contents: {os.listdir(DATA_DIR)}")
    print()

# List of all CSV files to profile
csv_files = {
    'lead_log': 'lead_log(in).csv',
    'user_referrals': 'user_referrals(in).csv',
    'user_referral_logs': 'user_referral_logs(in).csv',
    'user_logs': 'user_logs(in).csv',
    'user_referral_statuses': 'user_referral_statuses(in).csv',
    'referral_rewards': 'referral_rewards(in).csv',
    'paid_transactions': 'paid_transactions(in).csv'
}

def profile_dataframe(df, table_name):
    """
    Profile a single dataframe and return profiling statistics
    
    Args:
        df: pandas DataFrame to profile
        table_name: name of the table
        
    Returns:
        DataFrame with profiling information
    """
    profile_data = []
    
    for column in df.columns:
        # Calculate statistics
        total_rows = len(df)
        null_count = df[column].isnull().sum()
        null_percentage = (null_count / total_rows) * 100
        distinct_count = df[column].nunique()
        data_type = str(df[column].dtype)
        
        # Sample values (first 3 non-null unique values)
        sample_values = df[column].dropna().unique()[:3]
        sample_str = ', '.join([str(v) for v in sample_values])
        
        profile_data.append({
            'Table Name': table_name,
            'Column Name': column,
            'Data Type': data_type,
            'Total Rows': total_rows,
            'Null Count': null_count,
            'Null Percentage': round(null_percentage, 2),
            'Distinct Count': distinct_count,
            'Sample Values': sample_str
        })
    
    return pd.DataFrame(profile_data)

def main():
    """
    Main function to profile all tables
    """
    print("=" * 80)
    print("DATA PROFILING STARTED")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    all_profiles = []
    
    # Profile each CSV file
    for table_name, filename in csv_files.items():
        filepath = os.path.join(DATA_DIR, filename)
        
        print(f"Profiling: {table_name} ({filename})")
        print(f"  Path: {filepath}")
        
        # Check if file exists
        if not os.path.exists(filepath):
            print(f"  ✗ File not found: {filepath}")
            print()
            continue
        
        try:
            # Read CSV file
            print(f"  - Reading CSV...")
            df = pd.read_csv(filepath)
            print(f"  - CSV loaded: {len(df)} rows, {len(df.columns)} columns")
            
            # Profile the dataframe
            print(f"  - Profiling...")
            profile_df = profile_dataframe(df, table_name)
            all_profiles.append(profile_df)
            
            print(f"  ✓ Rows: {len(df)}, Columns: {len(df.columns)}")
            print()
            
        except Exception as e:
            print(f"  ✗ Error profiling {table_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            print()
    
    # Combine all profiles
    if all_profiles:
        print("Combining all profiles...")
        combined_profile = pd.concat(all_profiles, ignore_index=True)
        print(f"Total profile records: {len(combined_profile)}")
        
        # Save to Excel
        output_file = os.path.join(OUTPUT_DIR, 'data_profiling_report.xlsx')
        print(f"Saving to: {output_file}")
        
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Write combined profile
                print("  - Writing 'All Tables Profile' sheet...")
                combined_profile.to_excel(writer, sheet_name='All Tables Profile', index=False)
                
                # Write individual table profiles
                for table_name in csv_files.keys():
                    table_profile = combined_profile[combined_profile['Table Name'] == table_name]
                    if not table_profile.empty:
                        sheet_name = table_name[:31]  # Excel sheet name limit
                        print(f"  - Writing '{sheet_name}' sheet...")
                        table_profile.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print()
            print("=" * 80)
            print(f"✓ Profiling completed successfully!")
            print(f"✓ Report saved to: {output_file}")
            print("=" * 80)
            
            # Print summary statistics
            print("\nSUMMARY:")
            print(f"Total tables profiled: {len(csv_files)}")
            print(f"Total columns profiled: {len(combined_profile)}")
            print(f"\nTables with null values:")
            
            tables_with_nulls = combined_profile[combined_profile['Null Count'] > 0]['Table Name'].unique()
            for table in tables_with_nulls:
                null_cols = combined_profile[(combined_profile['Table Name'] == table) & 
                                            (combined_profile['Null Count'] > 0)]
                print(f"  - {table}: {len(null_cols)} columns with nulls")
            
            print()
            print("Script completed successfully!")
            
        except Exception as e:
            print(f"✗ Error saving Excel file: {e}")
            import traceback
            traceback.print_exc()
    
    else:
        print("=" * 80)
        print("✗ No profiles generated. Check for errors above.")
        print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\nPress Enter to exit...")
    input()