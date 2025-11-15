"""
Data Dictionary Generator
Purpose: Create comprehensive data dictionary for business users
"""

import pandas as pd
import os
import sys

print("=" * 80)
print("DATA DICTIONARY GENERATOR")
print("=" * 80)
print(f"Python version: {sys.version}")
print(f"Pandas version: {pd.__version__}")
print(f"Current directory: {os.getcwd()}\n")

OUTPUT_DIR = 'docs'

# Create docs directory
try:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"✓ Output directory ready: {OUTPUT_DIR}\n")
except Exception as e:
    print(f"✗ Error creating directory: {e}")
    sys.exit(1)

print("Building data dictionary...")

# Define data dictionary
data_dictionary = {
    'Column Name': [
        'referral_details_id',
        'referral_id',
        'referral_source',
        'referral_source_category',
        'referral_at',
        'referrer_id',
        'referrer_name',
        'referrer_phone_number',
        'referrer_homeclub',
        'referee_id',
        'referee_name',
        'referee_phone',
        'referral_status',
        'num_reward_days',
        'transaction_id',
        'transaction_status',
        'transaction_at',
        'transaction_location',
        'transaction_type',
        'updated_at',
        'reward_granted_at',
        'is_business_logic_valid'
    ],
    'Data Type': [
        'INTEGER',
        'TEXT',
        'TEXT',
        'TEXT',
        'DATETIME',
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
        'INTEGER',
        'TEXT',
        'TEXT',
        'DATETIME',
        'TEXT',
        'TEXT',
        'DATETIME',
        'DATETIME',
        'BOOLEAN'
    ],
    'Description': [
        'Unique identifier for each referral detail record. This is an auto-generated number.',
        'Unique code assigned to each referral. This is the main identifier for tracking a referral.',
        'How the referral was made. Values: "User Sign Up" (online), "Draft Transaction" (in-store), or "Lead" (marketing campaign).',
        'Simplified category of referral source. Values: "Online" or "Offline".',
        'The exact date and time when the referral was created (in local timezone).',
        'Unique identifier of the person who referred the new user.',
        'Full name of the person who referred the new user.',
        'Contact phone number of the person who made the referral.',
        'The gym location where the referrer is a member.',
        'Unique identifier of the person who was referred (new user).',
        'Full name of the person who was referred.',
        'Contact phone number of the person who was referred.',
        'Current status of the referral. Values: "Berhasil" (Successful), "Menunggu" (Pending), "Tidak Berhasil" (Failed).',
        'Number of days awarded as reward. For example, 10 means 10 days free membership.',
        'Unique code of the transaction linked to this referral.',
        'Payment status of the transaction. Value: "Paid" or "No Transaction".',
        'The exact date and time when the transaction was completed (in local timezone).',
        'The gym location where the transaction took place.',
        'Type of transaction. Values: "New" (new membership) or "Rejoin" (returning member).',
        'The date and time when the referral record was last updated.',
        'The date and time when the reward was actually given to the referee.',
        'Indicates if this referral passed all fraud detection checks. TRUE = Valid referral, FALSE = Potential fraud detected.'
    ],
    'Business Rules': [
        'Always unique. System-generated.',
        'Always unique. Cannot be null.',
        'Must be one of three values: User Sign Up, Draft Transaction, or Lead.',
        'Derived from referral_source. Either Online or Offline.',
        'Cannot be null. Must be a valid date/time.',
        'Must exist in user system. Can be null for some transaction types.',
        'Formatted in Title Case (First Letter Capitalized).',
        'Valid phone number format.',
        'Must be a valid gym location. UPPERCASE format preserved.',
        'Can be null if referral source is not "Lead".',
        'Formatted in Title Case.',
        'Valid phone number format.',
        'Must be one of: Berhasil, Menunggu, or Tidak Berhasil.',
        'Zero or positive integer. Zero means no reward.',
        'Can be null if transaction has not occurred yet.',
        'Either "Paid" or "No Transaction".',
        'Can be null if no transaction yet. Must be after referral_at.',
        'Must be a valid gym location.',
        'Either "New" or "Rejoin" or "No Transaction".',
        'Must be same or after referral_at.',
        'Only filled when reward is actually granted.',
        'TRUE if all fraud checks passed, FALSE otherwise.'
    ],
    'Example Values': [
        '1, 2, 3, 4...',
        '9331c8f144dad5a3b8e4a10467b4343a',
        'User Sign Up, Draft Transaction, Lead',
        'Online, Offline',
        '2024-05-15 14:35:00',
        '2c71c5d66c7e12a0b3c200ba6ed3b78e',
        'John Doe, Jane Smith',
        '123-456-7890, 987-654-3210',
        'PERMATA HIJAU, BENHIL, BLOK M',
        'f12348hbsdkjkfhkjdf',
        'Michael Johnson, Sarah Williams',
        '555-123-4567',
        'Berhasil, Menunggu, Tidak Berhasil',
        '0, 10, 15, 20',
        '1d1eb8a9e864a1cccb2d850398461807',
        'Paid, No Transaction',
        '2024-05-20 10:00:00',
        'BENHIL, ARTERI PONDOK INDAH',
        'New, Rejoin, No Transaction',
        '2024-05-21 12:00:00',
        '2024-05-30 14:00:00',
        'TRUE, FALSE'
    ],
    'Used By': [
        'System tracking and reporting',
        'Customer service, Operations team',
        'Marketing team for campaign analysis',
        'Marketing team for channel analysis',
        'Operations team for timeline tracking',
        'Customer service, Rewards team',
        'Customer service for contact',
        'Customer service for contact',
        'Operations team for location analysis',
        'Customer service, Operations team',
        'Customer service for contact',
        'Customer service for contact',
        'Rewards team for approval workflow',
        'Rewards team for reward distribution',
        'Finance team for payment tracking',
        'Finance team for payment verification',
        'Finance team for transaction tracking',
        'Operations team for location analysis',
        'Operations team for membership type',
        'System audit and tracking',
        'Rewards team for fulfillment tracking',
        'Fraud detection team, Compliance team'
    ]
}

print(f"  - Main dictionary created with {len(data_dictionary['Column Name'])} columns")

# Create DataFrame
df_dict = pd.DataFrame(data_dictionary)
print(f"  ✓ Dictionary DataFrame created")

# Create explanation sheet
explanation = {
    'Section': [
        'Document Purpose',
        'How to Use',
        'Fraud Detection',
        'Status Meanings',
        'Contact'
    ],
    'Details': [
        'This data dictionary explains every column in the Referral Fraud Detection Report. It is designed for non-technical users like Marketing Managers, Operations Teams, and Business Analysts.',
        
        'Use this document to understand what each column means, what values it can have, and how it is used in business operations. The "Business Rules" column explains any restrictions or validations applied to the data.',
        
        'The "is_business_logic_valid" column is the most important for fraud detection. FALSE values indicate referrals that failed one or more validation checks and should be reviewed by the compliance team.',
        
        'Berhasil = Successful (reward should be given)\nMenunggu = Pending (waiting for transaction)\nTidak Berhasil = Failed (no reward)',
        
        'For questions about this report or data dictionary, contact the Data Engineering team or your IT support.'
    ]
}

df_explanation = pd.DataFrame(explanation)
print(f"  ✓ Explanation sheet created")

# Fraud detection rules
fraud_rules = {
    'Check Type': [
        'Reward Without Success',
        'Reward Without Transaction',
        'Transaction Without Reward',
        'Success Without Reward',
        'Transaction Before Referral',
        'Expired Membership',
        'Deleted Account',
        'Different Month',
        'Reward Not Granted'
    ],
    'Description': [
        'A reward was assigned but the referral status is not "Berhasil" (Successful).',
        'A reward was assigned but there is no transaction ID linked to the referral.',
        'A paid transaction exists but no reward was assigned to the referrer.',
        'The referral status is "Berhasil" but the reward value is zero or null.',
        'The transaction date is before the referral creation date (impossible scenario).',
        'The referrer\'s membership had expired when the referral was made.',
        'The referrer\'s account has been deleted.',
        'The transaction occurred in a different month than the referral.',
        'The reward has not been granted even though the referral is successful.'
    ],
    'Action Required': [
        'Review by Compliance team',
        'Review by Operations team',
        'Review by Rewards team',
        'Review by Rewards team',
        'Investigate - Critical issue',
        'Review eligibility',
        'Investigate account status',
        'Review reward policy',
        'Process reward distribution'
    ]
}

df_fraud_rules = pd.DataFrame(fraud_rules)
print(f"  ✓ Fraud rules sheet created with {len(fraud_rules['Check Type'])} rules")

print("\nSaving to Excel...")

# Save to Excel with multiple sheets
output_file = os.path.join(OUTPUT_DIR, 'data_dictionary.xlsx')

try:
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Write explanation first
        print("  - Writing 'How to Use' sheet...")
        df_explanation.to_excel(writer, sheet_name='How to Use', index=False)
        
        # Write main dictionary
        print("  - Writing 'Data Dictionary' sheet...")
        df_dict.to_excel(writer, sheet_name='Data Dictionary', index=False)
        
        # Write fraud rules
        print("  - Writing 'Fraud Detection Rules' sheet...")
        df_fraud_rules.to_excel(writer, sheet_name='Fraud Detection Rules', index=False)
        
        # Format columns
        print("  - Formatting columns...")
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print()
    print("=" * 80)
    print("✓ Data Dictionary created successfully!")
    print(f"✓ Saved to: {output_file}")
    print("=" * 80)
    print("\nThe dictionary includes:")
    print("  - How to Use: Guide for business users")
    print("  - Data Dictionary: Complete column definitions (22 columns)")
    print("  - Fraud Detection Rules: Explanation of validation checks (9 rules)")
    print()
    print("Script completed successfully!")
    
except Exception as e:
    print(f"\n✗ Error saving Excel file: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\nPress Enter to exit...")
input()