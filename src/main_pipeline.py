"""
Referral Program Data Pipeline
Purpose: Process referral data and detect potential fraud with reasons
Author: Data Engineer Intern
Company: Springer Capital
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import pytz
import sys

print("=" * 80)
print("REFERRAL PROGRAM DATA PIPELINE")
print("=" * 80)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Python version: {sys.version}")
print(f"Pandas version: {pd.__version__}")
print(f"Current directory: {os.getcwd()}\n")

# CONFIG
DATA_DIR = 'data'
OUTPUT_DIR = 'output'
os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"✓ Output directory: {OUTPUT_DIR}\n")

# STEP 1 — LOAD FILES
print("STEP 1: Loading CSV files...")

lead_logs = pd.read_csv(os.path.join(DATA_DIR, 'lead_log(in).csv'))
user_referrals = pd.read_csv(os.path.join(DATA_DIR, 'user_referrals(in).csv'))
user_referral_logs = pd.read_csv(os.path.join(DATA_DIR, 'user_referral_logs(in).csv'))
user_logs = pd.read_csv(os.path.join(DATA_DIR, 'user_logs(in).csv'))
user_referral_statuses = pd.read_csv(os.path.join(DATA_DIR, 'user_referral_statuses(in).csv'))
referral_rewards = pd.read_csv(os.path.join(DATA_DIR, 'referral_rewards(in).csv'))
paid_transactions = pd.read_csv(os.path.join(DATA_DIR, 'paid_transactions(in).csv'))

print("  ✓ All files loaded.\n")

# STEP 2 — CLEANING
print("STEP 2: Cleaning data...")

for df in [lead_logs, user_referrals, user_referral_logs, user_logs,
           user_referral_statuses, referral_rewards, paid_transactions]:
    df.replace(['null', ''], np.nan, inplace=True)

user_logs_clean = user_logs.drop_duplicates(subset=['user_id'], keep='first')
lead_logs_clean = lead_logs.sort_values('created_at').drop_duplicates(subset=['lead_id'], keep='last')

print("  ✓ Cleaned & removed duplicates\n")

# STEP 3 — TIME PROCESSING
print("STEP 3: Processing data...")

def convert_utc_to_local(utc_time_str, timezone_str):
    if pd.isna(utc_time_str) or pd.isna(timezone_str):
        return None
    try:
        utc_time = pd.to_datetime(utc_time_str, utc=True)
        tz = pytz.timezone(timezone_str)
        return utc_time.astimezone(tz).replace(tzinfo=None)
    except:
        return None

paid_transactions['transaction_at_local'] = paid_transactions.apply(
    lambda r: convert_utc_to_local(r['transaction_at'], r['timezone_transaction']), axis=1
)

lead_logs_clean['created_at_local'] = lead_logs_clean.apply(
    lambda r: convert_utc_to_local(r['created_at'], r['timezone_location']), axis=1
)

referral_rewards['num_reward_days'] = referral_rewards['reward_value'].apply(
    lambda v: int(str(v).split()[0]) if pd.notna(v) else None
)

print("  ✓ Time conversion & reward parsing complete\n")

# STEP 4 — JOIN TABLES
print("STEP 4: Joining tables...")

df = user_referrals.copy()

latest_logs = user_referral_logs.sort_values("created_at").drop_duplicates(
    subset=["user_referral_id"], keep="last"
)

df = df.merge(latest_logs, left_on='referral_id', right_on='user_referral_id', how='left')
df = df.merge(user_referral_statuses[['id', 'description']], 
              left_on='user_referral_status_id', right_on='id', how='left')
df.rename(columns={'description': 'referral_status'}, inplace=True)

df = df.merge(referral_rewards[['id', 'num_reward_days']], 
              left_on='referral_reward_id', right_on='id', how='left')

df = df.merge(
    paid_transactions[['transaction_id', 'transaction_status', 'transaction_at_local',
                       'transaction_location', 'transaction_type']],
    on='transaction_id', how='left'
)

df = df.merge(
    user_logs_clean[['user_id', 'name', 'phone_number', 'homeclub',
                     'timezone_homeclub', 'membership_expired_date', 'is_deleted']],
    left_on='referrer_id', right_on='user_id', how='left'
)

df.rename(columns={
    'name': 'referrer_name',
    'phone_number': 'referrer_phone_number',
    'homeclub': 'referrer_homeclub',
    'timezone_homeclub': 'referrer_timezone',
    'membership_expired_date': 'referrer_membership_expired',
    'is_deleted': 'referrer_is_deleted'
}, inplace=True)

df = df.merge(
    lead_logs_clean[['lead_id', 'source_category', 'timezone_location']],
    left_on='referee_id', right_on='lead_id', how='left'
)

print("  ✓ Joined tables successfully\n")

# STEP 5 — REFERRAL TIMESTAMPS
print("STEP 5: Adjusting timestamps...")

df['referral_at_local'] = df.apply(
    lambda r: convert_utc_to_local(
        r['referral_at'], 
        r['referrer_timezone'] if pd.notna(r['referrer_timezone']) 
        else r['timezone_location']
    ),
    axis=1
)

df['updated_at_local'] = df.apply(
    lambda r: convert_utc_to_local(
        r['updated_at'], 
        r['referrer_timezone'] if pd.notna(r['referrer_timezone']) else 'Asia/Jakarta'
    ),
    axis=1
)

df['reward_granted_at'] = df.apply(
    lambda r: convert_utc_to_local(
        r['created_at'], 
        r['referrer_timezone'] if pd.notna(r['referrer_timezone']) else 'Asia/Jakarta'
    ),
    axis=1
)

print("  ✓ Timestamp conversion complete\n")

# STEP 6 — SOURCE CATEGORY
print("STEP 6: Determining referral source...")

def get_source_category(row):
    if row['referral_source'] == 'User Sign Up': return 'Online'
    if row['referral_source'] == 'Draft Transaction': return 'Offline'
    if row['referral_source'] == 'Lead': return row['source_category']
    return None

df['referral_source_category'] = df.apply(get_source_category, axis=1)

print("  ✓ Referral source category assigned\n")

# STEP 7 — INITCAP
print("STEP 7: Normalizing text...")

for col in ['referrer_name', 'referee_name', 'referral_status',
            'transaction_status', 'transaction_type',
            'referral_source', 'referral_source_category']:
    df[col] = df[col].apply(lambda v: str(v).title() if pd.notna(v) else v)

print("  ✓ String normalization done\n")

# STEP 8 — FRAUD DETECTION + REASON
print("STEP 8: Running fraud detection rules...")

df['referrer_membership_expired'] = pd.to_datetime(
    df['referrer_membership_expired'], errors='coerce'
)

def fraud_reason(row):
    reward = row['num_reward_days']
    status = row['referral_status']
    tid = row['transaction_id']
    tstatus = row['transaction_status']
    ttype = row['transaction_type']
    ra = row['referral_at_local']
    ta = row['transaction_at_local']
    exp = row['referrer_membership_expired']
    deleted = str(row['referrer_is_deleted']).upper() == "TRUE"
    granted = str(row['is_reward_granted']).upper() == "TRUE"

    if pd.notna(reward) and reward > 0 and status != 'Berhasil':
        return "Reward > 0 but status not Berhasil"
    if pd.notna(reward) and reward > 0 and pd.isna(tid):
        return "Reward > 0 but no transaction ID"
    if (pd.isna(reward) or reward == 0) and pd.notna(tid) and tstatus == "Paid":
        return "Paid transaction but reward = 0"
    if pd.notna(ra) and pd.notna(ta) and ta < ra:
        return "Transaction before referral"
    if pd.notna(exp) and pd.notna(ra) and exp <= ra:
        return "Membership expired before referral"
    if pd.notna(reward) and reward > 0 and deleted:
        return "Account deleted but reward given"
    if pd.notna(reward) and reward > 0 and pd.notna(ra) and pd.notna(ta):
        if ra.month != ta.month or ra.year != ta.year:
            return "Transaction month does not match referral month"
    if status == 'Berhasil' and (pd.isna(reward) or reward == 0):
        return "Successful status but no reward"
    if pd.notna(reward) and reward > 0 and status == 'Berhasil' and not granted:
        return "Reward not granted but status Berhasil"

    return None

df['fraud_reason'] = df.apply(fraud_reason, axis=1)
df['is_business_logic_valid'] = df['fraud_reason'].isna()

print(f"  ✓ Valid referrals: {df['is_business_logic_valid'].sum()}")
print(f"  ✓ Invalid referrals: {(~df['is_business_logic_valid']).sum()}\n")

def get_fraud_reason(row):
    reward_value = row['num_reward_days']
    referral_status = row['referral_status']
    transaction_id = row['transaction_id']
    transaction_status = row['transaction_status']
    transaction_type = row['transaction_type']
    referral_at = row['referral_at_local']
    transaction_at = row['transaction_at_local']
    membership_expired = row['referrer_membership_expired']
    is_deleted = row['referrer_is_deleted']
    is_reward_granted = row['is_reward_granted']

    # Convert boolean-like strings
    if isinstance(is_deleted, str):
        is_deleted = is_deleted.upper() == 'TRUE'
    if isinstance(is_reward_granted, str):
        is_reward_granted = is_reward_granted.upper() == 'TRUE'

    # -------- FRAUD RULES ---------

    if pd.notna(reward_value) and reward_value > 0 and referral_status != 'Berhasil':
        return "Reward > 0 but status not Berhasil"

    if pd.notna(reward_value) and reward_value > 0 and pd.isna(transaction_id):
        return "Reward > 0 but no transaction ID"

    if (pd.isna(reward_value) or reward_value == 0) and pd.notna(transaction_id) and transaction_status == 'Paid':
        return "Paid transaction but reward = 0"

    if referral_status == 'Berhasil' and (pd.isna(reward_value) or reward_value == 0):
        return "Status Berhasil but reward = 0"

    if pd.notna(referral_at) and pd.notna(transaction_at) and transaction_at < referral_at:
        return "Transaction date earlier than referral date"

    if pd.notna(reward_value) and reward_value > 0 and pd.notna(membership_expired) and membership_expired <= referral_at:
        return "Membership expired before referral"

    if pd.notna(reward_value) and reward_value > 0 and is_deleted:
        return "Referrer account deleted"

    if (pd.notna(referral_at) and pd.notna(transaction_at) and
        (referral_at.month != transaction_at.month or referral_at.year != transaction_at.year)):
        return "Transaction & referral in different month"

    if (pd.notna(reward_value) and reward_value > 0 and 
        referral_status == 'Berhasil' and not is_reward_granted):
        return "Reward not granted but status Berhasil"

    return None  # Means valid
df['fraud_reason'] = df.apply(get_fraud_reason, axis=1)


# STEP 9 — FINAL OUTPUT
print("STEP 9: Preparing final output...")

final_df = df[[
    'id', 'referral_id', 'referral_source', 'referral_source_category',
    'referral_at_local', 'referrer_id', 'referrer_name', 'referrer_phone_number',
    'referrer_homeclub', 'referee_id', 'referee_name', 'referee_phone',
    'referral_status', 'num_reward_days', 'transaction_id', 'transaction_status',
    'transaction_at_local', 'transaction_location', 'transaction_type',
    'updated_at_local', 'reward_granted_at', 'is_business_logic_valid',
    'fraud_reason'
]].copy()


final_df.rename(columns={
    'id': 'referral_details_id',
    'referral_at_local': 'referral_at',
    'transaction_at_local': 'transaction_at',
    'updated_at_local': 'updated_at'
}, inplace=True)


print(f"  ✓ Final dataset rows: {len(final_df)}\n")

# STEP 10 — SAVE OUTPUT
print("STEP 10: Saving output report...")

output_file = os.path.join(OUTPUT_DIR, 'referral_fraud_detection_report.csv')
final_df.to_csv(output_file, index=False)

print(f"  ✓ Report saved to: {output_file}")
print("\nPipeline Completed Successfully!")
print("=" * 80)
input("Press Enter to exit...")
