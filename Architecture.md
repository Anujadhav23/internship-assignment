# System Architecture - Referral Program Fraud Detection Pipeline

## 📋 Overview
This document outlines the complete architecture and data flow of the Referral Program Fraud Detection Pipeline. The system processes referral data from multiple sources, applies business logic validation, and generates a comprehensive fraud detection report.

---

## 🏗️ Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│              INPUT LAYER (7 CSV Files)                      │
├─────────────────────────────────────────────────────────────┤
│ • lead_log(in).csv                                          │
│ • paid_transactions(in).csv                                 │
│ • referral_rewards(in).csv                                  │
│ • user_logs(in).csv                                         │
│ • user_referral_logs(in).csv                                │
│ • user_referral_statuses(in).csv                            │
│ • user_referrals(in).csv                                    │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  v
┌─────────────────────────────────────────────────────────────┐
│          DATA CLEANING & TRANSFORMATION LAYER               │
├─────────────────────────────────────────────────────────────┤
│ • Replace null values with NaN                              │
│ • Remove duplicate records                                  │
│ • Validate data types and formats                           │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  v
┌─────────────────────────────────────────────────────────────┐
│           DATA PROCESSING & ENRICHMENT LAYER                │
├─────────────────────────────────────────────────────────────┤
│ • Timezone conversion to local time                         │
│ • Extract reward days from timestamps                       │
│ • Normalize string values (InitCap)                         │
│ • Determine referral source category                        │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  v
┌─────────────────────────────────────────────────────────────┐
│              DATA JOINING & AGGREGATION LAYER               │
├─────────────────────────────────────────────────────────────┤
│ Core Table: user_referrals                                  │
│     ├── + referral_logs                                     │
│     ├── + referral_statuses                                 │
│     ├── + referral_rewards                                  │
│     ├── + paid_transactions                                 │
│     ├── + user_logs                                         │
│     └── + lead_logs                                         │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  v
┌─────────────────────────────────────────────────────────────┐
│          BUSINESS LOGIC VALIDATION LAYER                    │
├─────────────────────────────────────────────────────────────┤
│ • Apply fraud detection rules                               │
│ • Validate referral conditions                              │
│ • Generate fraud reason explanations                        │
│ • Flag suspicious transactions                              │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  v
┌─────────────────────────────────────────────────────────────┐
│              OUTPUT LAYER (Final Report)                    │
├─────────────────────────────────────────────────────────────┤
│ • referral_fraud_detection_report.csv                       │
│ • Data profiling summary (optional)                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow Pipeline

### **Stage 1: Data Ingestion**
- Load 7 CSV files from `/data/` directory
- Validate file existence and schema
- Handle missing or corrupted files gracefully

### **Stage 2: Data Cleaning**
- Remove null/empty values
- Eliminate duplicate records
- Standardize data formats
- Ensure data type consistency

### **Stage 3: Data Processing**
- Convert timestamps to local timezone
- Calculate reward duration (days)
- Extract temporal features
- Normalize string fields

### **Stage 4: Data Integration**
Merge all tables using referral IDs as keys:
- **Base Table**: `user_referrals` (main referral records)
- **Dimension 1**: `referral_logs` (referral activity logs)
- **Dimension 2**: `referral_statuses` (status history)
- **Dimension 3**: `referral_rewards` (reward details)
- **Dimension 4**: `paid_transactions` (transaction records)
- **Dimension 5**: `user_logs` (user activity)
- **Dimension 6**: `lead_logs` (lead information)

### **Stage 5: Business Logic & Validation**
Apply fraud detection rules to flag suspicious referrals:
- Cross-validate transaction records
- Check reward eligibility criteria
- Detect timing anomalies
- Identify duplicate claims
- Generate fraud reason codes

### **Stage 6: Output Generation**
- Export final report as CSV
- Include fraud status and reason codes
- Maintain data audit trail

---

## 📊 Data Model

### Key Entities
| Entity | Source | Purpose |
|--------|--------|---------|
| Referrals | user_referrals | Core referral data |
| Referrer Info | user_logs | User profile & activity |
| Referee Info | user_logs | User profile & activity |
| Transactions | paid_transactions | Payment validation |
| Rewards | referral_rewards | Reward eligibility |
| Status Changes | referral_statuses | Referral lifecycle |
| Activity Logs | referral_logs | Detailed event logs |
| Lead Info | lead_logs | Lead source & category |

---

## 🛡️ Fraud Detection Logic

The system flags referrals as fraudulent based on:
1. **Transaction Validation** - No corresponding paid transaction
2. **Timing Anomalies** - Impossible referral/transaction sequences
3. **Duplicate Claims** - Multiple rewards for same action
4. **Eligibility Violations** - User ineligible for reward category
5. **Data Inconsistencies** - Conflicting status records

---

## 📁 Module Architecture

```
src/
├── main_pipeline.py
│   ├── Data Loading
│   ├── Data Cleaning
│   ├── Data Processing
│   ├── Table Joining
│   ├── Fraud Detection
│   └── Output Generation
│
└── data_profiling.py
    ├── Data Quality Analysis
    ├── Statistical Summary
    └── Profiling Report
```

---

## ⚙️ Technology Stack
- **Language**: Python 3.x
- **Data Processing**: Pandas, NumPy
- **Input Format**: CSV
- **Output Format**: CSV
- **Containerization**: Docker (optional)

---

## 🔍 Quality Assurance
- Data validation at each stage
- Duplicate removal mechanisms
- Null value handling
- Timestamp normalization
- Output consistency checks

---

## 📝 Notes
- All timestamps are converted to local timezone for consistency
- String values are normalized using InitCap for readability
- Referral source is determined by signup method or lead category
- Fraud reasons are clearly documented in the output report
