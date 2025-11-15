# Referral Program Data Pipeline – Take Home Test
Author: Data Engineer Intern  
Company: Springer Capital  

---

## 📌 Project Overview
This project processes a referral program dataset consisting of 7 CSV tables.  
The pipeline:

- Profiles all tables  
- Cleans & joins all data  
- Converts timestamps  
- Normalizes text  
- Applies business fraud rules  
- Generates:
  - Data Profiling Report (Excel)
  - Referral Fraud Detection Report (CSV)
  - Data Dictionary (Excel)

Designed in a *production-style* using Docker.

---

# 📂 Folder Structure

project/
│── src/
│ ├── data_profiling.py
│ ├── main_pipeline.py
│── data/
│── output/
│── docs/
│── Dockerfile
│── requirements.txt
│── README.md


---

# 🚀 How to Run Using Docker (Recommended)

## 1. Build Docker Image
docker build -t referral_pipeline .

## 2. Run the Pipeline
docker run -it --rm -v "%cd%/output":/app/output referral_pipeline


This automatically runs:

1. `data_profiling.py`
2. `main_pipeline.py`

All results appear in your local `output/` folder.

---

# 🧪 How to Run Without Docker (Local Run)

pip install -r requirements.txt
python src/data_profiling.py
python src/main_pipeline.py


Outputs are created inside `/output`.

---

# 📊 Output Files

### **1) Data Profiling Report (Excel)**
Generated at:
output/data_profiling_report.xlsx


Includes:
- Null counts
- Distinct counts
- Data types
- Sample values

---

### **2) Referral Fraud Detection Report (CSV)**
Generated at:
output/referral_fraud_detection_report.csv


Columns include:
- referral_id  
- referrer info  
- referee info  
- transaction info  
- reward info  
- fraud validation result  
- fraud reason  

---

### **3) Data Dictionary (Excel)**
Located in:
docs/data_dictionary.xlsx


Contains:
- Column descriptions  
- Rules explanation  
- Business meaning  

---

# 🔍 Business Logic Summary (Fraud Rules)
Referral is marked valid only when:

1. Reward > 0  
2. Status = “Berhasil”  
3. Paid transaction exists  
4. Transaction happens after referral  
5. Same month  
6. Membership active  
7. Reward granted  
8. User not deleted  
9. No contradictory status  

Otherwise, referral is flagged invalid with a fraud reason.

---

# 🛠 Troubleshooting

### ⛔ Missing timezone?
→ Default = Asia/Jakarta  

### ⛔ output folder empty?
Ensure Docker volume mount is correct.

### ⛔ Windows PowerShell mount example:
docker run -it --rm -v "${pwd}/output:/app/output" referral_pipelin


---

# ✔ Submission Ready

All components required by the assignment are complete:
- Data profiling script ✔  
- Main pipeline script ✔  
- Fraud logic ✔  
- Fraud reason ✔  
- Data dictionary ✔  
- Dockerfile ✔  
- README ✔  
- Clean prints ✔  

You can zip the entire project and submit.

Internship_Project/
│── src/
│   ├── data_profiling.py
│   ├── main_pipeline.py
│── data/
│   ├── lead_log(in).csv
│   ├── user_referrals(in).csv
│   ├── user_referral_logs(in).csv
│   ├── user_logs(in).csv
│   ├── user_referral_statuses(in).csv
│   ├── referral_rewards(in).csv
│   ├── paid_transactions(in).csv
│── output/         ← auto-created
│── docs/
│   ├── data_dictionary.xlsx
│── requirements.txt
│── Dockerfile
│── README.md
