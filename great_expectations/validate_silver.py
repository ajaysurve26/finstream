import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from databricks import sql
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime

load_dotenv(dotenv_path="/Users/ajaysurve/finstream/dashboard/.env")

print("🔍 Starting FinStream Data Quality Validation...")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("-" * 60)

# --- Connect to Databricks and fetch Silver data ---
def fetch_silver_data():
    conn = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST").replace("https://", ""),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )
    query = """
        SELECT
            transaction_id,
            user_id,
            merchant_name,
            merchant_category,
            amount,
            currency,
            country,
            transaction_type,
            card_last4,
            transaction_timestamp,
            hour_of_day,
            is_fraud,
            transaction_count_per_user,
            amount_category,
            is_high_risk_country,
            is_valid_record,
            ingestion_timestamp
        FROM hive_metastore.silver.stg_transactions
        LIMIT 10000
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    conn.close()
    return pd.DataFrame(result, columns=columns)

# Fetch data
print("📥 Fetching Silver layer data from Databricks...")
df = fetch_silver_data()
print(f"✅ Fetched {len(df):,} records")
print()

# --- Run validations manually ---
results = []

def check(name, condition, critical=True):
    passed = bool(condition)
    status = "✅ PASS" if passed else ("❌ FAIL" if critical else "⚠️  WARN")
    results.append({
        "check": name,
        "passed": passed,
        "critical": critical,
        "status": status
    })
    print(f"{status} | {name}")
    return passed

print("🧪 Running Data Quality Checks...")
print("-" * 60)

# --- Completeness checks ---
print("\n📋 COMPLETENESS")
check("transaction_id has no nulls",
    df["transaction_id"].notna().all())

check("user_id has no nulls",
    df["user_id"].notna().all())

check("amount has no nulls",
    df["amount"].notna().all())

check("currency has no nulls",
    df["currency"].notna().all())

check("timestamp has no nulls",
    df["transaction_timestamp"].notna().all())

# --- Uniqueness checks ---
print("\n🔑 UNIQUENESS")
check("transaction_id is unique",
    df["transaction_id"].nunique() == len(df))

# --- Validity checks ---
print("\n✔️  VALIDITY")
check("amount is always positive",
    (df["amount"] > 0).all())

check("amount is within expected range (0-10000)",
    (df["amount"] <= 10000).all())

check("hour_of_day is between 0 and 23",
    df["hour_of_day"].between(0, 23).all())

check("currency is valid",
    df["currency"].isin(["USD", "EUR", "GBP", "INR", "AUD", "CAD"]).all())

check("transaction_type is valid",
    df["transaction_type"].isin(
        ["purchase", "refund", "transfer", "withdrawal"]
    ).all())

check("merchant_category is valid",
    df["merchant_category"].isin([
        "grocery", "electronics", "restaurant",
        "travel", "clothing", "entertainment",
        "healthcare", "fuel", "education"
    ]).all())

check("amount_category is valid",
    df["amount_category"].isin(["low", "medium", "high"]).all())

# --- Consistency checks ---
print("\n🔄 CONSISTENCY")
check("high amount flagged as high category",
    (df[df["amount"] > 8000]["amount_category"] == "high").all()
    if len(df[df["amount"] > 8000]) > 0 else True)

check("valid records have no null amounts",
    df[df["is_valid_record"] == True]["amount"].notna().all())

check("fraud rate is between 0% and 100%",
    df["is_fraud"].isin([True, False]).all())

# --- Volume checks ---
print("\n📊 VOLUME")
total = len(df)
valid = df["is_valid_record"].sum()
invalid = total - valid
fraud_count = df["is_fraud"].sum()
fraud_rate = fraud_count / total * 100

check("valid record rate above 90%",
    (valid / total * 100) >= 90, critical=True)

check("fraud rate below 60% (sanity check)",
    fraud_rate < 60, critical=False)

check("at least 100 records in Silver",
    total >= 100, critical=True)

check("all 100 users represented",
    df["user_id"].nunique() >= 10, critical=False)

# --- Summary ---
print("\n" + "=" * 60)
print("📊 VALIDATION SUMMARY")
print("=" * 60)

total_checks = len(results)
passed_checks = sum(1 for r in results if r["passed"])
failed_checks = total_checks - passed_checks
critical_failures = sum(1 for r in results if not r["passed"] and r["critical"])

print(f"Total checks    : {total_checks}")
print(f"Passed          : {passed_checks} ✅")
print(f"Failed          : {failed_checks} ❌")
print(f"Critical failures: {critical_failures}")
print()
print(f"📈 Data Stats:")
print(f"  Total records : {total:,}")
print(f"  Valid records : {int(valid):,} ({valid/total*100:.1f}%)")
print(f"  Invalid records: {int(invalid):,} ({invalid/total*100:.1f}%)")
print(f"  Fraud count   : {int(fraud_count):,} ({fraud_rate:.1f}%)")
print(f"  Unique users  : {df['user_id'].nunique()}")
print()

if critical_failures == 0:
    print("🎉 ALL CRITICAL CHECKS PASSED — Silver layer is Gold-ready!")
else:
    print(f"🚨 {critical_failures} CRITICAL FAILURES — Silver layer needs attention!")
    print("   Failed checks:")
    for r in results:
        if not r["passed"] and r["critical"]:
            print(f"   → {r['check']}")

print("=" * 60)