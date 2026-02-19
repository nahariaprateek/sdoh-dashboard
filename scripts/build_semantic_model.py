import pandas as pd
import os

default_src = "data/sdoh_data_adjusted.csv" if os.path.exists("data/sdoh_data_adjusted.csv") else "data/sdoh_data.csv"
SRC = os.environ.get("SDOH_SOURCE", default_src)
OUT_DIR = "data/model"

os.makedirs(OUT_DIR, exist_ok=True)

df = pd.read_csv(SRC)

member_cols = [
    "member","member_name","age","age_group","age_class","gender","race",
    "hp","hp_name","pcp_x","grp_name","plan","segment","agent","address",
    "county","state","county_clean","county_fips","zip","contract"
]
member_cols = [c for c in member_cols if c in df.columns]

dim_member = df[member_cols].drop_duplicates(subset=["member"])

dim_zip_cols = [c for c in ["zip","county","state","county_clean","county_fips"] if c in df.columns]

dim_zip = df[dim_zip_cols].drop_duplicates(subset=["zip"])

dim_plan_cols = [c for c in ["plan","segment","hp","hp_name"] if c in df.columns]

dim_plan = df[dim_plan_cols].drop_duplicates(subset=["plan"])

dim_contract_cols = [c for c in ["contract"] if c in df.columns]

dim_contract = df[dim_contract_cols].drop_duplicates(subset=["contract"])

risk_cols = [c for c in ["member","risk_score_x","risk_full","risk_no_sdoh","sdoh_lift","sdoh_lift_level"] if c in df.columns]

fact_member_risk = df[risk_cols].copy()

clinical_cols = [c for c in [
    "member","compliance","compliance_2023","compliance_hba1c","compliancebcs",
    "pcp_visits","no_ip_visits_2023","a1c_value","ldl_value","bmi","bp_systolic","bp_diastolic"
] if c in df.columns]

fact_member_clinical = df[clinical_cols].copy()

sdoh_cols = [c for c in [
    "member","income_weighted_index","income_inequality","per_capita_income","education_score",
    "labor_market_hardship","housing_instability","car_access_risk","mean_commute",
    "commute_hardship_index","transit_dependency","food_insecurity_index","health_access_score",
    "digital_disadvantage","social_isolation_index","environmental_burden","rurality_index"
] if c in df.columns]

fact_member_sdoh = df[sdoh_cols].copy()

rows = []
for _, row in df.iterrows():
    mid = row["member"]
    for t in ["sdoh", "nonsdoh"]:
        for i in range(1, 6):
            name = row.get(f"{t}_driver_{i}")
            val = row.get(f"{t}_driver_{i}_value")
            if pd.isna(name) or name == "":
                continue
            rows.append({
                "member": mid,
                "driver_type": t,
                "driver_rank": i,
                "driver_name": name,
                "driver_value": val
            })

fact_member_drivers = pd.DataFrame(rows)

# Save star schema

dim_member.to_csv(os.path.join(OUT_DIR, "dim_member.csv"), index=False)
dim_zip.to_csv(os.path.join(OUT_DIR, "dim_zip.csv"), index=False)
dim_plan.to_csv(os.path.join(OUT_DIR, "dim_plan.csv"), index=False)
dim_contract.to_csv(os.path.join(OUT_DIR, "dim_contract.csv"), index=False)

fact_member_risk.to_csv(os.path.join(OUT_DIR, "fact_member_risk.csv"), index=False)
fact_member_clinical.to_csv(os.path.join(OUT_DIR, "fact_member_clinical.csv"), index=False)
fact_member_sdoh.to_csv(os.path.join(OUT_DIR, "fact_member_sdoh.csv"), index=False)
fact_member_drivers.to_csv(os.path.join(OUT_DIR, "fact_member_drivers.csv"), index=False)

# Build member view for dashboard
member_view = dim_member.merge(fact_member_risk, on="member", how="left")\
    .merge(fact_member_clinical, on="member", how="left")\
    .merge(fact_member_sdoh, on="member", how="left")

for t in ["sdoh", "nonsdoh"]:
    for i in range(1, 6):
        name_col = f"{t}_driver_{i}"
        val_col = f"{t}_driver_{i}_value"
        member_view[name_col] = df[name_col].values
        member_view[val_col] = df[val_col].values

member_view.to_csv(os.path.join(OUT_DIR, "member_view.csv"), index=False)

print("Wrote star schema + member_view to", OUT_DIR)
