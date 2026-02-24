# Data Dictionary for sdoh-dashboard/data

## Semantic Model (Star Schema)

All dashboard data is sourced from CSVs under `data/model/`. The dashboard reads the **member view** (`data/model/member_view.csv`) which is a joined view across the star schema for UI performance. The star schema tables are still the source of truth and can be regenerated from `data/sdoh_data.csv`.

### Dimensions

- `data/model/dim_member.csv`
  - Keys: `member`
  - Attributes: member demographics and membership attributes (`member_name`, `age`, `age_group`, `age_class`, `gender`, `race`, `plan`, `segment`, `hp`, `hp_name`, `pcp_x`, `grp_name`, `agent`, `address`, `county`, `state`, `county_clean`, `county_fips`, `zip`, `contract`)
- `data/model/dim_zip.csv`
  - Keys: `zip`
  - Attributes: `county`, `state`, `county_clean`, `county_fips`
- `data/model/dim_plan.csv`
  - Keys: `plan`
  - Attributes: `segment`, `hp`, `hp_name`
- `data/model/dim_contract.csv`
  - Keys: `contract`

### Facts

- `data/model/fact_member_risk.csv`
  - Keys: `member`
  - Measures: `risk_score_x`, `risk_full`, `risk_no_sdoh`, `sdoh_lift`, `sdoh_lift_level`
- `data/model/fact_member_clinical.csv`
  - Keys: `member`
  - Measures: `compliance`, `compliance_2023`, `compliance_hba1c`, `compliancebcs`, `pcp_visits`, `no_ip_visits_2023`, `a1c_value`, `ldl_value`, `bmi`, `bp_systolic`, `bp_diastolic`
- `data/model/fact_member_sdoh.csv`
  - Keys: `member`
  - Measures: SDOH indices (`income_weighted_index`, `income_inequality`, `per_capita_income`, `education_score`, `labor_market_hardship`, `housing_instability`, `car_access_risk`, `mean_commute`, `commute_hardship_index`, `transit_dependency`, `food_insecurity_index`, `health_access_score`, `digital_disadvantage`, `social_isolation_index`, `environmental_burden`, `rurality_index`)
- `data/model/fact_member_drivers.csv`
  - Keys: `member`, `driver_type`, `driver_rank`
  - Measures: `driver_name`, `driver_value` (one row per driver position)

### Dashboard View (joined)

- `data/model/member_view.csv`
  - Flattened join of dims + fact tables for dashboard consumption.

---

## Legacy Source (raw)

The raw file `data/sdoh_data.csv` is retained as source input and can be used to regenerate the star schema. The sections below describe its columns.

To rebuild the semantic model:

```bash
python scripts/build_semantic_model.py
```

# Data Dictionary for sdoh-dashboard/data/sdoh_data.csv

Rows: 2329

Columns: 74

Note: The documentation file has a data dictionary table with empty descriptions. The details below are derived from the CSV data itself (types, null rates, ranges) and field names.

| Field | Type | Non-null % | Unique | Example | Numeric stats (min/median/mean/max) |
| --- | --- | ---: | ---: | --- | --- |
| member | text | 100.0 | 2329 | M01553 |  |
| member_name | text | 100.0 | 400 | Aiden Martinez |  |
| age | number | 100.0 | 73 | 47 | 18/60.0/60.43795620437956/90 |
| sex | text | 100.0 | 2 | F |  |
| age_group | text | 100.0 | 5 | 18-34 |  |
| age_class | text | 100.0 | 3 | 45–64 |  |
| gender | text | 100.0 | 2 | Female |  |
| race | text | 100.0 | 5 | Black |  |
| hp | text | 100.0 | 4 | Aetna Silver Plus |  |
| hp_name | text | 100.0 | 4 | Aetna Silver Plus |  |
| pcp_x | text | 100.0 | 10 | Dr. Nguyen |  |
| grp_name | text | 100.0 | 4 | Hudson Health Group |  |
| plan | text | 100.0 | 3 | HMO |  |
| segment | text | 100.0 | 3 | Medicare |  |
| agent | text | 100.0 | 20 | Agent_10 |  |
| address | text | 100.0 | 2328 | 5016 Walnut Ter, Hoboken |  |
| county | text | 100.0 | 17 | Essex |  |
| state | text | 100.0 | 4 | MA |  |
| county_clean | text | 100.0 | 17 | Essex |  |
| county_fips | number | 100.0 | 20 | 25009 | 9001/34017.0/30325.15027908974/36085 |
| zip | number | 100.0 | 20 | 1810 | 1005/7003.0/6871.911550021468/11201 |
| compliance | number | 100.0 | 1778 | 0.8275 | 0.3023/0.7432/0.7433419493344783/1.0 |
| compliance_2023 | number | 100.0 | 1801 | 0.8893 | 0.2661/0.7458/0.7437367969085444/1.0 |
| compliance_hba1c | number | 100.0 | 1851 | 0.8091 | 0.0/0.7002/0.6957702447402319/1.0 |
| compliancebcs | number | 100.0 | 1857 | 0.8968 | 0.0/0.6487/0.6423350794332331/1.0 |
| pcp_visits | number | 100.0 | 9 | 0 | 0/2.0/2.004293688278231/8 |
| no_ip_visits_2023 | number | 100.0 | 2 | 1 | 0/1.0/0.9231429798196651/1 |
| a1c_value | number | 100.0 | 632 | 5.95 | 5.0/8.24/8.266371833404893/11.5 |
| ldl_value | number | 100.0 | 1176 | 100.6 | 60.0/136.6/136.18166595105194/209.9 |
| bmi | number | 100.0 | 271 | 33.1 | 18.0/31.4/31.48720480893087/45.0 |
| bp_systolic | number | 100.0 | 70 | 161 | 110/144.0/143.8458565908115/179 |
| bp_diastolic | number | 100.0 | 40 | 91 | 70/89.0/89.18806354658652/109 |
| income_weighted_index | number | 100.0 | 20 | 78886.85 | 55051.925/79833.35/79076.14441820525/93975.975 |
| income_inequality | number | 100.0 | 19 | 0.479 | 0.411/0.479/0.47945727780163155/0.54 |
| per_capita_income | number | 100.0 | 20 | 43465.822 | 23333.705/41166.252/42073.7600592529/58531.49 |
| education_score | number | 100.0 | 20 | 11.0942 | -12.6882/10.732/9.404393817088879/19.9678 |
| labor_market_hardship | number | 100.0 | 20 | 16.4204 | 14.7522/16.5778/17.389664233576642/22.8136 |
| housing_instability | number | 100.0 | 20 | 41.2595 | 32.718/39.219/38.51387891799055/46.5075 |
| car_access_risk | number | 100.0 | 20 | 0.0867 | 0.0365/0.0916/0.16412206955775013/0.5502 |
| mean_commute | number | 100.0 | 20 | 32.7515 | 26.6424/35.4936/36.45094774581365/46.8705 |
| commute_hardship_index | number | 100.0 | 20 | 32.7515 | 26.6424/35.4936/36.45094774581365/46.8705 |
| transit_dependency | number | 100.0 | 20 | 0.1527 | 0.0305/0.206/0.20533963074280806/0.3569 |
| food_insecurity_index | number | 100.0 | 20 | 14.006 | 4.316/12.8575/12.939676255903821/36.0305 |
| health_access_score | number | 100.0 | 20 | 2.6324 | 0.3775/1.023/1.4624605839416058/4.2124 |
| digital_disadvantage | number | 100.0 | 19 | 10.506 | 6.847/10.649/10.680054529841135/16.426 |
| social_isolation_index | number | 100.0 | 20 | 18.6666 | 14.81/16.8992/17.162060541004724/20.049 |
| environmental_burden | number | 100.0 | 14 | 5.732 | 0.0/4.3944/3.1386223701159297/8.2664 |
| rurality_index | number | 100.0 | 20 | 1.3768 | 0.9766/1.3696/1.4294055817947617/3.549 |
| risk_score_x | number | 100.0 | 381 | 2.0 | 0.1/2.01/2.0181665951051957/4.5 |
| risk_full | number | 100.0 | 1115 | 2.0299 | 1.8011/2.0158/2.01550493774152/2.2034 |
| risk_no_sdoh | number | 100.0 | 2329 | 1.9900698619865096 | 1.481844235266312/2.039285093748233/1.9904443445686748/2.2276221202559623 |
| sdoh_lift | number | 100.0 | 2329 | 0.0398301380134904 | -0.1194904774694936/-0.0270995661611421/0.025060593172845123/0.4489506287240012 |
| sdoh_lift_level | text | 100.0 | 4 | Mild SDOH Contribution |  |
| sdoh_driver_1 | text | 100.0 | 15 | digital_disadvantage |  |
| sdoh_driver_1_value | number | 100.0 | 211 | 0.0025 | -0.0267/0.0026/-0.0010839845427221986/0.0157 |
| sdoh_driver_2 | text | 100.0 | 15 | health_access_score |  |
| sdoh_driver_2_value | number | 100.0 | 154 | -0.0021 | -0.0146/0.0024/-0.00019652211249463289/0.0082 |
| sdoh_driver_3 | text | 100.0 | 15 | commute_hardship_index |  |
| sdoh_driver_3_value | number | 100.0 | 121 | -0.0013 | -0.0102/0.002/0.00020145985401459854/0.0074 |
| sdoh_driver_4 | text | 100.0 | 15 | income_inequality |  |
| sdoh_driver_4_value | number | 100.0 | 107 | -0.0009 | -0.0089/0.0016/0.0003294117647058823/0.0064 |
| sdoh_driver_5 | text | 100.0 | 15 | education_score |  |
| sdoh_driver_5_value | number | 100.0 | 95 | -0.0008 | -0.0073/0.0013/0.0002395878059252898/0.0061 |
| nonsdoh_driver_1 | text | 100.0 | 15 | county_clean |  |
| nonsdoh_driver_1_value | number | 100.0 | 467 | 0.0112 | -0.0409/0.0065/0.0028211249463288963/0.0476 |
| nonsdoh_driver_2 | text | 100.0 | 17 | race |  |
| nonsdoh_driver_2_value | number | 100.0 | 301 | -0.0105 | -0.035/-0.0055/-0.0005158437097466724/0.0254 |
| nonsdoh_driver_3 | text | 100.0 | 18 | hp |  |
| nonsdoh_driver_3_value | number | 100.0 | 235 | 0.0075 | -0.0206/-0.0045/-0.0008060541004723057/0.0197 |
| nonsdoh_driver_4 | text | 100.0 | 18 | plan |  |
| nonsdoh_driver_4_value | number | 100.0 | 194 | -0.0065 | -0.0151/-0.0033/-0.00032129669386002575/0.0155 |
| nonsdoh_driver_5 | text | 100.0 | 18 | gender |  |
| nonsdoh_driver_5_value | number | 100.0 | 163 | -0.0049 | -0.0109/-0.0025/-0.00027282095319879773/0.0139 |
| contract | text | 100.0 | 33 | H3748 |  |
