// ==========================
//  DATA INGESTION
// ==========================
let DATA = [];
let INTERVENTION_CHOICES = [];

// ==========================
//  DATA LOADING
// ==========================
const DATA_VERSION = "2026-02-19";
const DATA_SOURCE = "./data/model/member_view.csv?v=" + DATA_VERSION;
const API_SOURCE = "/api/members";
const CONFIG_SOURCES = {
  contracts: "./data/contract_fallbacks.csv",
  riskBands: "./data/risk_bands.csv",
  sdohLevels: "./data/sdoh_level_order.csv",
  interventionPlans: "./data/intervention_plans.csv",
  curatedInterventions: "./data/curated_intervention_keys.csv",
  defaultIntervention: "./data/default_intervention_plan.csv",
  careNavigators: "./data/care_navigators.csv"
};

let CONTRACT_FALLBACKS = [];

async function loadDashboardData() {
  if (typeof window !== "undefined" && window.DASHBOARD_API) {
    const response = await fetch(window.DASHBOARD_API);
    if (!response.ok) {
      throw new Error("Failed to load API data (" + response.status + ")");
    }
    const payload = await response.json();
    const rows = Array.isArray(payload) ? payload : (payload.rows || []);
    return ensureContracts(rows.map(function(row) {
      return Object.assign({}, row);
    }));
  }

  if (typeof window !== "undefined" && Array.isArray(window.RAW_DATA) && window.RAW_DATA.length) {
    return ensureContracts(
      window.RAW_DATA.map(function(row) {
        return Object.assign({}, row);
      })
    );
  }

  const response = await fetch(DATA_SOURCE);
  if (!response.ok) {
    throw new Error("Failed to load CSV (" + response.status + ")");
  }
  const text = await response.text();
  const rows = parseCsv(text);
  if (!rows.length) return [];

  const headers = rows[0];
  const dataRows = rows.slice(1).filter(function(row) {
    return row.some(function(cell) {
      return cell !== undefined && String(cell).trim() !== "";
    });
  });

  return ensureContracts(dataRows.map(function(row) {
    const obj = {};
    headers.forEach(function(header, idx) {
      obj[header] = row[idx] !== undefined ? row[idx] : "";
    });
    return obj;
  }));
}

function parseCsv(text) {
  const rows = [];
  let current = [];
  let value = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (inQuotes) {
      if (char === "\"") {
        if (text[i + 1] === "\"") {
          value += "\"";
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        value += char;
      }
    } else if (char === "\"") {
      inQuotes = true;
    } else if (char === ",") {
      current.push(value);
      value = "";
    } else if (char === "\r") {
      continue;
    } else if (char === "\n") {
      current.push(value);
      rows.push(current);
      current = [];
      value = "";
    } else {
      value += char;
    }
  }

  if (value !== "" || current.length) {
    current.push(value);
    rows.push(current);
  }

  return rows.filter(function(row) { return row.length; });
}

async function loadCsvFromPath(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error("Failed to load CSV (" + response.status + "): " + path);
  }
  const text = await response.text();
  return parseCsv(text);
}

function parseSingleColumn(rows) {
  return rows.slice(1).map(function(row) {
    return String(row[0] || "").trim();
  }).filter(function(value) {
    return value !== "";
  });
}

function parseActions(value) {
  return String(value || "").split("|").map(function(action) {
    return action.trim();
  }).filter(function(action) {
    return action !== "";
  });
}

function requireNonEmpty(label, values) {
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error("Missing required config: " + label);
  }
}

async function loadConfig() {
  let results = null;
  if (typeof window !== "undefined" && window.CONFIG_CSVS) {
    const inline = window.CONFIG_CSVS;
    results = [
      parseCsv(String(inline.contracts || "")),
      parseCsv(String(inline.riskBands || "")),
      parseCsv(String(inline.sdohLevels || "")),
      parseCsv(String(inline.interventionPlans || "")),
      parseCsv(String(inline.curatedInterventions || "")),
      parseCsv(String(inline.defaultIntervention || "")),
      parseCsv(String(inline.careNavigators || ""))
    ];
  } else {
    results = await Promise.all([
      loadCsvFromPath(CONFIG_SOURCES.contracts),
      loadCsvFromPath(CONFIG_SOURCES.riskBands),
      loadCsvFromPath(CONFIG_SOURCES.sdohLevels),
      loadCsvFromPath(CONFIG_SOURCES.interventionPlans),
      loadCsvFromPath(CONFIG_SOURCES.curatedInterventions),
      loadCsvFromPath(CONFIG_SOURCES.defaultIntervention),
      loadCsvFromPath(CONFIG_SOURCES.careNavigators)
    ]);
  }

  const contractRows = results[0];
  const riskRows = results[1];
  const sdohRows = results[2];
  const interventionRows = results[3];
  const curatedRows = results[4];
  const defaultRows = results[5];
  const navigatorRows = results[6];

  CONTRACT_FALLBACKS = parseSingleColumn(contractRows);
  DEFAULT_RISK_BANDS = parseSingleColumn(riskRows);
  SDOH_LEVEL_ORDER = parseSingleColumn(sdohRows);

  interventionPlaybook = {};
  interventionRows.slice(1).forEach(function(row) {
    var key = String(row[0] || "").trim().toLowerCase();
    if (!key) return;
    var title = String(row[1] || "").trim();
    var summary = String(row[2] || "").trim();
    var actions = parseActions(row[3]);
    interventionPlaybook[key] = {
      title: title,
      summary: summary,
      actions: actions
    };
  });

  CURATED_INTERVENTION_KEYS = parseSingleColumn(curatedRows).map(function(key) {
    return key.toLowerCase();
  });

  var defaultRow = defaultRows[1] || [];
  DEFAULT_INTERVENTION_PLAN = {
    title: String(defaultRow[0] || "").trim(),
    summary: String(defaultRow[1] || "").trim(),
    actions: parseActions(defaultRow[2])
  };

  CARE_NAVIGATORS = navigatorRows.slice(1).map(function(row) {
    return {
      id: String(row[0] || "").trim(),
      name: String(row[1] || "").trim(),
      specialty: String(row[2] || "").trim()
    };
  }).filter(function(nav) {
    return nav.id && nav.name;
  });

  requireNonEmpty("contract_fallbacks", CONTRACT_FALLBACKS);
  requireNonEmpty("risk_bands", DEFAULT_RISK_BANDS);
  requireNonEmpty("sdoh_level_order", SDOH_LEVEL_ORDER);
  requireNonEmpty("intervention_plans", Object.keys(interventionPlaybook));
  requireNonEmpty("curated_intervention_keys", CURATED_INTERVENTION_KEYS);
  requireNonEmpty("default_intervention_plan", DEFAULT_INTERVENTION_PLAN.title ? [DEFAULT_INTERVENTION_PLAN.title] : []);
  requireNonEmpty("care_navigators", CARE_NAVIGATORS);
}

function ensureContracts(rows) {
  if (!Array.isArray(rows)) return [];
  if (!Array.isArray(CONTRACT_FALLBACKS) || !CONTRACT_FALLBACKS.length) return rows;
  var idx = 0;
  return rows.map(function(row) {
    var record = row || {};
    var hasContract =
      record.contract !== undefined &&
      record.contract !== null &&
      String(record.contract).trim() !== "";
    if (!hasContract) {
      record.contract = CONTRACT_FALLBACKS[idx % CONTRACT_FALLBACKS.length];
      idx += 1;
    }
    return record;
  });
}

function normalizeZip(val) {
  var str = String(val || "").trim();
  if (!str) return "";
  var digitsOnly = str.replace(/\D/g, "");
  if (digitsOnly && digitsOnly.length > 0 && digitsOnly.length <= 5) {
    return digitsOnly.padStart(5, "0");
  }
  return str;
}

// Normalize CSV rows into dashboard-ready objects
function deriveAgeGroup(age) {
  if (age === null || age === undefined || isNaN(age)) return "";
  var a = Number(age);
  if (a < 18) return "Under 18";
  if (a < 35) return "18-34";
  if (a < 45) return "35-44";
  if (a < 65) return "45-64";
  if (a < 80) return "65-79";
  return "80+";
}

function normalizeRecord(row, idx) {
  function num(x) {
    if (x === null || x === undefined || x === "") return null;
    const v = Number(x);
    return isNaN(v) ? null : v;
  }
  const lift = num(row.sdoh_lift);
  const riskFullVal = num(row.risk_full);
  let seg = String(row.sdoh_lift_level || "").trim();
  if (!seg) {
    if (lift === null) {
      seg = "Unknown";
    } else if (lift > 0.5) {
      seg = "Extremely high SDOH burden";
    } else if (lift > 0.2) {
      seg = "Significant SDOH influence on risk";
    } else if (lift >= 0) {
      seg = "Mild SDOH contribution";
    } else {
      seg = "SDOH Protective / No Impact";
    }
  }

  var record = {
    _idx: idx,
    member: String(row.member || ""),
    member_name: String(row.member_name || ""),
    age: num(row.age),
    sex: String(row.sex || ""),
    age_group: deriveAgeGroup(num(row.age)),
    age_class: String(row.age_class || ""),
    gender: String(row.gender || ""),
    race: String(row.race || ""),
    hp: String(row.hp || ""),
    hp_name: String(row.hp_name || ""),
    pcp_x: String(row.pcp_x || ""),
    grp_name: String(row.grp_name || ""),
    plan: String(row.plan || ""),
    contract: String(row.contract || ""),
    segment: String(row.segment || ""),
    agent: String(row.agent || ""),
    address: String(row.address || ""),
    county: String(row.county || ""),
    state: String(row.state || ""),
    county_clean: String(row.county_clean || ""),
    county_fips: String(row.county_fips || ""),
    zip: normalizeZip(row.zip),
    compliance: num(row.compliance),
    compliance_2023: num(row.compliance_2023),
    compliance_hba1c: num(row.compliance_hba1c),
    compliancebcs: num(row.compliancebcs),
    pcp_visits: num(row.pcp_visits),
    no_ip_visits_2023: num(row.no_ip_visits_2023),
    a1c_value: num(row.a1c_value),
    ldl_value: num(row.ldl_value),
    bmi: num(row.bmi),
    bp_systolic: num(row.bp_systolic),
    bp_diastolic: num(row.bp_diastolic),
    income_weighted_index: num(row.income_weighted_index),
    income_inequality: num(row.income_inequality),
    per_capita_income: num(row.per_capita_income),
    education_score: num(row.education_score),
    labor_market_hardship: num(row.labor_market_hardship),
    housing_instability: num(row.housing_instability),
    car_access_risk: num(row.car_access_risk),
    mean_commute: num(row.mean_commute),
    commute_hardship_index: num(row.commute_hardship_index),
    transit_dependency: num(row.transit_dependency),
    food_insecurity_index: num(row.food_insecurity_index),
    health_access_score: num(row.health_access_score),
    digital_disadvantage: num(row.digital_disadvantage),
    social_isolation_index: num(row.social_isolation_index),
    environmental_burden: num(row.environmental_burden),
    rurality_index: num(row.rurality_index),
    risk_score_x: num(row.risk_score_x),
    risk_full: riskFullVal,
    risk_no_sdoh: num(row.risk_no_sdoh),
    sdoh_lift: lift,
    sdoh_lift_level: seg,
    sdoh_driver_1: String(row.sdoh_driver_1 || ""),
    sdoh_driver_1_value: num(row.sdoh_driver_1_value),
    sdoh_driver_2: String(row.sdoh_driver_2 || ""),
    sdoh_driver_2_value: num(row.sdoh_driver_2_value),
    sdoh_driver_3: String(row.sdoh_driver_3 || ""),
    sdoh_driver_3_value: num(row.sdoh_driver_3_value),
    sdoh_driver_4: String(row.sdoh_driver_4 || ""),
    sdoh_driver_4_value: num(row.sdoh_driver_4_value),
    sdoh_driver_5: String(row.sdoh_driver_5 || ""),
    sdoh_driver_5_value: num(row.sdoh_driver_5_value),
    nonsdoh_driver_1: String(row.nonsdoh_driver_1 || ""),
    nonsdoh_driver_1_value: num(row.nonsdoh_driver_1_value),
    nonsdoh_driver_2: String(row.nonsdoh_driver_2 || ""),
    nonsdoh_driver_2_value: num(row.nonsdoh_driver_2_value),
    nonsdoh_driver_3: String(row.nonsdoh_driver_3 || ""),
    nonsdoh_driver_3_value: num(row.nonsdoh_driver_3_value),
    nonsdoh_driver_4: String(row.nonsdoh_driver_4 || ""),
    nonsdoh_driver_4_value: num(row.nonsdoh_driver_4_value),
    nonsdoh_driver_5: String(row.nonsdoh_driver_5 || ""),
    nonsdoh_driver_5_value: num(row.nonsdoh_driver_5_value)
  };
  record.risk_band = riskBandFromScore(record.risk_score_x);
  return record;
}

// ==========================
//  GLOBAL STATE
// ==========================
let DEFAULT_RISK_BANDS = [];
let SDOH_LEVEL_ORDER = [];

const state = {
  page: "zip-page",
  filters: {
    sdoh_level: "",
    county: "",
    zip: "",
    plan: "",
    contract: "",
    race: "",
    risk_level: "",
    age_group: "",
    search: ""
  },
  selectedMemberIdx: null,
  isMemberModalOpen: false,
  isInterventionModalOpen: false,
  focusHighActive: false,
  memberOverrides: {},
  careNavigatorAssignments: {},
  outreachSchedules: {},
  lastMemberFiltered: [],
  lastDistributionBase: [],
  lastZipRows: [],
  sdohDistributionFilter: "",
  selectedZip: "",
  mapFuturistic: false,
  mapIntensity: 0.75,
  selectedContract: "",
  selectedCampaignId: "",
  campaignEnrollments: {},
  campaigns: []
};

var sdohRadarChart = null;
var nonSdohRadarChart = null;
var modalSdohRadarChart = null;
var modalNonSdohRadarChart = null;
var zipGridCharts = [];
var zipModalSdohChart = null;
var zipModalNonSdohChart = null;
var zipLeafletMap = null;
var zipGeoLayer = null;
var zipGeoData = null;
var zipMapBaseLayer = null;
var zipMapDarkLayer = null;
var zipMapPulseLayer = null;
var zipClusterLayer = null;
var zipPanelSdohChart = null;
var zipPanelNonSdohChart = null;
var zipRiskBaselineMap = null;
var zipRiskBaselineAvg = null;

var CAMPAIGN_RULE_CATEGORIES = [
  {
    id: "clinical",
    label: "Clinical",
    fields: [
      { key: "compliance_hba1c", label: "HbA1c adherence" },
      { key: "a1c_value", label: "A1c value" },
      { key: "ldl_value", label: "LDL value" },
      { key: "bmi", label: "BMI" },
      { key: "bp_systolic", label: "BP systolic" },
      { key: "bp_diastolic", label: "BP diastolic" }
    ]
  },
  {
    id: "risk",
    label: "Risk",
    fields: [
      { key: "risk_full", label: "Risk with SDOH" },
      { key: "risk_no_sdoh", label: "Risk no SDOH" },
      { key: "sdoh_lift", label: "SDOH lift" }
    ]
  },
  {
    id: "sdoh",
    label: "SDOH",
    fields: [
      { key: "digital_disadvantage", label: "Digital disadvantage" },
      { key: "commute_hardship_index", label: "Commute hardship index" },
      { key: "health_access_score", label: "Health access score" },
      { key: "income_weighted_index", label: "Income weighted index" },
      { key: "education_score", label: "Education score" },
      { key: "labor_market_hardship", label: "Labor market hardship" },
      { key: "housing_instability", label: "Housing instability" },
      { key: "food_insecurity_index", label: "Food insecurity index" },
      { key: "social_isolation_index", label: "Social isolation index" },
      { key: "environmental_burden", label: "Environmental burden" },
      { key: "rurality_index", label: "Rurality index" }
    ]
  },
  {
    id: "utilization",
    label: "Utilization",
    fields: [
      { key: "pcp_visits", label: "PCP visits" },
      { key: "no_ip_visits_2023", label: "No IP visits (2023)" }
    ]
  },
  {
    id: "engagement",
    label: "Engagement",
    fields: [
      { key: "OutreachAttemptCount", label: "Outreach attempts" },
      { key: "PDC_Before", label: "PDC before" },
      { key: "PDC_After", label: "PDC after" }
    ]
  }
];

var CAMPAIGN_OPERATORS = [
  { value: ">=", label: "≥" },
  { value: "<=", label: "≤" },
  { value: ">", label: ">" },
  { value: "<", label: "<" },
  { value: "=", label: "=" }
];

var CAMPAIGN_FIELD_STATS = null;

let interventionPlaybook = {};
let CURATED_INTERVENTION_KEYS = [];
let CARE_NAVIGATORS = [];

function getNavigatorById(id) {
  if (!id) return null;
  return CARE_NAVIGATORS.find(function(nav) {
    return nav.id === id;
  }) || null;
}

let DEFAULT_INTERVENTION_PLAN = null;

const CAMPAIGN_STORAGE_KEY = "sdoh_campaigns_v1";
const DEFAULT_CAMPAIGNS = [
  {
    id: "diabetes-med-adherence",
    name: "Medication Adherence Counselling (Diabetes)",
    description: "Auto-enroll members with low HbA1c adherence, elevated risk, and positive SDOH lift.",
    autoEnroll: true,
    outreachMethods: ["Phone", "SMS", "Email"]
  },
  {
    id: "cdc-diabetes-awareness",
    name: "CDC Diabetes Awareness Communications",
    description: "Regional (US) CDC campaign focused on diabetes management, healthy behaviors, and adherence outreach.",
    autoEnroll: false,
    outreachMethods: ["Phone", "SMS", "Email"]
  },
  {
    id: "world-diabetes-day",
    name: "World Diabetes Day",
    description: "Global & International Campaign: Annual awareness on Nov 14 led by IDF/WHO for prevention and care.",
    autoEnroll: false,
    outreachMethods: ["Phone", "SMS", "Email"]
  },
  {
    id: "world-adherence-day",
    name: "World Adherence Day",
    description: "Global & International Campaign (Mar 27) highlighting adherence to medications, lifestyle, and care plans.",
    autoEnroll: false,
    outreachMethods: ["Phone", "SMS", "Email"]
  },
  {
    id: "bloodsugar-selfie",
    name: "BloodSugarSelfie",
    description: "Global social campaign encouraging blood glucose monitoring engagement and awareness sharing.",
    autoEnroll: false,
    outreachMethods: ["Phone", "SMS", "Email"]
  }
];

function diabetesCampaignRule(member) {
  return (
    member &&
    member.compliance_hba1c !== null &&
    member.compliance_hba1c < 0.8 &&
    member.risk_full !== null &&
    member.risk_full >= 2.0 &&
    member.sdoh_lift !== null &&
    member.sdoh_lift > 0
  );
}

function hydrateCampaigns(list) {
  function normalizeOutreachMethods(methods) {
    if (!Array.isArray(methods)) return ["Phone", "SMS", "Email", "Mail"];
    var normalized = [];
    methods.forEach(function(m) {
      var key = String(m || "").toLowerCase();
      if (!key) return;
      if (key.indexOf("phone/sms") !== -1 || (key.indexOf("phone") !== -1 && key.indexOf("sms") !== -1)) {
        normalized.push("Phone");
        normalized.push("SMS");
        return;
      }
      if (key.indexOf("sms") !== -1) {
        normalized.push("SMS");
        return;
      }
      if (key.indexOf("phone") !== -1) {
        normalized.push("Phone");
        return;
      }
      if (key.indexOf("email") !== -1) {
        normalized.push("Email");
        return;
      }
      if (key.indexOf("mail") !== -1) {
        normalized.push("Mail");
        return;
      }
    });
    var seen = {};
    var out = normalized.filter(function(m) {
      if (seen[m]) return false;
      seen[m] = true;
      return true;
    });
    return out.length ? out : ["Phone", "SMS", "Email", "Mail"];
  }

  return (list || []).map(function(c) {
    var campaign = Object.assign({}, c);
    if (campaign.id === "diabetes-med-adherence") {
      campaign.autoEnroll = true;
      campaign.rule = diabetesCampaignRule;
    } else {
      campaign.autoEnroll = Boolean(campaign.autoEnroll);
    }
    campaign.rules = Array.isArray(campaign.rules) ? campaign.rules : [];
    campaign.outreachMethods = normalizeOutreachMethods(campaign.outreachMethods);
    return campaign;
  });
}

function truthyValue(val) {
  if (val === null || val === undefined) return false;
  if (typeof val === "boolean") return val;
  if (typeof val === "number") return !isNaN(val) && val !== 0;
  var key = String(val).trim().toLowerCase();
  if (!key || key === "0" || key === "false" || key === "no" || key === "n" || key === "null" || key === "nan") {
    return false;
  }
  return true;
}

function toNumber(val) {
  var num = parseFloat(val);
  return isNaN(num) ? null : num;
}

function normalizeChannelValue(val) {
  var key = String(val || "").trim().toLowerCase();
  if (!key) return "";
  if (key.indexOf("sms") !== -1) return "SMS";
  if (key.indexOf("phone") !== -1) return "Phone";
  if (key.indexOf("email") !== -1) return "Email";
  if (key.indexOf("mail") !== -1) return "Mail";
  return "";
}

function deriveMemberChannel(member, overrideValue) {
  var overrideChannel = normalizeChannelValue(overrideValue);
  if (overrideChannel) return overrideChannel;

  var channelFromData = normalizeChannelValue(member && member.Channel);
  if (channelFromData) return channelFromData;

  var phoneAvailable = truthyValue(member && member.phone);
  var mailAvailable = truthyValue(member && member.mail);
  var responseFlag = truthyValue(member && member.Response_Flag);
  var readFlag = truthyValue(member && member.Read_Flag);
  var deliveredFlag = truthyValue(member && member.Delivered_Flag);
  var outreachAttempts = toNumber(member && member.OutreachAttemptCount);
  var digital = toNumber(member && member.digital_disadvantage);
  var rural = toNumber(member && member.rurality_index);
  var age = toNumber(member && member.age);
  var risk = toNumber(member && member.risk_full);

  var highDigital = digital !== null && digital >= 0.6;
  var highRural = rural !== null && rural >= 0.6;
  var older = age !== null && age >= 65;

  if (phoneAvailable && (responseFlag || (outreachAttempts !== null && outreachAttempts >= 2) || (risk !== null && risk >= 2.3 && !highDigital))) {
    return "Phone";
  }

  if (mailAvailable && (highDigital || highRural || older)) {
    return "Mail";
  }

  if (readFlag || deliveredFlag) {
    return "Email";
  }

  if (phoneAvailable) return "SMS";
  if (mailAvailable) return "Mail";
  return "Email";
}

function loadCampaignState() {
  var stored = null;
  if (typeof window !== "undefined" && window.localStorage) {
    try {
      stored = JSON.parse(window.localStorage.getItem(CAMPAIGN_STORAGE_KEY));
    } catch (err) {
      stored = null;
    }
  }
  var storedCampaigns = stored && Array.isArray(stored.campaigns) ? stored.campaigns : [];
  var merged = DEFAULT_CAMPAIGNS.slice();
  storedCampaigns.forEach(function(c) {
    if (!merged.some(function(existing) { return existing.id === c.id; })) {
      merged.push(c);
    }
  });
  state.campaigns = hydrateCampaigns(merged);
  state.campaignEnrollments = (stored && stored.enrollments) ? stored.enrollments : {};
  state.selectedCampaignId =
    (stored && stored.selectedCampaignId) ||
    (state.campaigns[0] ? state.campaigns[0].id : "");
}

function saveCampaignState() {
  if (typeof window === "undefined" || !window.localStorage) return;
  var payload = {
    campaigns: state.campaigns.map(function(c) {
      return {
        id: c.id,
        name: c.name,
        description: c.description,
        autoEnroll: Boolean(c.autoEnroll),
        outreachMethods: c.outreachMethods,
        rules: Array.isArray(c.rules) ? c.rules : []
      };
    }),
    enrollments: state.campaignEnrollments,
    selectedCampaignId: state.selectedCampaignId
  };
  window.localStorage.setItem(CAMPAIGN_STORAGE_KEY, JSON.stringify(payload));
}

function getCampaignById(id) {
  return (state.campaigns || []).find(function(c) { return c.id === id; }) || null;
}

function getCampaignEnrollment(campaignId, memberId) {
  if (!campaignId || !memberId) return null;
  var bucket = state.campaignEnrollments[campaignId];
  if (!bucket) return null;
  return bucket[memberId] || null;
}

function setCampaignEnrollment(campaignId, memberId, patch) {
  if (!campaignId || !memberId) return;
  if (!state.campaignEnrollments[campaignId]) {
    state.campaignEnrollments[campaignId] = {};
  }
  var current = state.campaignEnrollments[campaignId][memberId] || {};
  var next = Object.assign({}, current, patch);
  if (!next.override && !next.outreachMethod && !next.status && !next.note) {
    delete state.campaignEnrollments[campaignId][memberId];
  } else {
    state.campaignEnrollments[campaignId][memberId] = next;
  }
  saveCampaignState();
}

function isCampaignEligible(campaign, member) {
  if (!campaign || !campaign.autoEnroll) return false;
  if (Array.isArray(campaign.rules) && campaign.rules.length) {
    return evaluateCampaignRules(member, campaign.rules);
  }
  if (typeof campaign.rule === "function") return Boolean(campaign.rule(member));
  return false;
}

function isCampaignEnrolled(campaign, member) {
  if (!campaign || !member) return false;
  var record = getCampaignEnrollment(campaign.id, member.member);
  if (record && record.override === "include") return true;
  if (record && record.override === "exclude") return false;
  return isCampaignEligible(campaign, member);
}

function enrollmentSource(campaign, member) {
  var record = getCampaignEnrollment(campaign.id, member.member);
  if (record && record.override === "include") return "Manual";
  if (record && record.override === "exclude") return "Excluded";
  if (isCampaignEligible(campaign, member)) return "Auto";
  return "Not enrolled";
}

function getInterventionPlanByKey(key) {
  var normalized = (key || "").toLowerCase();
  return interventionPlaybook[normalized] || DEFAULT_INTERVENTION_PLAN;
}

function resolveActivePlan(member) {
  if (!member) {
    return {
      plan: null,
      overrideActive: false,
      defaultKey: "",
      overrideKey: ""
    };
  }
  var defaultKey = String(member.sdoh_driver_1 || "").toLowerCase();
  var overrideKey = state.memberOverrides[member.member];
  var activeKey = overrideKey || defaultKey;
  return {
    plan: getInterventionPlanByKey(activeKey),
    overrideActive: Boolean(overrideKey),
    defaultKey: defaultKey,
    overrideKey: overrideKey
  };
}

function describeIntervention(member) {
  var overrideKey = state.memberOverrides[member.member];
  var key = overrideKey || String(member.sdoh_driver_1 || "").toLowerCase();
  return getInterventionPlanByKey(key).title;
}

function buildInterventionChoices() {
  var seen = new Set();
  DATA.forEach(function(d) {
    for (var i = 1; i <= 5; i++) {
      var drv = d["sdoh_driver_" + i];
      if (drv) {
        seen.add(String(drv).toLowerCase());
      }
    }
  });
  CURATED_INTERVENTION_KEYS.forEach(function(key) {
    seen.add(key);
  });
  if (seen.size === 0) {
    Object.keys(interventionPlaybook).forEach(function(key) {
      seen.add(key);
    });
  }
  var uniqueByLabel = new Map();
  Array.from(seen).forEach(function(key) {
    var plan = getInterventionPlanByKey(key);
    var label = plan.title;
    if (!uniqueByLabel.has(label)) {
      uniqueByLabel.set(label, {
        key: key,
        label: label
      });
    }
  });
  INTERVENTION_CHOICES = Array.from(uniqueByLabel.values()).sort(function(a, b) {
    return a.label.localeCompare(b.label);
  });
}

function setFooter(msg) {
  var el = document.getElementById("footer-status");
  if (el) el.textContent = msg;
}

// ==========================
//  UTILITIES
// ==========================
function uniqueSorted(arr) {
  return Array.from(new Set(arr.filter(function(x){ return x && String(x).trim() !== ""; }))).sort();
}

function shuffleArray(list) {
  for (var i = list.length - 1; i > 0; i--) {
    var j = Math.floor(Math.random() * (i + 1));
    var temp = list[i];
    list[i] = list[j];
    list[j] = temp;
  }
  return list;
}

function parseNumericValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return isNaN(value) ? null : value;
  var cleaned = String(value).replace(/[%$,]/g, "").trim();
  if (!cleaned) return null;
  var num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

function getCampaignFieldStats() {
  if (CAMPAIGN_FIELD_STATS) return CAMPAIGN_FIELD_STATS;
  var stats = {};
  CAMPAIGN_RULE_CATEGORIES.forEach(function(cat) {
    cat.fields.forEach(function(field) {
      stats[field.key] = { min: null, max: null };
    });
  });
  (DATA || []).forEach(function(row) {
    Object.keys(stats).forEach(function(key) {
      var val = parseNumericValue(row[key]);
      if (val === null) return;
      if (stats[key].min === null || val < stats[key].min) stats[key].min = val;
      if (stats[key].max === null || val > stats[key].max) stats[key].max = val;
    });
  });
  CAMPAIGN_FIELD_STATS = stats;
  return stats;
}

function evaluateCampaignRules(member, rules) {
  if (!member || !Array.isArray(rules) || !rules.length) return false;
  for (var i = 0; i < rules.length; i++) {
    var rule = rules[i];
    if (!rule || !rule.field || rule.value === "" || rule.value === null || rule.value === undefined) {
      continue;
    }
    var left = parseNumericValue(member[rule.field]);
    var right = parseNumericValue(rule.value);
    if (left === null || right === null) return false;
    switch (rule.op) {
      case ">=":
        if (!(left >= right)) return false;
        break;
      case "<=":
        if (!(left <= right)) return false;
        break;
      case ">":
        if (!(left > right)) return false;
        break;
      case "<":
        if (!(left < right)) return false;
        break;
      case "=":
        if (!(left === right)) return false;
        break;
      default:
        return false;
    }
  }
  return true;
}

function slugify(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function sortByPriority(values, priorityList) {
  if (!Array.isArray(values)) return [];
  var rank = {};
  (priorityList || []).forEach(function(label, idx) {
    rank[String(label).toLowerCase()] = idx;
  });
  return values.slice().sort(function(a, b) {
    var aRank = rank[String(a).toLowerCase()];
    var bRank = rank[String(b).toLowerCase()];
    if (aRank === undefined) aRank = (priorityList ? priorityList.length : 999);
    if (bRank === undefined) bRank = (priorityList ? priorityList.length : 999);
    if (aRank !== bRank) return aRank - bRank;
    return String(a).localeCompare(String(b));
  });
}

function getMemberByIdx(idx) {
  if (idx === null || idx === undefined) return null;
  return DATA[idx] || null;
}

function getSelectedMember() {
  return getMemberByIdx(state.selectedMemberIdx);
}

function csvEscape(value) {
  if (value === null || value === undefined) return "";
  var str = String(value);
  if (/["\n,]/.test(str)) {
    str = "\"" + str.replace(/"/g, "\"\"") + "\"";
  }
  return str;
}

function downloadCsv(filename, rows, columns) {
  if (!rows || !rows.length) {
    alert("No data available to export.");
    return;
  }
  var header = columns.map(function(col) {
    return csvEscape(col.label || col.key || "");
  }).join(",");
  var lines = [header];
  rows.forEach(function(row) {
    var line = columns.map(function(col) {
      var val;
      if (typeof col.value === "function") {
        val = col.value(row);
      } else if (col.key) {
        val = row[col.key];
      } else {
        val = "";
      }
      return csvEscape(val);
    }).join(",");
    lines.push(line);
  });
  var blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  var link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  setTimeout(function() {
    URL.revokeObjectURL(link.href);
  }, 0);
}

function tableToCsvText(table) {
  if (!table) return "";
  var rows = [];
  var headerCells = table.querySelectorAll("thead th");
  if (headerCells.length) {
    var header = Array.from(headerCells).map(function(cell) {
      return escapeCsv(cell.textContent.trim());
    }).join(",");
    rows.push(header);
  }
  var bodyRows = table.querySelectorAll("tbody tr");
  bodyRows.forEach(function(tr) {
    var cells = tr.querySelectorAll("td");
    var row = Array.from(cells).map(function(cell) {
      return escapeCsv(cell.textContent.trim());
    }).join(",");
    rows.push(row);
  });
  return rows.join("\n");
}

function escapeCsv(value) {
  var text = String(value || "");
  if (text.indexOf("\"") !== -1) {
    text = text.replace(/\"/g, "\"\"");
  }
  if (/[\",\\n]/.test(text)) {
    text = "\"" + text + "\"";
  }
  return text;
}

function copyTableToClipboard(tableId, button) {
  var table = document.getElementById(tableId);
  if (!table) return;
  var csv = tableToCsvText(table);
  if (!csv) return;
  var done = function() {
    if (!button) return;
    var original = button.textContent;
    button.textContent = "Copied";
    setTimeout(function() { button.textContent = original; }, 1200);
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(csv).then(done).catch(function() {});
  } else {
    var ta = document.createElement("textarea");
    ta.value = csv;
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand("copy");
      done();
    } catch (err) {}
    document.body.removeChild(ta);
  }
}

function fmtNumber(x, decimals) {
  if (x === null || x === undefined || isNaN(x)) return "-";
  var d = (decimals === undefined ? 2 : decimals);
  return Number(x).toFixed(d);
}

function fmtPercent(x, decimals) {
  if (x === null || x === undefined || isNaN(x)) return "-";
  var d = (decimals === undefined ? 1 : decimals);
  return (x * 100).toFixed(d) + "%";
}

function fmtSignedNumber(x, decimals) {
  if (x === null || x === undefined || isNaN(x)) return "-";
  var val = fmtNumber(x, decimals);
  return Number(x) > 0 ? "+" + val : val;
}

function extractDrivers(member, prefix) {
  var items = [];
  if (!member) return items;
  for (var i = 1; i <= 5; i++) {
    var name = member[prefix + i];
    var value = member[prefix + i + "_value"];
    if (!name) continue;
    items.push({ name: name, value: value });
  }
  return items;
}

function buildDriverList(member, prefixName, prefixValue) {
  var labels = [];
  var values = [];
  if (!member) return { labels: labels, values: values };
  for (var i = 1; i <= 5; i++) {
    var rawName = member[prefixName + i];
    if (rawName === null || rawName === undefined) continue;
    var label = String(rawName).trim();
    if (!label) continue;
    var rawValue = member[prefixValue + i + "_value"];
    var value = (rawValue === null || rawValue === undefined || isNaN(rawValue)) ? 0 : Number(rawValue);
    labels.push(label);
    values.push(value);
  }
  return { labels: labels, values: values };
}

function normalizeAbs(values) {
  if (!values || !values.length) return [];
  var maxAbs = 0;
  values.forEach(function(v) {
    var absVal = Math.abs(v || 0);
    if (absVal > maxAbs) maxAbs = absVal;
  });
  if (!maxAbs) maxAbs = 1;
  return values.map(function(v) {
    return Math.abs(v || 0) / maxAbs;
  });
}

function collectTopDrivers(members, type) {
  var prefix = type === "nonsdoh" ? "nonsdoh_driver_" : "sdoh_driver_";
  var totals = {};
  if (!members || !members.length) return [];
  members.forEach(function(member) {
    for (var i = 1; i <= 5; i++) {
      var rawName = member[prefix + i];
      if (rawName === null || rawName === undefined) continue;
      var name = String(rawName).trim();
      if (!name) continue;
      var rawValue = member[prefix + i + "_value"];
      var value = (rawValue === null || rawValue === undefined || isNaN(rawValue)) ? 0 : Number(rawValue);
      if (!totals[name]) {
        totals[name] = { name: name, sum: 0, sumAbs: 0, posAbs: 0, negAbs: 0 };
      }
      var entry = totals[name];
      var absVal = Math.abs(value);
      entry.sum += value;
      entry.sumAbs += absVal;
      if (value > 0) entry.posAbs += absVal;
      if (value < 0) entry.negAbs += absVal;
    }
  });
  var list = Object.keys(totals).map(function(key) { return totals[key]; });
  list.sort(function(a, b) { return b.sumAbs - a.sumAbs; });
  return list.slice(0, 5);
}

function destroyRadarCharts() {
  if (sdohRadarChart) {
    sdohRadarChart.destroy();
    sdohRadarChart = null;
  }
  if (nonSdohRadarChart) {
    nonSdohRadarChart.destroy();
    nonSdohRadarChart = null;
  }
}

function destroyModalRadarCharts() {
  if (modalSdohRadarChart) {
    modalSdohRadarChart.destroy();
    modalSdohRadarChart = null;
  }
  if (modalNonSdohRadarChart) {
    modalNonSdohRadarChart.destroy();
    modalNonSdohRadarChart = null;
  }
}

function createRadarChart(canvas, labels, values, color, fillColor) {
  if (!canvas || !labels.length) return null;
  var ctx = canvas.getContext("2d");
  return new Chart(ctx, {
    type: "radar",
    data: {
      labels: labels,
      datasets: [{
        data: normalizeAbs(values),
        borderColor: color,
        backgroundColor: fillColor,
        pointBackgroundColor: color,
        pointBorderColor: color,
        borderWidth: 1.5,
        pointRadius: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        r: {
          suggestedMin: 0,
          suggestedMax: 1,
          ticks: { display: false },
          grid: { color: "rgba(148, 163, 184, 0.25)" },
          angleLines: { color: "rgba(148, 163, 184, 0.35)" },
          pointLabels: {
            color: "#4B5563",
            font: { size: 10 }
          }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(context) {
              return "Magnitude: " + context.formattedValue;
            }
          }
        }
      }
    }
  });
}

function renderRadarCharts(member) {
  destroyRadarCharts();
  if (!member || typeof Chart === "undefined") return;
  var sdohList = buildDriverList(member, "sdoh_driver_", "sdoh_driver_");
  var nonList = buildDriverList(member, "nonsdoh_driver_", "nonsdoh_driver_");
  var sdohCanvas = document.getElementById("sdohRadar");
  var nonCanvas = document.getElementById("nonSdohRadar");
  sdohRadarChart = createRadarChart(
    sdohCanvas,
    sdohList.labels,
    sdohList.values,
    "#FF6B4A",
    "rgba(255, 107, 74, 0.18)"
  );
  nonSdohRadarChart = createRadarChart(
    nonCanvas,
    nonList.labels,
    nonList.values,
    "#2563EB",
    "rgba(37, 99, 235, 0.18)"
  );
}

function renderModalRadarCharts(member) {
  destroyModalRadarCharts();
  if (!member || typeof Chart === "undefined") return;
  var sdohList = buildDriverList(member, "sdoh_driver_", "sdoh_driver_");
  var nonList = buildDriverList(member, "nonsdoh_driver_", "nonsdoh_driver_");
  var sdohCanvas = document.getElementById("modalSdohRadar");
  var nonCanvas = document.getElementById("modalNonSdohRadar");
  modalSdohRadarChart = createRadarChart(
    sdohCanvas,
    sdohList.labels,
    sdohList.values,
    "#FF6B4A",
    "rgba(255, 107, 74, 0.18)"
  );
  modalNonSdohRadarChart = createRadarChart(
    nonCanvas,
    nonList.labels,
    nonList.values,
    "#2563EB",
    "rgba(37, 99, 235, 0.18)"
  );
}

function destroyZipGridCharts() {
  if (!zipGridCharts.length) return;
  zipGridCharts.forEach(function(chart) {
    if (chart) chart.destroy();
  });
  zipGridCharts = [];
}

function createZipGridRadar(canvas, labels, values, color) {
  if (!canvas || !labels.length) return null;
  var ctx = canvas.getContext("2d");
  return new Chart(ctx, {
    type: "radar",
    data: {
      labels: labels,
      datasets: [{
        data: normalizeAbs(values),
        borderColor: color,
        backgroundColor: "rgba(255, 255, 255, 0.0)",
        pointBackgroundColor: color,
        pointBorderColor: color,
        borderWidth: 1.2,
        pointRadius: 1.5
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        r: {
          suggestedMin: 0,
          suggestedMax: 1,
          ticks: { display: false },
          grid: { color: "rgba(148, 163, 184, 0.25)" },
          angleLines: { color: "rgba(148, 163, 184, 0.3)" },
          pointLabels: { display: false }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false }
      }
    }
  });
}

function prettifyDriverName(name) {
  if (!name) return "";
  return name.replace(/_/g, " ").replace(/\b\w/g, function(char) {
    return char.toUpperCase();
  });
}

function sdohBadgeClass(seg) {
  seg = (seg || "").toLowerCase();
  if (seg.indexOf("extreme") !== -1) return "extreme";
  if (seg.indexOf("significant") !== -1) return "significant";
  if (seg.indexOf("mild") !== -1) return "mild";
  if (seg.indexOf("protective") !== -1 || seg.indexOf("no impact") !== -1) return "protective";
  return "";
}

function sdohColorForLift(lift) {
  if (lift === null || lift === undefined || isNaN(lift)) {
    return "#9CA3C7";
  }
  var minVal = -0.1;
  var maxVal = 0.6;
  var t = (lift - minVal) / (maxVal - minVal);
  if (t < 0) t = 0;
  if (t > 1) t = 1;
  // Blend four pastel anchors for leadership-friendly palette
  var anchors = [
    [126, 211, 178], // protective teal
    [165, 200, 255], // neutral soft blue
    [255, 200, 146], // mild apricot
    [255, 138, 174]  // extreme coral
  ];
  var segment = t * (anchors.length - 1);
  var idx = Math.floor(segment);
  var frac = segment - idx;
  var start = anchors[idx];
  var end = anchors[Math.min(idx + 1, anchors.length - 1)];
  var r = Math.round(start[0] + (end[0] - start[0]) * frac);
  var g = Math.round(start[1] + (end[1] - start[1]) * frac);
  var b = Math.round(start[2] + (end[2] - start[2]) * frac);
  return "rgb(" + r + "," + g + "," + b + ")";
}

function isHighBurden(seg) {
  seg = (seg || "").toLowerCase();
  return (
    seg.indexOf("extremely high") !== -1 ||
    seg.indexOf("extreme") !== -1 ||
    seg.indexOf("significant") !== -1
  );
}

function riskBandFromValue(val) {
  if (val === null || val === undefined || isNaN(val)) return "Unknown risk";
  if (val > 2.3) return "High risk";
  if (val >= 1.8) return "Moderate risk";
  return "Lower risk";
}

function riskBandFromScore(val) {
  if (val === null || val === undefined || isNaN(val)) return "Unknown risk";
  if (val > 2.3) return "High risk";
  if (val >= 1.8) return "Moderate risk";
  return "Lower risk";
}

function riskBandRank(band) {
  switch ((band || "").toLowerCase()) {
    case "high risk":
      return 3;
    case "moderate risk":
      return 2;
    case "lower risk":
      return 1;
    default:
      return 0;
  }
}

function riskBandColor(band) {
  var key = (band || "").toLowerCase();
  if (key === "high risk") return "#FF6B6B";
  if (key === "moderate risk") return "#FFB347";
  if (key === "lower risk") return "#39C59D";
  return "#94A3B8";
}

function riskBandClass(band) {
  var key = (band || "").toLowerCase();
  if (key === "high risk") return "risk-high";
  if (key === "moderate risk") return "risk-med";
  if (key === "lower risk") return "risk-low";
  return "risk-unknown";
}

// ==========================
//  FILTERING
// ==========================
function matchesMulti(filterValue, candidate) {
  if (filterValue === undefined || filterValue === null) return true;

  if (Array.isArray(filterValue)) {
    if (filterValue.length === 0) return true;
    return filterValue.includes(candidate);
  }

  var str = String(filterValue);
  if (!str) return true;
  var cand = candidate === undefined || candidate === null ? "" : String(candidate);
  return cand === str;
}

function applyFilters() {
  const f = state.filters;
  const search = (f.search || "").toLowerCase();

  return DATA.filter(function(d) {
    if (!matchesMulti(f.sdoh_level, d.sdoh_lift_level)) return false;
    if (!matchesMulti(f.county, d.county)) return false;
    if (!matchesMulti(f.zip, d.zip)) return false;
    if (!matchesMulti(f.plan, d.plan)) return false;
    if (!matchesMulti(f.contract, d.contract)) return false;
    if (!matchesMulti(f.race, d.race)) return false;
    if (!matchesMulti(f.risk_level, d.risk_band)) return false;
    if (!matchesMulti(f.age_group, d.age_group)) return false;

    if (search) {
      var text = (d.member + d.member_name + d.zip).toLowerCase();
      if (!text.includes(search)) return false;
    }
    return true;
  });
}

// ==========================
//  DROPDOWNS
// ==========================
function initFilters() {
  var sdohLevelSelect = document.getElementById("filter-sdoh-level");
  var countySelect = document.getElementById("filter-county");
  var zipSelect = document.getElementById("filter-zip");
  var planSelect = document.getElementById("filter-plan");
  var contractSelect = document.getElementById("filter-contract");
  var raceSelect = document.getElementById("filter-race");
  var riskLevelSelect = document.getElementById("filter-risk-level");
  var ageGroupSelect = document.getElementById("filter-age-group");
  var focusBtn = document.getElementById("btn-focus-high");

  var sdohLevels = sortByPriority(
    uniqueSorted(DATA.map(function(d) { return d.sdoh_lift_level; })),
    SDOH_LEVEL_ORDER
  );
  sdohLevels.forEach(function(v) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    sdohLevelSelect.appendChild(opt);
  });

  var counties = uniqueSorted(DATA.map(function(d) { return d.county; }));
  counties.forEach(function(v) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    countySelect.appendChild(opt);
  });

  var zips = uniqueSorted(DATA.map(function(d) { return d.zip; }));
  zips.forEach(function(v) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    zipSelect.appendChild(opt);
  });

  var plans = uniqueSorted(DATA.map(function(d) { return d.plan; }));
  plans.forEach(function(v) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    planSelect.appendChild(opt);
  });

  var contracts = uniqueSorted(DATA.map(function(d) { return d.contract; }));
  contracts.forEach(function(v) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    contractSelect.appendChild(opt);
  });

  var races = uniqueSorted(DATA.map(function(d) { return d.race; }));
  races.forEach(function(v) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    raceSelect.appendChild(opt);
  });

  var riskBands = sortByPriority(
    uniqueSorted(
      DATA.map(function(d) { return d.risk_band; }).concat(DEFAULT_RISK_BANDS)
    ),
    DEFAULT_RISK_BANDS
  );
  riskBands.filter(function(v) { return String(v).toLowerCase() !== "unknown risk"; }).forEach(function(v) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    riskLevelSelect.appendChild(opt);
  });

  var ageGroups = uniqueSorted(DATA.map(function(d) { return d.age_group; }));
  ageGroups.forEach(function(v) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    ageGroupSelect.appendChild(opt);
  });

  sdohLevelSelect.addEventListener("change", function(e) {
    state.filters.sdoh_level = e.target.value;
    renderAll();
  });
  countySelect.addEventListener("change", function(e) {
    state.filters.county = e.target.value;
    renderAll();
  });
  zipSelect.addEventListener("change", function(e) {
    state.filters.zip = e.target.value;
    renderAll();
  });
  planSelect.addEventListener("change", function(e) {
    state.filters.plan = e.target.value;
    renderAll();
  });
  contractSelect.addEventListener("change", function(e) {
    state.filters.contract = e.target.value;
    renderAll();
  });
  raceSelect.addEventListener("change", function(e) {
    state.filters.race = e.target.value;
    renderAll();
  });
  riskLevelSelect.addEventListener("change", function(e) {
    state.filters.risk_level = e.target.value;
    renderAll();
  });
  ageGroupSelect.addEventListener("change", function(e) {
    state.filters.age_group = e.target.value;
    renderAll();
  });

  document.getElementById("member-search").addEventListener("input", function(e) {
    state.filters.search = e.target.value;
    renderAll();
  });

  document.getElementById("btn-reset").addEventListener("click", function() {
    state.filters = {
      sdoh_level: "",
      county: "",
      zip: "",
      plan: "",
      contract: "",
      race: "",
      risk_level: "",
      age_group: "",
      search: ""
    };
    state.focusHighActive = false;
    state.sdohDistributionFilter = "";
    state.selectedZip = "";
    state.selectedContract = "";
    sdohLevelSelect.value = "";
    countySelect.value = "";
    zipSelect.value = "";
    planSelect.value = "";
    contractSelect.value = "";
    raceSelect.value = "";
    riskLevelSelect.value = "";
    ageGroupSelect.value = "";
    document.getElementById("member-search").value = "";
    focusBtn.classList.remove("active");
    renderAll();
  });

  focusBtn.addEventListener("click", function() {
    state.focusHighActive = !state.focusHighActive;
    focusBtn.classList.toggle("active", state.focusHighActive);
    if (state.focusHighActive) {
      state.filters.sdoh_level = "";
      sdohLevelSelect.value = "";
    }
    renderAll();
  });
}

// ==========================
//  MEMBER KPIs & DISTRIBUTION
// ==========================
function renderMemberKpis(filtered) {
  var container = document.getElementById("member-kpi-grid");
  container.innerHTML = "";
  if (!filtered.length) {
    container.innerHTML = "<div class='kpi-card'><div class='kpi-label'>No members</div><div class='kpi-main'>0</div><div class='kpi-sub'>Adjust filters to see results.</div></div>";
    return;
  }

  var n = filtered.length;
  var avgFull = 0, avgNoSdoh = 0, avgLift = 0;
  var highCount = 0;
  var protectiveCount = 0;

  var driverCounts = {};
  filtered.forEach(function(d) {
    if (d.risk_full !== null) avgFull += d.risk_full;
    if (d.risk_no_sdoh !== null) avgNoSdoh += d.risk_no_sdoh;
    if (d.sdoh_lift !== null) avgLift += d.sdoh_lift;
    if (isHighBurden(d.sdoh_lift_level)) highCount += 1;
    if (sdohBadgeClass(d.sdoh_lift_level) === "protective") protectiveCount += 1;

    var sd1 = d.sdoh_driver_1;
    if (sd1) {
      if (!driverCounts[sd1]) driverCounts[sd1] = 0;
      driverCounts[sd1] += 1;
    }
  });

  avgFull /= n;
  avgNoSdoh /= n;
  avgLift /= n;
  var pctHigh = highCount / n;
  var pctProtective = protectiveCount / n;

  var topDriver = "-";
  var maxCount = 0;
  Object.keys(driverCounts).forEach(function(k) {
    if (driverCounts[k] > maxCount) {
      maxCount = driverCounts[k];
      topDriver = k;
    }
  });

  var cards = [
    {
      label: "Members in cohort",
      main: String(n),
      sub: "After filters",
      pill: "Cohort size",
      pillClass: ""
    },
    {
      label: "Avg predicted (with SDOH)",
      main: fmtNumber(avgFull, 3),
      sub: "Model with SDOH features",
      pill: "",
      pillClass: ""
    },
    {
      label: "Avg predicted (no SDOH)",
      main: fmtNumber(avgNoSdoh, 3),
      sub: "Model without SDOH features",
      pill: "",
      pillClass: ""
    },
    {
      label: "Avg SDOH lift",
      main: fmtNumber(avgLift, 3),
      sub: "Risk with SDOH - Risk no SDOH",
      pill: avgLift > 0.2 ? "Risk amplified" : (avgLift < 0 ? "Protective" : "Mild"),
      pillClass: avgLift > 0.2 ? "bad" : (avgLift < 0 ? "good" : "")
    },
    {
      label: "% high burden",
      main: fmtPercent(pctHigh, 1),
      sub: "% with Significant / Extreme SDOH",
      pill: "Protective: " + fmtPercent(pctProtective, 1),
      pillClass: ""
    },
    {
      label: "Leading SDOH driver",
      main: topDriver !== "-" ? "<span class='kpi-main wrap'>" + prettifyDriverName(topDriver) + "</span>" : "No driver surfaced",
      sub: maxCount ? fmtPercent(maxCount / n, 1) + " of cohort have this driver" : "Driver rank shown after loading data",
      pill: maxCount ? (String(maxCount) + " members") : "",
      pillClass: ""
    }
  ];

  cards.forEach(function(c) {
    var card = document.createElement("div");
    card.className = "kpi-card";
    var inner = "<div class='kpi-label'>" + c.label + "</div>" +
      "<div class='kpi-main'>" + c.main + "</div>" +
      "<div class='kpi-sub'>" + c.sub + "</div>";
    if (c.pill) {
      inner += "<div class='kpi-pill " + (c.pillClass || "") + "'>" + c.pill + "</div>";
    }
    card.innerHTML = inner;
    container.appendChild(card);
  });

  var tag = document.getElementById("cohort-tag");
  tag.textContent = "Filtered cohort: " + String(n) + " members";
}

function renderDistributionBars(filtered, distributionUniverse) {
  var container = document.getElementById("dist-bars");
  container.innerHTML = "";
  var source = Array.isArray(distributionUniverse) && distributionUniverse.length >= 0 ? distributionUniverse : filtered;

  var configs = [
    { label: "Protective", classKey: "protective", color: "var(--protective)" },
    { label: "Mild", classKey: "mild", color: "var(--success)" },
    { label: "Significant", classKey: "significant", color: "var(--warning)" },
    { label: "Extreme", classKey: "extreme", color: "var(--danger)" }
  ];
  var buckets = {};
  configs.forEach(function(cfg) { buckets[cfg.classKey] = 0; });

  source.forEach(function(d) {
    var cl = sdohBadgeClass(d.sdoh_lift_level);
    if (buckets.hasOwnProperty(cl)) {
      buckets[cl] += 1;
    }
  });

  var n = source.length || 0;
  var activeClass = state.sdohDistributionFilter;

  configs.forEach(function(cfg) {
    var val = buckets[cfg.classKey] || 0;
    var pct = n ? val / n : 0;
    var bar = document.createElement("div");
    bar.className = "dist-bar" + (activeClass === cfg.classKey ? " active" : "");
    bar.dataset.bucket = cfg.classKey;
    bar.innerHTML =
      "<div class='dist-bar-label'>" + cfg.label + "</div>" +
      "<div class='dist-bar-value'>" + String(val) + " (" + fmtPercent(pct, 1) + ")</div>" +
      "<div class='dist-bar-fill'><div class='dist-bar-fill-inner' style='width: " + (pct * 100).toFixed(1) + "%; background: " + cfg.color + ";'></div></div>";
    bar.addEventListener("click", function() {
      if (state.sdohDistributionFilter === cfg.classKey) {
        state.sdohDistributionFilter = "";
      } else {
        state.sdohDistributionFilter = cfg.classKey;
      }
      renderAll();
    });
    container.appendChild(bar);
  });

  var label = document.getElementById("member-count-label");
  label.textContent = "Members in view: " + String(filtered.length);
}


// ==========================
//  MEMBER TABLE & DETAIL
// ==========================
function renderMemberTable(filtered) {
  var tbody = document.querySelector("#member-table tbody");
  tbody.innerHTML = "";
  var selfIdx = state.selectedMemberIdx;
  var rows = filtered.slice().sort(function(a, b) {
    var aId = parseInt(String(a.member || "").replace(/^\\D+/, ""), 10);
    var bId = parseInt(String(b.member || "").replace(/^\\D+/, ""), 10);
    if (isNaN(aId)) aId = Number.MAX_SAFE_INTEGER;
    if (isNaN(bId)) bId = Number.MAX_SAFE_INTEGER;
    if (aId !== bId) return aId - bId;
    return String(a.member || "").localeCompare(String(b.member || ""));
  });
  rows.forEach(function(d) {
    var tr = document.createElement("tr");
    tr.dataset.idx = String(d._idx);

    if (selfIdx !== null && d._idx === selfIdx) {
      tr.classList.add("selected");
    }

    var liftStr = d.sdoh_lift === null ? "-" : fmtNumber(d.sdoh_lift, 3);
    var intervention = describeIntervention(d);
    var badgeCl = sdohBadgeClass(d.sdoh_lift_level);
    var badgeClass = "badge-level";
    if (badgeCl) badgeClass += " " + badgeCl;

        tr.innerHTML =
          "<td><span class='pill-small'>" + d.member + "</span> " + d.member_name + "</td>" +
          "<td>" + (d.age !== null ? d.age : "-") + "</td>" +
          "<td>" + d.race + "</td>" +
          "<td>" + d.plan + "</td>" +
          "<td>" + (d.contract || "-") + "</td>" +
          "<td>" + d.county + "</td>" +
          "<td>" + d.zip + "</td>" +
          "<td>" + fmtNumber(d.risk_full, 3) + "</td>" +
          "<td>" + fmtNumber(d.risk_no_sdoh, 3) + "</td>" +
          "<td>" + liftStr + "</td>" +
          "<td><span class='" + badgeClass + "'>" + d.sdoh_lift_level + "</span></td>" +
          "<td>" + intervention + "</td>";

    tr.addEventListener("click", function() {
      state.selectedMemberIdx = d._idx;
      renderMemberTable(filtered);
      renderMemberDetail(d);
    });

    tbody.appendChild(tr);
  });

  if (rows.length && (state.selectedMemberIdx === null || !rows.some(function(x){ return x._idx === state.selectedMemberIdx;}))) {
    state.selectedMemberIdx = rows[0]._idx;
    renderMemberTable(filtered);
    renderMemberDetail(rows[0]);
  } else if (!rows.length) {
    state.selectedMemberIdx = null;
    renderMemberDetail(null);
  }
}

function formatDateHuman(dateStr) {
  if (!dateStr) return "";
  var date = new Date(dateStr + "T00:00:00");
  if (isNaN(date.getTime())) return dateStr;
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function buildCampaignEnrollmentPanel(member) {
  var wrapper = document.createElement("div");
  wrapper.className = "campaign-enrollment";

  var head = document.createElement("div");
  head.className = "campaign-enrollment-head";
  head.innerHTML = "<strong>Campaign enrollment</strong><span>Auto-add with manual override</span>";
  wrapper.appendChild(head);

  if (!member || !state.campaigns.length) {
    var empty = document.createElement("div");
    empty.className = "campaign-enrollment-empty";
    empty.textContent = "No campaigns available.";
    wrapper.appendChild(empty);
    return wrapper;
  }

  var select = document.createElement("select");
  state.campaigns.forEach(function(c) {
    var opt = document.createElement("option");
    opt.value = c.id;
    opt.textContent = c.name;
    select.appendChild(opt);
  });
  if (state.selectedCampaignId) {
    select.value = state.selectedCampaignId;
  }
  wrapper.appendChild(select);

  var status = document.createElement("div");
  status.className = "campaign-enrollment-status";
  wrapper.appendChild(status);

  var methodRow = document.createElement("div");
  methodRow.className = "campaign-enrollment-method";
  var methodLabel = document.createElement("div");
  methodLabel.textContent = "Outreach method";
  var methodSelect = document.createElement("select");
  methodRow.appendChild(methodLabel);
  methodRow.appendChild(methodSelect);
  wrapper.appendChild(methodRow);

  var actionRow = document.createElement("div");
  actionRow.className = "campaign-enrollment-actions";
  var actionBtn = document.createElement("button");
  actionBtn.type = "button";
  actionBtn.className = "btn-ghost btn-small";
  var viewBtn = document.createElement("button");
  viewBtn.type = "button";
  viewBtn.className = "btn-ghost btn-small";
  viewBtn.textContent = "View campaign";
  actionRow.appendChild(actionBtn);
  actionRow.appendChild(viewBtn);
  wrapper.appendChild(actionRow);

  function refreshCampaignUI() {
    var campaign = getCampaignById(select.value);
    if (!campaign) return;
    state.selectedCampaignId = campaign.id;
    var enrolled = isCampaignEnrolled(campaign, member);
    var eligible = isCampaignEligible(campaign, member);
    var source = enrollmentSource(campaign, member);
    var record = getCampaignEnrollment(campaign.id, member.member) || {};

    status.textContent = source === "Auto"
      ? "Auto-enrolled (eligible)"
      : source === "Manual"
        ? "Manually enrolled"
        : source === "Excluded"
          ? "Excluded (override)"
          : eligible ? "Eligible but not enrolled" : "Not eligible";

    methodSelect.innerHTML = "";
    var methodOpt = document.createElement("option");
    methodOpt.value = "";
    methodOpt.textContent = "Select method";
    methodSelect.appendChild(methodOpt);
  (campaign.outreachMethods || []).forEach(function(m) {
    var opt = document.createElement("option");
    opt.value = m;
    opt.textContent = m;
    methodSelect.appendChild(opt);
  });
    var methodValue = String(record.outreachMethod || "");
    var methodKey = methodValue.toLowerCase();
    if (methodKey.indexOf("phone/sms") !== -1 || (methodKey.indexOf("phone") !== -1 && methodKey.indexOf("sms") !== -1)) {
      methodValue = "SMS";
    } else if (methodKey.indexOf("sms") !== -1) {
      methodValue = "SMS";
    } else if (methodKey.indexOf("phone") !== -1) {
      methodValue = "Phone";
    } else if (methodKey.indexOf("email") !== -1 || methodKey.indexOf("mail") !== -1) {
      methodValue = "Email";
    } else if (methodValue && (campaign.outreachMethods || []).length && !campaign.outreachMethods.includes(methodValue)) {
      methodValue = "";
    }
    methodSelect.value = methodValue;
    methodSelect.disabled = !enrolled;

    if (enrolled) {
      actionBtn.textContent = eligible && !record.override ? "Remove from campaign" : "Remove from campaign";
    } else {
      actionBtn.textContent = eligible ? "Re-enable enrollment" : "Add manually";
    }
  }

  function applyEnrollment(enroll) {
    var campaign = getCampaignById(select.value);
    if (!campaign) return;
    var eligible = isCampaignEligible(campaign, member);
    if (enroll) {
      if (eligible) {
        setCampaignEnrollment(campaign.id, member.member, { override: "" });
      } else {
        setCampaignEnrollment(campaign.id, member.member, { override: "include" });
      }
    } else {
      if (eligible) {
        setCampaignEnrollment(campaign.id, member.member, { override: "exclude" });
      } else {
        setCampaignEnrollment(campaign.id, member.member, { override: "" });
      }
    }
    refreshCampaignUI();
  }

  select.addEventListener("change", function() {
    saveCampaignState();
    refreshCampaignUI();
  });

  methodSelect.addEventListener("change", function() {
    var campaign = getCampaignById(select.value);
    if (!campaign) return;
    setCampaignEnrollment(campaign.id, member.member, { outreachMethod: methodSelect.value });
    refreshCampaignUI();
  });

  actionBtn.addEventListener("click", function() {
    var campaign = getCampaignById(select.value);
    if (!campaign) return;
    var enrolled = isCampaignEnrolled(campaign, member);
    applyEnrollment(!enrolled);
  });

  viewBtn.addEventListener("click", function() {
    state.selectedCampaignId = select.value;
    state.page = "campaign-page";
    renderAll();
    closeInterventionModal();
  });

  refreshCampaignUI();
  return wrapper;
}

function buildPreferredInterventionCard(member) {

  function buildFooter(disabled) {
    var footer = document.createElement("div");
    footer.className = "intervention-footer";

    var btnCare = document.createElement("button");
    btnCare.type = "button";
    btnCare.className = "btn-ghost intervention-footer-btn";
    btnCare.textContent = "Assign Care Navigator";
    btnCare.disabled = disabled;

    var btnOutreach = document.createElement("button");
    btnOutreach.type = "button";
    btnOutreach.className = "btn-ghost intervention-footer-btn";
    btnOutreach.textContent = "Schedule Outreach";
    btnOutreach.disabled = disabled;

    var btnClose = document.createElement("button");
    btnClose.type = "button";
    btnClose.className = "btn-ghost intervention-footer-btn ghost-secondary";
    btnClose.textContent = "Close";
    btnClose.style.display = "";

    footer.appendChild(btnCare);
    footer.appendChild(btnOutreach);
    footer.appendChild(btnClose);
    return {
      footer: footer,
      btnCare: btnCare,
      btnOutreach: btnOutreach,
      btnClose: btnClose
    };
  }

  var card = document.createElement("div");
  card.className = "intervention-card";

  var label = document.createElement("div");
  label.className = "intervention-label";
  var iconSvg =
    "<svg viewBox='0 0 24 24' role='img' aria-hidden='true'>" +
      "<path d='M12 2c-2.21 0-4 1.79-4 4v2H6a4 4 0 000 8h2v6h8v-6h2a4 4 0 000-8h-2V6c0-2.21-1.79-4-4-4zm0 2a2 2 0 012 2v2h-4V6a2 2 0 012-2zm-6 8a2 2 0 010-4h2v4H6zm12 0h-2v-4h2a2 2 0 010 4z' fill='currentColor'/>" +
    "</svg>";
  label.innerHTML = "<span class='intervention-icon'>" + iconSvg + "</span><span>Preferred intervention</span>";
  var headRow = document.createElement("div");
  headRow.className = "intervention-head";
  headRow.appendChild(label);
  var exportBtn = document.createElement("button");
  exportBtn.type = "button";
  exportBtn.className = "btn-ghost btn-small intervention-export-btn";
  exportBtn.textContent = "Download CSV";
  exportBtn.disabled = !member;
  headRow.appendChild(exportBtn);
  card.appendChild(headRow);

  if (!member) {
    var summaryEmpty = document.createElement("div");
    summaryEmpty.className = "intervention-summary";
    summaryEmpty.textContent = "Select a member to surface recommended actions.";
    card.appendChild(summaryEmpty);
    var emptyControls = buildFooter(true);
    card.appendChild(emptyControls.footer);
    return card;
  }
  exportBtn.addEventListener("click", function() {
    exportPreferredInterventionCsv(member);
  });

  var memberPicker = document.createElement("div");
  memberPicker.className = "intervention-member-picker";
  memberPicker.innerHTML =
    "<label for='intervention-member-search'>Search member</label>" +
    "<div class='member-search-wrap'>" +
      "<input id='intervention-member-search' type='text' placeholder='Search by member ID or name' autocomplete='off' />" +
      "<div class='member-search-dropdown' id='intervention-member-dropdown'></div>" +
    "</div>";
  card.appendChild(memberPicker);

  var searchInput = memberPicker.querySelector("#intervention-member-search");
  var dropdown = memberPicker.querySelector("#intervention-member-dropdown");
  if (searchInput && dropdown) {
    searchInput.value = member.member + " - " + member.member_name;
    function renderMemberDropdown(query) {
      var val = String(query || "").trim().toLowerCase();
      var matches = (DATA || []).filter(function(d) {
        if (!val) return true;
        var id = String(d.member || "").toLowerCase();
        var name = String(d.member_name || "").toLowerCase();
        return id.indexOf(val) !== -1 || name.indexOf(val) !== -1 || (id + " - " + name).indexOf(val) !== -1;
      }).slice(0, 40);
      dropdown.innerHTML = "";
      if (!matches.length) {
        var empty = document.createElement("div");
        empty.className = "member-search-empty";
        empty.textContent = "No matches";
        dropdown.appendChild(empty);
        return;
      }
      matches.forEach(function(d) {
        var item = document.createElement("button");
        item.type = "button";
        item.className = "member-search-item";
        item.textContent = d.member + " - " + d.member_name;
        item.addEventListener("click", function() {
          searchInput.value = d.member + " - " + d.member_name;
          dropdown.classList.remove("open");
          populateInterventionModal(d);
        });
        dropdown.appendChild(item);
      });
    }
    function openDropdown() {
      renderMemberDropdown(searchInput.value);
      dropdown.classList.add("open");
    }
    searchInput.addEventListener("focus", openDropdown);
    searchInput.addEventListener("input", function() {
      renderMemberDropdown(searchInput.value);
      dropdown.classList.add("open");
    });
    searchInput.addEventListener("keydown", function(evt) {
      if (evt.key === "Escape") {
        dropdown.classList.remove("open");
      }
    });
    document.addEventListener("click", function(evt) {
      if (!memberPicker.contains(evt.target)) {
        dropdown.classList.remove("open");
      }
    });
  }

  var memberInfo = document.createElement("div");
  memberInfo.className = "intervention-member-info";
  memberInfo.innerHTML =
    "<div class='intervention-member-title'>Member Information</div>" +
    "<div class='intervention-member-grid'>" +
      "<div><span>Member</span><strong>" + member.member_name + " (" + member.member + ")</strong></div>" +
      "<div><span>Age</span><strong>" + (member.age || "-") + "</strong></div>" +
      "<div><span>Plan</span><strong>" + (member.plan || "-") + "</strong></div>" +
      "<div><span>ZIP</span><strong>" + (member.zip || "-") + "</strong></div>" +
      "<div><span>Risk with SDOH</span><strong>" + fmtNumber(member.risk_full, 2) + "</strong></div>" +
      "<div><span>SDOH lift</span><strong>" + fmtSignedNumber(member.sdoh_lift, 3) + "</strong></div>" +
    "</div>";
  card.appendChild(memberInfo);

  var defaultKey = String(member.sdoh_driver_1 || "").toLowerCase();
  var overrideKey = state.memberOverrides[member.member];
  var preferredPlan = getInterventionPlanByKey(defaultKey);
  var overridePlan = overrideKey ? getInterventionPlanByKey(overrideKey) : null;
  var overrideActive = Boolean(overrideKey);
  var overrideUnavailable = INTERVENTION_CHOICES.length === 0;

  var summary = document.createElement("div");
  summary.className = "intervention-summary";
  var overrideBadge = overrideActive ? "<span class='override-pill'>Override</span>" : "";
  var disableAttr = overrideUnavailable ? " disabled" : "";
  var planTitle = preferredPlan ? preferredPlan.title : "Preferred intervention";
  var planSummary = preferredPlan ? preferredPlan.summary : "No recommendation available.";
  summary.innerHTML =
    "<div class='intervention-title-row'>" +
      "<div class='intervention-title-stack'><strong>" + planTitle + "</strong> " + overrideBadge + "</div>" +
      "<button type='button' class='btn-ghost btn-small intervention-override'" + disableAttr + " title='Override Preferred Intervention'>Select Intervention</button>" +
    "</div>" +
    "<span>" + planSummary + "</span>";
  card.appendChild(summary);
  var overrideBtn = summary.querySelector(".intervention-override");
  if (overrideUnavailable && overrideBtn) {
    overrideBtn.disabled = true;
  }

  if (preferredPlan && preferredPlan.actions && preferredPlan.actions.length) {
    var list = document.createElement("ul");
    list.className = "intervention-actions";
    preferredPlan.actions.forEach(function(action) {
      var li = document.createElement("li");
      li.textContent = action;
      list.appendChild(li);
    });
    card.appendChild(list);
  }

  var overridePanel = null;
  if (!overrideUnavailable) {
    overridePanel = document.createElement("div");
    overridePanel.className = "intervention-override-panel collapsed";

    var overrideLabel = document.createElement("div");
    overrideLabel.className = "override-panel-label";
    overrideLabel.textContent = "Override preferred intervention";
    overridePanel.appendChild(overrideLabel);

    var overrideSelect = document.createElement("select");
    var optAuto = document.createElement("option");
    optAuto.value = "";
    optAuto.textContent = "Use preferred recommendation";
    overrideSelect.appendChild(optAuto);

    INTERVENTION_CHOICES.forEach(function(opt) {
      var option = document.createElement("option");
      option.value = opt.key;
      option.textContent = opt.label;
      overrideSelect.appendChild(option);
    });

    if (overrideKey) {
      overrideSelect.value = overrideKey;
    }

    overridePanel.appendChild(overrideSelect);

    var overrideActions = document.createElement("div");
    overrideActions.className = "override-actions";

    var overrideApply = document.createElement("button");
    overrideApply.type = "button";
    overrideApply.className = "btn-ghost btn-small";
    overrideApply.textContent = "Apply override";

    overrideApply.addEventListener("click", function() {
      var value = overrideSelect.value;
      if (!value) {
        delete state.memberOverrides[member.member];
      } else {
        state.memberOverrides[member.member] = value;
      }
      overridePanel.classList.add("collapsed");
      renderAll();
    });

    overrideActions.appendChild(overrideApply);
    overridePanel.appendChild(overrideActions);
    card.appendChild(overridePanel);

    overrideBtn.addEventListener("click", function() {
      overridePanel.classList.toggle("collapsed");
    });
  }

  var storedNavigatorRef = state.careNavigatorAssignments[member.member];
  var assignedNavigator = null;
  if (storedNavigatorRef && typeof storedNavigatorRef === "object") {
    assignedNavigator = storedNavigatorRef;
    if (assignedNavigator.id) {
      state.careNavigatorAssignments[member.member] = assignedNavigator.id;
    }
  } else {
    assignedNavigator = getNavigatorById(storedNavigatorRef);
  }
  var assignedNavigatorId = assignedNavigator ? assignedNavigator.id : state.careNavigatorAssignments[member.member];
  var careWrap = document.createElement("div");
  careWrap.className = "intervention-mini-card";

  var careRow = document.createElement("div");
  careRow.className = "care-nav-row";

  var careLabel = document.createElement("div");
  careLabel.className = "care-nav-label";
  careLabel.innerHTML = "<span>Care navigator</span><small>Owns SDOH follow-up</small>";

  var careValue = document.createElement("div");
  careValue.className = "care-nav-value";
  function updateCareValue(nav) {
    if (!nav) {
      careValue.textContent = "None assigned";
      careValue.classList.add("empty");
    } else {
      careValue.textContent = nav.name + " • " + nav.specialty;
      careValue.classList.remove("empty");
    }
  }
  updateCareValue(assignedNavigator);
  careRow.appendChild(careLabel);
  careRow.appendChild(careValue);
  careWrap.appendChild(careRow);

  var carePanel = document.createElement("div");
  carePanel.className = "care-nav-panel collapsed";
  var careSelect = document.createElement("select");
  var defCareOpt = document.createElement("option");
  defCareOpt.value = "";
  defCareOpt.textContent = "Select care navigator";
  careSelect.appendChild(defCareOpt);
  CARE_NAVIGATORS.forEach(function(nav) {
    var option = document.createElement("option");
    option.value = nav.id;
    option.textContent = nav.name + " — " + nav.specialty;
    careSelect.appendChild(option);
  });
  if (assignedNavigator && assignedNavigator.id) {
    careSelect.value = assignedNavigator.id;
  } else if (assignedNavigatorId) {
    careSelect.value = assignedNavigatorId;
  }
  carePanel.appendChild(careSelect);
  careWrap.appendChild(carePanel);

  var outreachWrap = document.createElement("div");
  outreachWrap.className = "intervention-mini-card";

  var scheduler = document.createElement("div");
  scheduler.className = "intervention-scheduler";

  var schedulerLabel = document.createElement("div");
  schedulerLabel.className = "scheduler-label";
  schedulerLabel.textContent = "Select outreach date";

  var schedulerInput = document.createElement("input");
  schedulerInput.type = "date";
  schedulerInput.className = "scheduler-input";

  var schedulerStatus = document.createElement("div");
  schedulerStatus.className = "scheduler-status";
  schedulerStatus.textContent = "No outreach scheduled.";

  scheduler.appendChild(schedulerLabel);
  scheduler.appendChild(schedulerInput);
  scheduler.appendChild(schedulerStatus);
  outreachWrap.appendChild(scheduler);
  var existingSchedule = state.outreachSchedules[member.member];
  if (existingSchedule) {
    schedulerInput.value = existingSchedule;
    schedulerStatus.textContent = "Outreach scheduled for " + formatDateHuman(existingSchedule) + ".";
    scheduler.classList.add("scheduled");
  }

  var controls = buildFooter(false);
  card.appendChild(controls.footer);


  careSelect.addEventListener("change", function() {
    var selectedId = careSelect.value;
    if (!selectedId) {
      delete state.careNavigatorAssignments[member.member];
      updateCareValue(null);
    } else {
      var chosen = getNavigatorById(selectedId);
      if (chosen) {
        state.careNavigatorAssignments[member.member] = selectedId;
        updateCareValue(chosen);
      }
    }
    carePanel.classList.add("collapsed");
  });

  controls.btnOutreach.addEventListener("click", function() {
    if (schedulerInput.showPicker) {
      schedulerInput.showPicker();
    } else {
      schedulerInput.focus();
    }
  });

  controls.btnClose.addEventListener("click", function() {
    closeInterventionModal();
  });

  schedulerInput.addEventListener("change", function() {
    if (schedulerInput.value) {
      schedulerStatus.textContent = "Outreach scheduled for " + formatDateHuman(schedulerInput.value) + ".";
      scheduler.classList.add("scheduled");
      state.outreachSchedules[member.member] = schedulerInput.value;
    } else {
      schedulerStatus.textContent = "No outreach scheduled.";
      scheduler.classList.remove("scheduled");
      delete state.outreachSchedules[member.member];
    }
  });

  controls.btnCare.addEventListener("click", function() {
    if (controls.btnCare.disabled) return;
    carePanel.classList.toggle("collapsed");
    if (!carePanel.classList.contains("collapsed")) {
      careSelect.focus();
    }
  });

  var campaignPanel = buildCampaignEnrollmentPanel(member);
  campaignPanel.classList.add("intervention-mini-card");

  var miniGrid = document.createElement("div");
  miniGrid.className = "intervention-mini-grid";
  miniGrid.appendChild(careWrap);
  miniGrid.appendChild(outreachWrap);
  miniGrid.appendChild(campaignPanel);
  card.appendChild(miniGrid);
  return card;
}

function populateInterventionModal(member) {
  var body = document.getElementById("intervention-modal-body");
  if (!body) return;
  body.innerHTML = "";
  body.appendChild(buildPreferredInterventionCard(member));
}

function openInterventionModal(member) {
  if (!member) return;
  var modal = document.getElementById("intervention-modal");
  if (!modal) return;
  populateInterventionModal(member);
  modal.classList.add("open");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("modal-open");
  state.isInterventionModalOpen = true;
}

function closeInterventionModal() {
  var modal = document.getElementById("intervention-modal");
  if (!modal) return;
  modal.classList.remove("open");
  modal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("modal-open");
  state.isInterventionModalOpen = false;
}

function initInterventionModal() {
  var modal = document.getElementById("intervention-modal");
  if (!modal) return;
  var closeBtn = document.getElementById("intervention-modal-close");
  var backdrop = modal.querySelector(".member-modal-backdrop");
  if (closeBtn) closeBtn.addEventListener("click", closeInterventionModal);
  if (backdrop) backdrop.addEventListener("click", closeInterventionModal);
  document.addEventListener("keydown", function(evt) {
    if (evt.key === "Escape" && state.isInterventionModalOpen) {
      closeInterventionModal();
    }
  });
}

function updatePreferredInterventionPanel(member) {
  var slot = document.getElementById("preferred-intervention-slot");
  if (!slot) return;
  slot.innerHTML = "";

  var row = document.createElement("div");
  row.className = "intervention-launch-row";

  var btn = document.createElement("button");
  btn.type = "button";
  btn.className = "btn-primary btn-small intervention-launch";
  btn.textContent = "Preferred Intervention";
  btn.disabled = !member;
  btn.addEventListener("click", function() {
    openInterventionModal(member);
  });

  var hint = document.createElement("div");
  hint.className = "intervention-launch-hint";
  if (member) {
    hint.innerHTML = "";
  } else {
    hint.textContent = "Select a member to view recommended actions.";
  }

  row.appendChild(btn);
  row.appendChild(hint);
  slot.appendChild(row);

  if (state.isInterventionModalOpen) {
    populateInterventionModal(member);
  }
}
function renderMemberDetail(d) {
  var panel = document.getElementById("member-detail-panel");
  var tag = document.getElementById("selected-member-tag");
  var detailExportBtn = document.getElementById("btn-export-member-detail");
  var printBtn = document.getElementById("btn-print-member-detail");
  if (!d) {
    panel.innerHTML = "<p style='font-size:12px; color:var(--text-muted);'>Select a row to view member details and SDOH drivers.</p>";
    tag.textContent = "No member selected";
    updatePreferredInterventionPanel(null);
    destroyRadarCharts();
    if (detailExportBtn) detailExportBtn.disabled = true;
    if (printBtn) printBtn.disabled = true;
    closeMemberModal();
    return;
  }
  if (detailExportBtn) detailExportBtn.disabled = false;
  if (printBtn) printBtn.disabled = false;

  tag.textContent = d.member + " | " + d.zip + " | " + d.sdoh_lift_level;

  var liftText = d.sdoh_lift === null ? "-" : fmtNumber(d.sdoh_lift, 3);
  var liftClass = "lift-indicator";
  if (d.sdoh_lift !== null) {
    liftClass += d.sdoh_lift > 0 ? " positive" : " negative";
  }

  var sdohDrivers = extractDrivers(d, "sdoh_driver_");
  var nonDrivers = extractDrivers(d, "nonsdoh_driver_");

  function renderDriverCards(list, title) {
    var maxAbs = 0;
    list.forEach(function(item) {
      if (item.value !== null && item.value !== undefined) {
        var v = Math.abs(item.value);
        if (v > maxAbs) maxAbs = v;
      }
    });
    if (maxAbs === 0) maxAbs = 1;

    var html =
      "<div class='driver-card'>" +
      "<div class='driver-card-title'>" + title + "</div>";

    if (!list.length) {
      html +=
        "<div style='font-size:12px; color:var(--text-muted); margin-top:4px;'>" +
        "No drivers available." +
        "</div>";
    } else {
      list.forEach(function(item) {
        var value = item.value !== null && item.value !== undefined ? item.value : 0;
        var scaledValue = Math.min(Math.abs(value) / maxAbs * 50, 50);
        var signClass = value >= 0 ? "positive" : "negative";

        html +=
          "<div class='driver-item'>" +
            "<div class='driver-name'>" + item.name + "</div>" +
            "<div class='driver-bar' title='Contribution: " + fmtNumber(value, 4) + "'>" +
              "<div class='driver-bar-center-line'></div>" +
              "<div class='driver-bar-inner " + signClass + "' style='width:" + scaledValue.toFixed(1) + "%;'></div>" +
            "</div>" +
            "<div class='driver-value'>" + fmtNumber(value, 4) + "</div>" +
          "</div>";
      });
    }

    html += "</div>";
    return html;
  }


  var html =
    "<div class='member-header-block'>" +
    "<div class='member-header-main'>" +
      "<div class='member-name-line'>" + d.member_name + " <span style='font-size:12px; color:var(--text-muted);'>(" + d.member + ")</span></div>" +
      "<div class='member-meta-line'>" +
        "Age " + (d.age !== null ? d.age : "-") + " • " + d.gender + " • " + d.race +
        " • " + d.plan + (d.contract ? " • Contract " + d.contract : "") + " • " + d.county + ", " + d.state +
      "</div>" +
    "</div>" +
    "<div class='badge-member'>Age Group: " + d.age_group + "</div>" +
    "</div>";

  html +=
    "<div class='member-risk-row'>" +
      "<div class='metric-block'>" +
        "<div class='metric-label'>Predicted (full)</div>" +
        "<div class='metric-value'>" + fmtNumber(d.risk_full, 3) + "</div>" +
        "<div class='metric-sub'>Model with SDOH</div>" +
      "</div>" +
      "<div class='metric-block'>" +
        "<div class='metric-label'>Predicted (no SDOH)</div>" +
        "<div class='metric-value'>" + fmtNumber(d.risk_no_sdoh, 3) + "</div>" +
        "<div class='metric-sub'>Model w/out SDOH</div>" +
      "</div>" +
    "</div>";

  html +=
    "<div style='margin-top:8px; font-size:12px; color:var(--text-soft);'>" +
      "<span class='" + liftClass + "'>SDOH lift: " + liftText + "</span>" +
      " &nbsp; • &nbsp;<span style='color:var(--text-muted);'>Interpretation: " + d.sdoh_lift_level + "</span>" +
      "<br/><span style='color:var(--text-muted);'>Lift &gt; 0 means environmental factors are amplifying risk; lift &lt; 0 means SDOH is protective.</span>" +
    "</div>";

  html +=
    "<div class='driver-radar-section'>" +
      "<div class='driver-radar-title'>Driver Profile (Magnitude Radar)</div>" +
      "<div class='driver-radar-grid'>" +
        "<div class='driver-radar-card'>" +
          "<div class='driver-radar-label'>SDOH impact magnitude (normalized)</div>" +
          "<div class='driver-radar-canvas'><canvas id='sdohRadar'></canvas></div>" +
        "</div>" +
        "<div class='driver-radar-card'>" +
          "<div class='driver-radar-label'>Non-SDOH impact magnitude (normalized)</div>" +
          "<div class='driver-radar-canvas'><canvas id='nonSdohRadar'></canvas></div>" +
        "</div>" +
      "</div>" +
      "<div class='driver-radar-caption'>Shape shows relative driver strength (0-1). Direction (+/-) remains in the bar chart list.</div>" +
    "</div>";

  html += "<div class='driver-grid'>" +
    renderDriverCards(sdohDrivers, "Top 5 SDOH drivers") +
    renderDriverCards(nonDrivers, "Top 5 non-SDOH drivers") +
    "</div>";

  panel.innerHTML = html;
  renderRadarCharts(d);
  updatePreferredInterventionPanel(d);
  if (state.isMemberModalOpen) {
    populateMemberModal(d);
  }
}

// ==========================
//  ZIP AGGREGATION & MAP
// ==========================
function aggregateByZip(filtered) {
  if (!zipRiskBaselineMap) {
    zipRiskBaselineMap = {};
    zipRiskBaselineAvg = {};
    var base = {};
    (DATA || []).forEach(function(d) {
      var key = d.zip || "(blank)";
      if (!base[key]) {
        base[key] = { members: 0, sumRiskFull: 0 };
      }
      base[key].members += 1;
      if (d.risk_full !== null) base[key].sumRiskFull += d.risk_full;
    });
    Object.keys(base).forEach(function(k) {
      var g = base[k];
      var avg = g.members ? g.sumRiskFull / g.members : null;
      zipRiskBaselineAvg[k] = avg;
      zipRiskBaselineMap[k] = (avg !== null && !isNaN(avg)) ? riskBandFromValue(avg) : "Unknown risk";
    });

    // If everything falls into one band, reassign by tertiles for visual separation
    var zones = Object.values(zipRiskBaselineMap);
    var hasHigh = zones.some(function(z){ return String(z).toLowerCase() === "high risk"; });
    var hasLow = zones.some(function(z){ return String(z).toLowerCase() === "lower risk"; });
    if (!hasHigh || !hasLow) {
      var avgs = Object.keys(zipRiskBaselineAvg)
        .map(function(k){ return zipRiskBaselineAvg[k]; })
        .filter(function(v){ return v !== null && !isNaN(v); })
        .sort(function(a,b){ return a-b; });
      if (avgs.length) {
        var q1 = avgs[Math.floor(avgs.length * 0.33)];
        var q2 = avgs[Math.floor(avgs.length * 0.66)];
        Object.keys(zipRiskBaselineAvg).forEach(function(k) {
          var avg = zipRiskBaselineAvg[k];
          if (avg === null || isNaN(avg)) {
            zipRiskBaselineMap[k] = "Unknown risk";
          } else if (avg >= q2) {
            zipRiskBaselineMap[k] = "High risk";
          } else if (avg >= q1) {
            zipRiskBaselineMap[k] = "Moderate risk";
          } else {
            zipRiskBaselineMap[k] = "Lower risk";
          }
        });
      }
    }
  }

  var map = {};
  filtered.forEach(function(d) {
    var key = d.zip || "(blank)";
    if (!map[key]) {
      map[key] = {
        zip: key,
        county: d.county,
        state: d.state,
        members: 0,
        sumRiskFull: 0,
        sumLift: 0,
        highCount: 0,
        riskCounts: {}
      };
    }
    var g = map[key];
    g.members += 1;
    if (d.risk_full !== null) g.sumRiskFull += d.risk_full;
    if (d.sdoh_lift !== null) g.sumLift += d.sdoh_lift;
    if (isHighBurden(d.sdoh_lift_level)) g.highCount += 1;
    var band = d.risk_band || "Unknown risk";
    g.riskCounts[band] = (g.riskCounts[band] || 0) + 1;
  });

  var rows = Object.keys(map).map(function(k) {
    var g = map[k];
    var avgRF = g.members ? g.sumRiskFull / g.members : null;
    var avgLift = g.members ? g.sumLift / g.members : null;
    var pctHigh = g.members ? g.highCount / g.members : 0;
    var zone = zipRiskBaselineMap[g.zip] || "Unknown risk";
    if (zone === "Unknown risk") {
      var fallbackBand = null;
      var fallbackCount = 0;
      Object.keys(g.riskCounts).forEach(function(band) {
        var c = g.riskCounts[band] || 0;
        if (c > fallbackCount) {
          fallbackBand = band;
          fallbackCount = c;
        }
      });
      if (fallbackBand) zone = fallbackBand;
    }
    return {
      zip: g.zip,
      county: g.county,
      state: g.state,
      members: g.members,
      avgRiskFull: avgRF,
      avgLift: avgLift,
      pctHigh: pctHigh,
      riskZone: zone,
      riskZoneRank: riskBandRank(zone),
      highRiskShare: g.members ? (g.riskCounts["High risk"] || 0) / g.members : 0,
      moderateRiskShare: g.members ? (g.riskCounts["Moderate risk"] || 0) / g.members : 0,
      lowerRiskShare: g.members ? (g.riskCounts["Lower risk"] || 0) / g.members : 0
    };
  });

  rows.sort(function(a, b) {
    var zoneDiff = (b.riskZoneRank || 0) - (a.riskZoneRank || 0);
    if (zoneDiff !== 0) return zoneDiff;
    var liftDiff = (b.avgLift || 0) - (a.avgLift || 0);
    if (liftDiff !== 0) return liftDiff;
    return (b.members || 0) - (a.members || 0);
  });

  var summary = {
    zipCount: rows.length,
    totalMembers: 0,
    avgLift: 0,
    avgRisk: 0,
    avgLiftDenom: 0,
    avgRiskDenom: 0,
    highLiftZipCount: 0,
    zoneZipCounts: {},
    zoneMemberCounts: {},
    zoneLeaders: {},
    topLiftZip: null,
    bottomLiftZip: null
  };

  rows.forEach(function(z) {
    summary.totalMembers += z.members;
    if (z.avgLift !== null && z.avgLift !== undefined && !isNaN(z.avgLift)) {
      summary.avgLift += z.avgLift;
      summary.avgLiftDenom += 1;
    }
    if (z.avgRiskFull !== null && z.avgRiskFull !== undefined && !isNaN(z.avgRiskFull)) {
      summary.avgRisk += z.avgRiskFull;
      summary.avgRiskDenom += 1;
    }
    if ((z.avgLift || 0) > 0.2) summary.highLiftZipCount += 1;
    var zone = z.riskZone || "Unknown risk";
    if (!summary.zoneZipCounts[zone]) summary.zoneZipCounts[zone] = 0;
    summary.zoneZipCounts[zone] += 1;
    if (!summary.zoneMemberCounts[zone]) summary.zoneMemberCounts[zone] = 0;
    summary.zoneMemberCounts[zone] += z.members;
    var leader = summary.zoneLeaders[zone];
    if (!leader || (z.avgLift || -Infinity) > (leader.avgLift || -Infinity)) {
      summary.zoneLeaders[zone] = z;
    }
    if (!summary.topLiftZip || (z.avgLift || -Infinity) > (summary.topLiftZip.avgLift || -Infinity)) {
      summary.topLiftZip = z;
    }
    if (!summary.bottomLiftZip || (z.avgLift || Infinity) < (summary.bottomLiftZip.avgLift || Infinity)) {
      summary.bottomLiftZip = z;
    }
  });

  summary.avgLift = summary.avgLiftDenom ? summary.avgLift / summary.avgLiftDenom : null;
  summary.avgRisk = summary.avgRiskDenom ? summary.avgRisk / summary.avgRiskDenom : null;

  return {
    rows: rows,
    summary: summary
  };
}

function aggregateByContract(filtered) {
  var map = {};
  filtered.forEach(function(d) {
    var key = d.contract || "(blank)";
    if (!map[key]) {
      map[key] = {
        contract: key,
        members: 0,
        sumRiskFull: 0,
        sumLift: 0,
        highCount: 0
      };
    }
    var g = map[key];
    g.members += 1;
    if (d.risk_full !== null) g.sumRiskFull += d.risk_full;
    if (d.sdoh_lift !== null) g.sumLift += d.sdoh_lift;
    if (isHighBurden(d.sdoh_lift_level)) g.highCount += 1;
  });

  var rows = Object.keys(map).map(function(k) {
    var g = map[k];
    return {
      contract: g.contract,
      members: g.members,
      avgRisk: g.members ? g.sumRiskFull / g.members : null,
      avgLift: g.members ? g.sumLift / g.members : null,
      pctHigh: g.members ? g.highCount / g.members : 0
    };
  });

  rows.sort(function(a, b) {
    return (b.avgLift || 0) - (a.avgLift || 0);
  });

  return rows;
}

// ==========================
//  EXPORT HELPERS
// ==========================
function exportMemberCohortCsv() {
  var rows = state.lastMemberFiltered || [];
  if (!rows.length) {
    alert("No members to export for the current filters.");
    return;
  }
  var columns = [
    { label: "Member ID", value: function(row) { return row.member; } },
    { label: "Member Name", value: function(row) { return row.member_name; } },
    { label: "Age", value: function(row) { return row.age; } },
    { label: "Gender", value: function(row) { return row.gender; } },
    { label: "Race", value: function(row) { return row.race; } },
    { label: "Plan", value: function(row) { return row.plan; } },
    { label: "Contract", value: function(row) { return row.contract; } },
    { label: "County", value: function(row) { return row.county; } },
    { label: "State", value: function(row) { return row.state; } },
    { label: "ZIP", value: function(row) { return row.zip; } },
    { label: "Risk With SDOH", value: function(row) { return fmtNumber(row.risk_full, 3); } },
    { label: "Risk No SDOH", value: function(row) { return fmtNumber(row.risk_no_sdoh, 3); } },
    { label: "SDOH Lift", value: function(row) { return fmtNumber(row.sdoh_lift, 3); } },
    { label: "SDOH Level", value: function(row) { return row.sdoh_lift_level; } },
    { label: "Preferred Intervention", value: function(row) { return describeIntervention(row); } }
  ];
  downloadCsv("member_cohort.csv", rows, columns);
}

function exportContractSummaryCsv() {
  var rows = aggregateByContract(state.lastMemberFiltered || []);
  if (!rows.length) {
    alert("No contracts to export for the current filters.");
    return;
  }
  var columns = [
    { label: "Contract", value: function(row) { return row.contract; } },
    { label: "Members", value: function(row) { return row.members; } },
    { label: "Avg Risk", value: function(row) { return fmtNumber(row.avgRisk, 3); } },
    { label: "Avg Lift", value: function(row) { return fmtNumber(row.avgLift, 3); } },
    { label: "% High/Extreme", value: function(row) { return fmtPercent(row.pctHigh, 1); } }
  ];
  downloadCsv("contract_summary.csv", rows, columns);
}

function exportSelectedMemberDetailCsv() {
  var member = getMemberByIdx(state.selectedMemberIdx);
  if (!member) {
    alert("Select a member before exporting details.");
    return;
  }
  function driverSummary(prefix) {
    var summary = [];
    for (var i = 1; i <= 5; i++) {
      var name = member[prefix + i];
      var value = member[prefix + i + "_value"];
      if (!name) continue;
      summary.push(name + " (" + fmtNumber(value, 4) + ")");
    }
    return summary.join(" | ");
  }
  var exportRow = {
    member: member.member,
    name: member.member_name,
    age: member.age,
    gender: member.gender,
    race: member.race,
    plan: member.plan,
    contract: member.contract,
    county: member.county,
    state: member.state,
    zip: member.zip,
    risk_full: fmtNumber(member.risk_full, 3),
    risk_no_sdoh: fmtNumber(member.risk_no_sdoh, 3),
    sdoh_lift: fmtNumber(member.sdoh_lift, 3),
    sdoh_level: member.sdoh_lift_level,
    sdoh_drivers: driverSummary("sdoh_driver_"),
    nonsdoh_drivers: driverSummary("nonsdoh_driver_")
  };
  var columns = [
    { key: "member", label: "Member ID" },
    { key: "name", label: "Member Name" },
    { key: "age", label: "Age" },
    { key: "gender", label: "Gender" },
    { key: "race", label: "Race" },
    { key: "plan", label: "Plan" },
    { key: "county", label: "County" },
    { key: "state", label: "State" },
    { key: "zip", label: "ZIP" },
    { key: "risk_full", label: "Risk With SDOH" },
    { key: "risk_no_sdoh", label: "Risk No SDOH" },
    { key: "sdoh_lift", label: "SDOH Lift" },
    { key: "sdoh_level", label: "SDOH Level" },
    { key: "sdoh_drivers", label: "Top SDOH Drivers" },
    { key: "nonsdoh_drivers", label: "Top Non-SDOH Drivers" }
  ];
  downloadCsv("selected_member_detail_" + member.member + ".csv", [exportRow], columns);
}

function buildInterventionExportRow(member) {
  if (!member) return null;
  var defaultKey = String(member.sdoh_driver_1 || "").toLowerCase();
  var overrideKey = state.memberOverrides[member.member];
  var preferredPlan = getInterventionPlanByKey(defaultKey);
  var overridePlan = overrideKey ? getInterventionPlanByKey(overrideKey) : null;
  var navigatorId = state.careNavigatorAssignments[member.member];
  var navigator = getNavigatorById(navigatorId);
  var outreachDate = state.outreachSchedules[member.member] || "";

  return {
    member: member.member,
    name: member.member_name,
    primary_driver: member.sdoh_driver_1 || "",
    recommended_plan: preferredPlan ? preferredPlan.title : "",
    recommended_summary: preferredPlan ? preferredPlan.summary : "",
    recommended_actions: preferredPlan && preferredPlan.actions ? preferredPlan.actions.join(" | ") : "",
    override_plan_key: overrideKey || "",
    override_plan_title: overridePlan ? overridePlan.title : "",
    override_actions: overridePlan && overridePlan.actions ? overridePlan.actions.join(" | ") : "",
    navigator_id: navigator ? navigator.id : "",
    navigator_name: navigator ? navigator.name : "",
    navigator_specialty: navigator ? navigator.specialty : "",
    outreach_date_iso: outreachDate,
    outreach_date_readable: outreachDate ? formatDateHuman(outreachDate) : ""
  };
}

function exportPreferredInterventionCsv(member) {
  if (!member) {
    alert("Select a member before exporting intervention details.");
    return;
  }
  var record = buildInterventionExportRow(member);
  if (!record) return;
  var columns = [
    { key: "member", label: "Member ID" },
    { key: "name", label: "Member Name" },
    { key: "primary_driver", label: "Primary SDOH Driver" },
    { key: "recommended_plan", label: "Recommended Plan" },
    { key: "recommended_summary", label: "Recommended Summary" },
    { key: "recommended_actions", label: "Recommended Actions" },
    { key: "override_plan_key", label: "Override Plan Key" },
    { key: "override_plan_title", label: "Override Plan" },
    { key: "override_actions", label: "Override Actions" },
    { key: "navigator_id", label: "Navigator ID" },
    { key: "navigator_name", label: "Navigator Name" },
    { key: "navigator_specialty", label: "Navigator Specialty" },
    { key: "outreach_date_iso", label: "Outreach Date (ISO)" },
    { key: "outreach_date_readable", label: "Outreach Date" }
  ];
  downloadCsv("preferred_intervention_" + member.member + ".csv", [record], columns);
}

function exportZipSummaryCsv() {
  var rows = state.lastZipRows || [];
  if (!rows.length) {
    alert("No ZIP rows to export for the current filters.");
    return;
  }
  var columns = [
    { label: "ZIP", value: function(row) { return row.zip; } },
    { label: "County", value: function(row) { return row.county; } },
    { label: "State", value: function(row) { return row.state; } },
    { label: "Members", value: function(row) { return row.members; } },
    { label: "Risk Zone", value: function(row) { return row.riskZone; } },
    { label: "Avg Risk With SDOH", value: function(row) { return fmtNumber(row.avgRiskFull, 3); } },
    { label: "Avg SDOH Lift", value: function(row) { return fmtNumber(row.avgLift, 3); } },
    { label: "% High/Extreme", value: function(row) { return fmtPercent(row.pctHigh, 1); } }
  ];
  downloadCsv("zip_summary.csv", rows, columns);
}

function renderZipKpis(zipAgg, summary) {
  var container = document.getElementById("zip-kpi-grid");
  container.innerHTML = "";

  if (!zipAgg.length) {
    container.innerHTML = "<div class='kpi-card'><div class='kpi-label'>No ZIPs</div><div class='kpi-main'>0</div><div class='kpi-sub'>Adjust filters to see ZIP level view.</div></div>";
    return;
  }

  summary = summary || {};
  var n = summary.zipCount || zipAgg.length;
  var totalMembers = summary.totalMembers || 0;
  var avgLift = summary.avgLift;
  var avgRiskFull = summary.avgRisk;
  var zoneCounts = summary.zoneZipCounts || {};
  var zoneMembers = summary.zoneMemberCounts || {};
  var zoneLeaders = summary.zoneLeaders || {};

  function leaderLabel(zone) {
    var l = zoneLeaders[zone];
    if (!l) return "";
    var liftTxt = fmtSignedNumber(l.avgLift, 3);
    return l.zip + " • Lift " + liftTxt;
  }

  var cards = [
    {
      label: "ZIPs analyzed",
      main: String(n),
      sub: "After filters",
      pill: "Members: " + String(totalMembers),
      pillClass: ""
    },
    {
      label: "High risk ZIPs",
      main: String(zoneCounts["High risk"] || 0),
      sub: "Avg risk ≥ 2.05",
      pill: leaderLabel("High risk"),
      pillClass: "bad"
    },
    {
      label: "Moderate risk ZIPs",
      main: String(zoneCounts["Moderate risk"] || 0),
      sub: "Avg risk 1.95 - 2.04",
      pill: leaderLabel("Moderate risk"),
      pillClass: ""
    },
    {
      label: "Lower risk ZIPs",
      main: String(zoneCounts["Lower risk"] || 0),
      sub: "Avg risk < 1.95",
      pill: leaderLabel("Lower risk"),
      pillClass: "good"
    },
    {
      label: "Mean ZIP risk",
      main: fmtNumber(avgRiskFull, 3),
      sub: "Average of ZIP avg risk_full",
      pill: "High-lift ZIPs: " + String(summary.highLiftZipCount || 0),
      pillClass: ""
    },
    {
      label: "Mean ZIP lift",
      main: fmtNumber(avgLift, 3),
      sub: "Avg risk amplification",
      pill: summary.topLiftZip ? ("Top lift: " + summary.topLiftZip.zip) : "",
      pillClass: avgLift > 0.2 ? "bad" : (avgLift < 0 ? "good" : "")
    }
  ];

  cards.forEach(function(c) {
    var card = document.createElement("div");
    card.className = "kpi-card";
    var inner =
      "<div class='kpi-label'>" + c.label + "</div>" +
      "<div class='kpi-main'>" + c.main + "</div>" +
      "<div class='kpi-sub'>" + c.sub + "</div>";
    if (c.pill) {
      inner += "<div class='kpi-pill " + (c.pillClass || "") + "'>" + c.pill + "</div>";
    }
    card.innerHTML = inner;
    container.appendChild(card);
  });

  var tag = document.getElementById("zip-cohort-tag");
  tag.textContent = "ZIPs: " + String(n) + " | Members: " + String(totalMembers);

  var sec = document.getElementById("zip-secondary-kpis");
  sec.innerHTML = "";
  var total = totalMembers || 1;
  ["High risk", "Moderate risk", "Lower risk"].forEach(function(zone) {
    var members = zoneMembers[zone] || 0;
    var share = members && totalMembers ? members / totalMembers : 0;
    var leader = zoneLeaders[zone];
    var riskCls = riskBandClass(zone);
    var div = document.createElement("div");
    div.className = "zip-kpi";
    div.innerHTML =
      "<span><span class='legend-dot " + riskCls + "'></span>" + zone + " members</span>" +
      "<strong>" + String(members) + "</strong>" +
      "<span>" + fmtPercent(share, 1) + " of filtered cohort</span>" +
      "<span class='zip-kpi-hint'>" + (leader ? ("Focus ZIP: " + leader.zip + " • Lift " + fmtSignedNumber(leader.avgLift, 3)) : "No ZIPs in band") + "</span>";
    sec.appendChild(div);
  });
}

function renderZipTable(zipAgg) {
  var tbody = document.querySelector("#zip-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  var rows = zipAgg.slice();
  if (!state.filters.risk_level || String(state.filters.risk_level).toLowerCase() === "all risk bands") {
    shuffleArray(rows);
  }
  var coll = document.getElementById("zip-collapsible");
  if (coll && rows.length) {
    coll.classList.remove("closed");
  }
  rows.forEach(function(z) {
    var tr = document.createElement("tr");
    var zone = z.riskZone || "Unknown risk";
    var zoneCls = riskBandClass(zone);
    tr.innerHTML =
      "<td>" + z.zip + "</td>" +
      "<td>" + z.county + "</td>" +
      "<td>" + z.state + "</td>" +
      "<td>" + z.members + "</td>" +
      "<td><span class='risk-chip " + zoneCls + "'>" + zone + "</span></td>" +
      "<td>" + fmtNumber(z.avgRiskFull, 3) + "</td>" +
      "<td>" + fmtNumber(z.avgLift, 3) + "</td>" +
      "<td>" + fmtPercent(z.pctHigh, 1) + "</td>" +
      "<td><button class='btn-ghost btn-small zip-driver-btn' type='button'>Top SDOH</button></td>";

    tr.addEventListener("click", function() {
      document.getElementById("filter-zip").value = z.zip;
      state.filters.zip = z.zip;
      state.page = "member-page";
      renderAll();
    });

    var driverBtn = tr.querySelector(".zip-driver-btn");
    if (driverBtn) {
      driverBtn.addEventListener("click", function(evt) {
        evt.stopPropagation();
        openZipModal(z.zip);
      });
    }

    tbody.appendChild(tr);
  });
}

function renderZipMap(zipAgg) {
  var grid = document.getElementById("zip-grid");
  if (!grid) return;
  grid.innerHTML = "";
  destroyZipGridCharts();
  if (!zipAgg.length) return;

  var maxMembers = 0;
  zipAgg.forEach(function(z) {
    if (z.members > maxMembers) maxMembers = z.members;
  });
  if (maxMembers === 0) maxMembers = 1;

  zipAgg.forEach(function(z) {
    var zone = z.riskZone || "Unknown risk";
    var zoneCls = riskBandClass(zone);
    var riskLabel = (zone || "Unknown").replace(" risk", "");
    var liftText = fmtSignedNumber(z.avgLift, 3);
    var members = z.members || 0;
    var sizeScale = 0.6 + 0.4 * (members / maxMembers);
    var radarSize = Math.round(90 * sizeScale);

    var card = document.createElement("div");
    card.className = "zip-card " + zoneCls;

    var tooltip = document.createElement("div");
    tooltip.className = "zip-card-tooltip";
    tooltip.innerHTML =
      "<strong>ZIP " + z.zip + "</strong><br/>" +
      (z.county ? (z.county + ", " + z.state) : "County unavailable") +
      "<br/>Avg risk " + fmtNumber(z.avgRiskFull, 2) +
      " • Lift " + liftText +
      "<br/>Members " + members;
    card.appendChild(tooltip);

    var header = document.createElement("div");
    header.className = "zip-card-header";
    header.innerHTML =
      "<div class='zip-card-zip'>" + z.zip + "</div>" +
      "<div class='zip-card-risk'>" + riskLabel + " • " + fmtNumber(z.avgRiskFull, 2) + "</div>";
    card.appendChild(header);

    var radarWrap = document.createElement("div");
    radarWrap.className = "zip-card-radar";
    radarWrap.style.width = radarSize + "px";
    radarWrap.style.height = radarSize + "px";
    var canvas = document.createElement("canvas");
    radarWrap.appendChild(canvas);
    card.appendChild(radarWrap);

    var metrics = document.createElement("div");
    metrics.className = "zip-card-metrics";
    metrics.innerHTML =
      "<div>Lift <strong>" + liftText + "</strong></div>" +
      "<div><strong>" + members + "</strong> members</div>";
    card.appendChild(metrics);

    var actions = document.createElement("div");
    actions.className = "zip-card-actions";
    var topBtn = document.createElement("button");
    topBtn.type = "button";
    topBtn.className = "btn-ghost btn-small";
    topBtn.textContent = "Top SDOH";
    topBtn.addEventListener("click", function(evt) {
      evt.stopPropagation();
      openZipModal(z.zip);
    });
    actions.appendChild(topBtn);
    card.appendChild(actions);

    card.addEventListener("click", function() {
      document.getElementById("filter-zip").value = z.zip;
      state.filters.zip = z.zip;
      state.page = "member-page";
      renderAll();
    });

    var zipMembers = (state.lastMemberFiltered || []).filter(function(m) {
      return String(m.zip) === String(z.zip);
    });
    var topDrivers = collectTopDrivers(zipMembers, "sdoh");
    var labels = topDrivers.map(function(d) { return d.name; });
    var values = topDrivers.map(function(d) { return d.sumAbs; });
    var chartColor = riskBandColor(zone);
    var chart = createZipGridRadar(canvas, labels, values, chartColor);
    if (chart) zipGridCharts.push(chart);

    grid.appendChild(card);
  });
}

function loadZipGeoData() {
  if (zipGeoData) return Promise.resolve(zipGeoData);
  if (typeof window !== "undefined" && window.ZIP_GEOJSON) {
    zipGeoData = window.ZIP_GEOJSON;
    return Promise.resolve(zipGeoData);
  }
  return fetch("./data/zip_boundaries.geojson")
    .then(function(resp) { return resp.json(); })
    .then(function(data) {
      zipGeoData = data;
      return data;
    })
    .catch(function(err) {
      console.error("Failed to load ZIP GeoJSON", err);
      return null;
    });
}

function initZipLeafletMap() {
  if (zipLeafletMap) return zipLeafletMap;
  var container = document.getElementById("zip-map-leaflet");
  if (!container || typeof L === "undefined") return null;
  zipLeafletMap = L.map(container, { zoomControl: true, scrollWheelZoom: false, zoomSnap: 0.25 })
    .setView([41.2, -72.6], 7.6);
  zipMapBaseLayer = L.tileLayer("https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png", {
    maxZoom: 12,
    minZoom: 5,
    attribution: "&copy; OpenStreetMap contributors &copy; CARTO"
  }).addTo(zipLeafletMap);
  zipMapDarkLayer = L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 12,
    minZoom: 5,
    attribution: "&copy; OpenStreetMap contributors &copy; CARTO"
  });
  return zipLeafletMap;
}

function renderZipMapView(zipAgg) {
  var map = initZipLeafletMap();
  if (!map) return;
  renderZipMapSidePanel(zipAgg);
  loadZipGeoData().then(function(geo) {
    if (!geo) return;
    if (zipGeoLayer) {
      map.removeLayer(zipGeoLayer);
      zipGeoLayer = null;
    }
    if (zipClusterLayer) {
      map.removeLayer(zipClusterLayer);
      zipClusterLayer = null;
    }

    var stats = {};
    zipAgg.forEach(function(z) {
      stats[String(z.zip)] = z;
    });

    function styleFeature(feature) {
      var zip = feature.properties && feature.properties.ZCTA5CE10;
      var info = stats[zip];
      var zone = info ? info.riskZone : "Unknown risk";
      var border = riskBandColor(zone);
      var fill = info ? sdohColorForLift(info.avgLift) : "#E5E7EB";
      return {
        color: border,
        weight: info ? 2.5 : 1,
        fillColor: fill,
        fillOpacity: info ? 0.3 + (0.5 * state.mapIntensity) : 0.12
      };
    }

    function onEachFeature(feature, layer) {
      var zip = feature.properties && feature.properties.ZCTA5CE10;
      var info = stats[zip];
      var title = "ZIP " + (zip || "-");
      var county = info ? (info.county + ", " + info.state) : "No data in filters";
      var avgRisk = info ? fmtNumber(info.avgRiskFull, 2) : "-";
      var lift = info ? fmtSignedNumber(info.avgLift, 3) : "-";
      var members = info ? info.members : 0;
      if (layer.closePopup) layer.closePopup();
      layer.on("mouseover", function() {
        layer.setStyle({ weight: 3, fillOpacity: 0.85 });
      });
      layer.on("mouseout", function() {
        zipGeoLayer.resetStyle(layer);
      });
      layer.on("click", function() {
        if (!zip) return;
        state.selectedZip = zip;
        renderZipMapSidePanel(zipAgg);
      });
    }

    zipGeoLayer = L.geoJSON(geo, {
      style: styleFeature,
      pointToLayer: function(feature, latlng) {
        var zip = feature.properties && feature.properties.ZCTA5CE10;
        var info = stats[zip];
        var zone = info ? info.riskZone : "Unknown risk";
        var border = riskBandColor(zone);
        var fill = info ? sdohColorForLift(info.avgLift) : "#E5E7EB";
        return L.circleMarker(latlng, {
          radius: 8,
          color: border,
          weight: 2,
          fillColor: fill,
          fillOpacity: 0.75
        });
      },
      onEachFeature: onEachFeature
    }).addTo(map);

    if (typeof L.markerClusterGroup === "function") {
      zipClusterLayer = L.markerClusterGroup({
        showCoverageOnHover: false,
        spiderfyOnMaxZoom: true,
        disableClusteringAtZoom: 11,
        maxClusterRadius: 44
      });

      geo.features.forEach(function(feature) {
        var zip = feature.properties && feature.properties.ZCTA5CE10;
        var info = stats[zip];
        if (!info) return;
        var center = null;
        if (feature.geometry && feature.geometry.type === "Point") {
          center = [feature.geometry.coordinates[1], feature.geometry.coordinates[0]];
        } else if (feature.geometry && feature.geometry.type && feature.geometry.coordinates) {
          var bounds = L.geoJSON(feature).getBounds();
          center = bounds.getCenter();
        }
        if (!center) return;
        var zone = info.riskZone || "Unknown risk";
        var border = riskBandColor(zone);
        var marker = L.marker(center, {
          icon: L.divIcon({
            className: "zip-cluster-marker",
            html: "<span style=\"background:" + border + "\"></span>",
            iconSize: [12, 12],
            iconAnchor: [6, 6]
          })
        });
        marker.on("click", function() {
          state.selectedZip = String(zip || "");
          renderZipMapSidePanel(zipAgg);
        });
        zipClusterLayer.addLayer(marker);
      });
      zipClusterLayer.addTo(map);
    }

    zipGeoLayer.eachLayer(function(layer) {
      if (!layer || !layer.feature) return;
      var zip = layer.feature.properties && layer.feature.properties.ZCTA5CE10;
      var info = stats[zip];
      if (!info || !layer._path) return;
      var key = (info.riskZone || "").toLowerCase();
      if (key === "high risk") layer._path.classList.add("zip-glow-high");
      if (key === "moderate risk") layer._path.classList.add("zip-glow-med");
      if (key === "lower risk") layer._path.classList.add("zip-glow-low");
    });

    if (zipMapPulseLayer) {
      map.removeLayer(zipMapPulseLayer);
      zipMapPulseLayer = null;
    }
    zipMapPulseLayer = L.layerGroup().addTo(map);
    var topRisk = zipAgg.slice().sort(function(a, b) {
      return (b.avgRiskFull || 0) - (a.avgRiskFull || 0);
    }).slice(0, 3);
    topRisk.forEach(function(z) {
      var feature = geo.features.find(function(f) {
        return f.properties && f.properties.ZCTA5CE10 === String(z.zip);
      });
      if (!feature) return;
      var center = null;
      if (feature.geometry && feature.geometry.type === "Point") {
        center = [feature.geometry.coordinates[1], feature.geometry.coordinates[0]];
      } else if (feature.geometry && feature.geometry.type && feature.geometry.coordinates) {
        var bounds = L.geoJSON(feature).getBounds();
        center = bounds.getCenter();
      }
      if (!center) return;
      L.circleMarker(center, {
        radius: 18,
        color: riskBandColor(z.riskZone),
        weight: 2,
        fillColor: sdohColorForLift(z.avgLift),
        fillOpacity: 0.2,
        className: "zip-heat-pulse"
      }).addTo(zipMapPulseLayer);
    });

    if (zipGeoLayer) {
      zipGeoLayer.closePopup();
    }

    try {
      map.fitBounds(zipGeoLayer.getBounds(), { padding: [20, 20] });
    } catch (err) {
      // Ignore fit errors for empty bounds.
    }
  });
}

function destroyZipPanelCharts() {
  if (zipPanelSdohChart) {
    zipPanelSdohChart.destroy();
    zipPanelSdohChart = null;
  }
  if (zipPanelNonSdohChart) {
    zipPanelNonSdohChart.destroy();
    zipPanelNonSdohChart = null;
  }
}

function renderZipMapSidePanel(zipAgg) {
  var panel = document.getElementById("zip-map-side-panel");
  if (!panel) return;
  destroyZipPanelCharts();

  var members = state.lastMemberFiltered || [];
  var selectedZip = state.selectedZip || "";
  var isSelected = Boolean(selectedZip);
  var summary = null;
  var targetMembers = members;
  var title = "Overall ZIP Drivers";
  var subtitle = "All members in current filters.";

  if (isSelected) {
    targetMembers = members.filter(function(m) { return String(m.zip) === String(selectedZip); });
    summary = (zipAgg || []).find(function(row) { return String(row.zip) === String(selectedZip); });
    title = "ZIP " + selectedZip + " Driver Profile";
    subtitle = summary ? (summary.county + ", " + summary.state) : "Selected ZIP";
  }

  var topSdoh = collectTopDrivers(targetMembers, "sdoh");
  var topNon = collectTopDrivers(targetMembers, "nonsdoh");
  var avgRisk = summary ? fmtNumber(summary.avgRiskFull, 2) : fmtNumber(averageOf(targetMembers, "risk_full"), 2);
  var avgLift = summary ? fmtSignedNumber(summary.avgLift, 3) : fmtSignedNumber(averageOf(targetMembers, "sdoh_lift"), 3);
  var memberCount = summary ? summary.members : targetMembers.length;

  panel.innerHTML =
    "<div class='zip-map-side-title'>" + title + "</div>" +
    "<div class='zip-map-side-sub'>" + subtitle + "</div>" +
    "<div class='zip-driver-meta'>" +
      "<span><strong>Members</strong> " + String(memberCount || 0) + "</span>" +
      "<span><strong>Avg risk</strong> " + (avgRisk || "-") + "</span>" +
      "<span><strong>Avg lift</strong> " + (avgLift || "-") + "</span>" +
    "</div>" +
    "<div class='zip-modal-radar-grid'>" +
      "<div class='zip-modal-radar-card'>" +
        "<div class='zip-modal-radar-label'>SDOH drivers</div>" +
        "<div class='zip-modal-radar-canvas'><canvas id='zipPanelSdohRadar'></canvas></div>" +
      "</div>" +
      "<div class='zip-modal-radar-card'>" +
        "<div class='zip-modal-radar-label'>Non-SDOH drivers</div>" +
        "<div class='zip-modal-radar-canvas'><canvas id='zipPanelNonSdohRadar'></canvas></div>" +
      "</div>" +
    "</div>" +
    "<div class='zip-driver-section'>" +
      "<div class='zip-driver-table'>" +
        "<div class='zip-driver-table-title'>Top SDOH drivers</div>" +
        buildZipDriverTableRows(topSdoh) +
      "</div>" +
      "<div class='zip-driver-table'>" +
        "<div class='zip-driver-table-title'>Top non-SDOH drivers</div>" +
        buildZipDriverTableRows(topNon) +
      "</div>" +
    "</div>" +
    "<div class='zip-map-side-actions'>" +
      (isSelected ? "<button class='btn-ghost btn-small' id='btn-zip-panel-print'>Print PDF</button>" : "") +
    "</div>";

  zipPanelSdohChart = createZipModalRadar(
    document.getElementById("zipPanelSdohRadar"),
    topSdoh.map(function(d) { return d.name; }),
    topSdoh.map(function(d) { return d.sumAbs; }),
    "#FF6B4A",
    "rgba(255, 107, 74, 0.18)"
  );
  zipPanelNonSdohChart = createZipModalRadar(
    document.getElementById("zipPanelNonSdohRadar"),
    topNon.map(function(d) { return d.name; }),
    topNon.map(function(d) { return d.sumAbs; }),
    "#2563EB",
    "rgba(37, 99, 235, 0.18)"
  );

  var printBtn = document.getElementById("btn-zip-panel-print");
  if (printBtn) {
    printBtn.addEventListener("click", function() {
      if (isSelected && selectedZip) {
        openZipModal(selectedZip);
        prepareZipModalPrint();
      }
    });
  }
}

function renderContractTable(rows) {
  var tbody = document.querySelector("#contract-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  rows.forEach(function(row) {
    var tr = document.createElement("tr");
    tr.innerHTML =
      "<td>" + row.contract + "</td>" +
      "<td>" + row.members + "</td>" +
      "<td>" + fmtNumber(row.avgRisk, 2) + "</td>" +
      "<td>" + fmtSignedNumber(row.avgLift, 3) + "</td>" +
      "<td>" + fmtPercent(row.pctHigh, 1) + "</td>";
    tr.addEventListener("click", function() {
      state.selectedContract = row.contract;
      renderContractSidePanel(rows);
    });
    tbody.appendChild(tr);
  });
}

function renderContractSidePanel(rows) {
  var panel = document.getElementById("contract-side-panel");
  if (!panel) return;
  destroyZipPanelCharts();

  var members = state.lastMemberFiltered || [];
  var selected = state.selectedContract || "";
  var isSelected = Boolean(selected);
  var targetMembers = members;
  var title = "Overall Contract Drivers";
  var subtitle = "All members in current filters.";

  if (isSelected) {
    targetMembers = members.filter(function(m) { return (m.contract || "(blank)") === selected; });
    title = "Contract " + selected + " Drivers";
    subtitle = "Selected contract cohort.";
  }

  var topSdoh = collectTopDrivers(targetMembers, "sdoh");
  var topNon = collectTopDrivers(targetMembers, "nonsdoh");
  var avgRisk = fmtNumber(averageOf(targetMembers, "risk_full"), 2);
  var avgLift = fmtSignedNumber(averageOf(targetMembers, "sdoh_lift"), 3);
  var memberCount = targetMembers.length;

  panel.innerHTML =
    "<div class='zip-map-side-title'>" + title + "</div>" +
    "<div class='zip-map-side-sub'>" + subtitle + "</div>" +
    "<div class='zip-driver-meta'>" +
      "<span><strong>Members</strong> " + String(memberCount || 0) + "</span>" +
      "<span><strong>Avg risk</strong> " + (avgRisk || "-") + "</span>" +
      "<span><strong>Avg lift</strong> " + (avgLift || "-") + "</span>" +
    "</div>" +
    "<div class='zip-modal-radar-grid'>" +
      "<div class='zip-modal-radar-card'>" +
        "<div class='zip-modal-radar-label'>SDOH drivers</div>" +
        "<div class='zip-modal-radar-canvas'><canvas id='contractSdohRadar'></canvas></div>" +
      "</div>" +
      "<div class='zip-modal-radar-card'>" +
        "<div class='zip-modal-radar-label'>Non-SDOH drivers</div>" +
        "<div class='zip-modal-radar-canvas'><canvas id='contractNonSdohRadar'></canvas></div>" +
      "</div>" +
    "</div>" +
    "<div class='zip-driver-section'>" +
      "<div class='zip-driver-table'>" +
        "<div class='zip-driver-table-title'>Top SDOH drivers</div>" +
        buildZipDriverTableRows(topSdoh) +
      "</div>" +
      "<div class='zip-driver-table'>" +
        "<div class='zip-driver-table-title'>Top non-SDOH drivers</div>" +
        buildZipDriverTableRows(topNon) +
      "</div>" +
    "</div>";

  zipPanelSdohChart = createZipModalRadar(
    document.getElementById("contractSdohRadar"),
    topSdoh.map(function(d) { return d.name; }),
    topSdoh.map(function(d) { return d.sumAbs; }),
    "#FF6B4A",
    "rgba(255, 107, 74, 0.18)"
  );
  zipPanelNonSdohChart = createZipModalRadar(
    document.getElementById("contractNonSdohRadar"),
    topNon.map(function(d) { return d.name; }),
    topNon.map(function(d) { return d.sumAbs; }),
    "#2563EB",
    "rgba(37, 99, 235, 0.18)"
  );

  var tag = document.getElementById("contract-cohort-tag");
  if (tag) {
    tag.textContent = "Contracts: " + String(rows.length);
  }
}

function buildCampaignRows(members, campaign) {
  var rows = [];
  if (!campaign) return rows;
  (members || []).forEach(function(m) {
    var eligible = isCampaignEligible(campaign, m);
    var enrolled = isCampaignEnrolled(campaign, m);
    if (!eligible && !enrolled) return;
    var record = getCampaignEnrollment(campaign.id, m.member) || {};
    var riskBand = riskBandFromValue(m.risk_full) || "Unknown risk";
    var riskClass = "Unknown";
    var riskKey = String(riskBand).toLowerCase();
    if (riskKey.indexOf("high") !== -1) riskClass = "High";
    else if (riskKey.indexOf("moderate") !== -1) riskClass = "Medium";
    else if (riskKey.indexOf("lower") !== -1) riskClass = "Low";
    var outreachPriority = riskClass;
    var interventionLabel = "Medication Adherence Counselling";
    rows.push({
      member: m.member,
      member_name: m.member_name,
      zip: m.zip,
      risk_full: m.risk_full,
      risk_class: riskClass,
      outreach_priority: outreachPriority,
      sdoh_lift: m.sdoh_lift,
      sdoh_lift_level: m.sdoh_lift_level,
      eligible: eligible,
      enrolled: enrolled,
      override: record.override || "",
      outreachMethod: record.outreachMethod || "",
      channel: deriveMemberChannel(m, record.outreachMethod),
      intervention: interventionLabel,
      preferred_intervention: describeIntervention(m)
    });
  });
  return rows;
}

function renderCampaignView(members) {
  var campaign = getCampaignById(state.selectedCampaignId) || state.campaigns[0];
  if (!campaign) return;
  state.selectedCampaignId = campaign.id;

  var select = document.getElementById("campaign-select");
  if (select) {
    select.innerHTML = "";
    state.campaigns.forEach(function(c) {
      var opt = document.createElement("option");
      opt.value = c.id;
      opt.textContent = c.name;
      select.appendChild(opt);
    });
    select.value = campaign.id;
  }

  var summary = document.getElementById("campaign-summary");
  var rows = buildCampaignRows(members, campaign);
  var eligibleCount = rows.filter(function(r){ return r.eligible; }).length;
  var enrolledCount = rows.filter(function(r){ return r.enrolled; }).length;
  var manualCount = rows.filter(function(r){ return r.override === "include"; }).length;
  var excludedCount = rows.filter(function(r){ return r.override === "exclude"; }).length;
  var logicText = "Logic: HbA1c adherence < 0.8 AND risk with SDOH ≥ 2.0 AND SDOH lift > 0.";
  if (Array.isArray(campaign.rules) && campaign.rules.length) {
    var labelMap = {};
    CAMPAIGN_RULE_CATEGORIES.forEach(function(cat) {
      (cat.fields || []).forEach(function(f) { labelMap[f.key] = f.label; });
    });
    logicText = "Logic: " + campaign.rules.map(function(rule) {
      var label = labelMap[rule.field] || rule.field;
      return label + " " + rule.op + " " + rule.value;
    }).join(" AND ");
  }
  if (summary) {
    summary.innerHTML =
      "<div class='campaign-summary-title'>" +
        "<strong>" + campaign.name + "</strong>" +
        "<button type='button' class='campaign-info-btn' id='campaign-logic-info' aria-label='Show campaign logic'>i</button>" +
        "<span class='campaign-logic-tooltip' id='campaign-logic-tooltip' role='tooltip'>" + logicText + "</span>" +
        "<button type='button' class='btn-ghost btn-small' id='campaign-edit-rules'>Edit rules</button>" +
      "</div>" +
      "<span>" + (campaign.description || "Manual outreach campaign.") + "</span>" +
      "<div class='campaign-kpis'>" +
        "<div class='campaign-kpi'><span>Eligible</span><strong>" + eligibleCount + "</strong></div>" +
        "<div class='campaign-kpi'><span>Enrolled</span><strong>" + enrolledCount + "</strong></div>" +
        "<div class='campaign-kpi'><span>Manual Adds</span><strong>" + manualCount + "</strong></div>" +
        "<div class='campaign-kpi'><span>Excluded</span><strong>" + excludedCount + "</strong></div>" +
      "</div>";
  }

  var infoBtn = document.getElementById("campaign-logic-info");
  var infoTip = document.getElementById("campaign-logic-tooltip");
  if (infoBtn && infoTip) {
    infoBtn.addEventListener("click", function(evt) {
      evt.stopPropagation();
      infoTip.classList.toggle("is-open");
    });
  }
  var editRulesBtn = document.getElementById("campaign-edit-rules");
  if (editRulesBtn) {
    editRulesBtn.addEventListener("click", function() {
      var panel = document.getElementById("campaign-designer");
      if (panel) {
        panel.scrollIntoView({ behavior: "smooth", block: "start" });
        panel.classList.add("highlight");
        setTimeout(function() { panel.classList.remove("highlight"); }, 800);
      }
    });
  }
  var deleteBtn = document.getElementById("btn-delete-campaign");
  if (deleteBtn) {
    var protectedIds = (state.campaigns || []).slice(0, 5).map(function(c){ return c.id; });
    var isProtected = protectedIds.indexOf(campaign.id) !== -1;
    deleteBtn.disabled = isProtected;
    deleteBtn.title = isProtected ? "Top 5 campaigns cannot be deleted." : "";
  }
  var channelInfoBtn = document.getElementById("campaign-channel-info");
  var channelInfoTip = document.getElementById("campaign-channel-tooltip");
  if (channelInfoBtn && channelInfoTip) {
    channelInfoBtn.addEventListener("click", function(evt) {
      evt.stopPropagation();
      channelInfoTip.classList.toggle("is-open");
    });
  }
  if (typeof window !== "undefined" && !window.__campaignLogicListener) {
    window.__campaignLogicListener = true;
    document.addEventListener("click", function() {
      var tip = document.getElementById("campaign-logic-tooltip");
      if (tip) tip.classList.remove("is-open");
      var channelTip = document.getElementById("campaign-channel-tooltip");
      if (channelTip) channelTip.classList.remove("is-open");
    });
  }

  renderCampaignDesigner(campaign);

  var tbody = document.querySelector("#campaign-table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  rows.forEach(function(r) {
    if (r.override === "exclude") return;
    var tr = document.createElement("tr");
    var eligibleLabel = r.eligible ? "Yes" : "No";
    var riskBand = riskBandFromValue(r.risk_full) || "Unknown risk";
    var riskClass = "Unknown";
    var riskKey = String(riskBand).toLowerCase();
    if (riskKey.indexOf("high") !== -1) riskClass = "High";
    else if (riskKey.indexOf("moderate") !== -1) riskClass = "Medium";
    else if (riskKey.indexOf("lower") !== -1) riskClass = "Low";
    var outreachPriority = riskClass;
    var interventionLabel = "Medication Adherence Counselling";
    var manualDot = r.override === "include" ? "<span class='manual-dot' title='Manual add'></span>" : "";
    tr.innerHTML =
      "<td>" + manualDot + r.member + "</td>" +
      "<td>" + r.member_name + "</td>" +
      "<td>" + r.zip + "</td>" +
      "<td>" + riskClass + "</td>" +
      "<td>" + fmtSignedNumber(r.sdoh_lift, 3) + "</td>" +
      "<td>" + eligibleLabel + "</td>" +
      "<td>" + outreachPriority + "</td>" +
      "<td>" + interventionLabel + "</td>" +
      "<td class='campaign-outreach-cell'></td>";

    var outreachCell = tr.querySelector(".campaign-outreach-cell");
    var outreachText = document.createElement("div");
    outreachText.className = "campaign-outreach-text";
    outreachText.textContent = r.channel || "Email";
    outreachCell.appendChild(outreachText);

    tbody.appendChild(tr);
  });

  if (tbody.__campaignExcludeBound) {
    tbody.__campaignExcludeBound = false;
  }
}

function renderCampaignDesigner(campaign) {
  var container = document.getElementById("campaign-designer");
  if (!container || !campaign) return;
  var stats = getCampaignFieldStats();
  var rulesByCategory = {};
  (campaign.rules || []).forEach(function(rule) {
    if (!rule || !rule.category) return;
    if (!rulesByCategory[rule.category]) rulesByCategory[rule.category] = [];
    rulesByCategory[rule.category].push(rule);
  });

  container.innerHTML = "";
  var header = document.createElement("div");
  header.className = "campaign-designer-header";
  header.innerHTML =
    "<div class='campaign-designer-title'>Campaign Designer</div>" +
    "<div class='campaign-designer-actions'>" +
      "<button type='button' class='btn-ghost btn-small' id='campaign-rules-clear'>Clear rules</button>" +
      "<button type='button' class='btn-primary btn-small' id='campaign-rules-save'>Save rules</button>" +
    "</div>";
  container.appendChild(header);

  CAMPAIGN_RULE_CATEGORIES.forEach(function(category) {
    var block = document.createElement("div");
    block.className = "campaign-category";
    var head = document.createElement("div");
    head.className = "campaign-category-head";
    head.innerHTML =
      "<div class='campaign-category-title'>" + category.label + "</div>" +
      "<button type='button' class='btn-ghost btn-small campaign-add-rule' data-category='" + category.id + "'>Add field</button>";
    block.appendChild(head);

    var list = document.createElement("div");
    list.className = "campaign-rule-list";
    list.setAttribute("data-category", category.id);

    var existing = rulesByCategory[category.id] || [];
    var defaultFields = category.fields.slice(0, 3);
    if (!existing.length) {
      defaultFields.forEach(function(field) {
        list.appendChild(buildRuleRow(category, { field: field.key, op: ">=", value: "" }, stats));
      });
    } else {
      existing.forEach(function(rule) {
        list.appendChild(buildRuleRow(category, rule, stats));
      });
    }

    block.appendChild(list);
    container.appendChild(block);
  });

  function buildRuleRow(category, rule, statsMap) {
    var row = document.createElement("div");
    row.className = "campaign-rule-row";
    row.setAttribute("data-category", category.id);

    var fieldSelect = document.createElement("select");
    category.fields.forEach(function(field) {
      var opt = document.createElement("option");
      opt.value = field.key;
      opt.textContent = field.label;
      fieldSelect.appendChild(opt);
    });
    fieldSelect.value = rule.field || category.fields[0].key;

    var opSelect = document.createElement("select");
    CAMPAIGN_OPERATORS.forEach(function(op) {
      var opt = document.createElement("option");
      opt.value = op.value;
      opt.textContent = op.label;
      opSelect.appendChild(opt);
    });
    opSelect.value = rule.op || ">=";

    var input = document.createElement("input");
    input.type = "number";
    input.step = "any";
    input.value = rule.value !== undefined && rule.value !== null ? rule.value : "";

    var range = document.createElement("div");
    range.className = "campaign-rule-range";
    row.appendChild(fieldSelect);
    row.appendChild(opSelect);
    row.appendChild(input);
    row.appendChild(range);

    function updateRange() {
      var key = fieldSelect.value;
      var stat = statsMap[key] || {};
      var min = stat.min;
      var max = stat.max;
      if (min !== null && min !== undefined) input.min = min;
      if (max !== null && max !== undefined) input.max = max;
      range.textContent = (min !== null && max !== null) ? ("Range " + fmtNumber(min, 2) + " – " + fmtNumber(max, 2)) : "Range not available";
    }
    updateRange();
    fieldSelect.addEventListener("change", updateRange);
    return row;
  }

  container.querySelectorAll(".campaign-add-rule").forEach(function(btn) {
    btn.addEventListener("click", function() {
      var categoryId = btn.getAttribute("data-category");
      var category = CAMPAIGN_RULE_CATEGORIES.find(function(c) { return c.id === categoryId; });
      if (!category) return;
      var list = container.querySelector(".campaign-rule-list[data-category='" + categoryId + "']");
      if (!list) return;
      list.appendChild(buildRuleRow(category, { field: category.fields[0].key, op: ">=", value: "" }, stats));
    });
  });

  var saveBtn = document.getElementById("campaign-rules-save");
  if (saveBtn) {
    saveBtn.addEventListener("click", function() {
      var newRules = [];
      container.querySelectorAll(".campaign-rule-row").forEach(function(row) {
        var categoryId = row.getAttribute("data-category");
        var selects = row.querySelectorAll("select");
        var input = row.querySelector("input");
        if (!selects.length || !input) return;
        var field = selects[0].value;
        var op = selects[1] ? selects[1].value : ">=";
        var value = input.value;
        if (value === "" || value === null || value === undefined) return;
        newRules.push({ category: categoryId, field: field, op: op, value: value });
      });
      campaign.rules = newRules;
      if (campaign.id !== "diabetes-med-adherence") {
        campaign.autoEnroll = newRules.length > 0;
      }
      saveCampaignState();
      renderCampaignView(state.lastMemberFiltered || DATA);
    });
  }

  var clearBtn = document.getElementById("campaign-rules-clear");
  if (clearBtn) {
    clearBtn.addEventListener("click", function() {
      campaign.rules = [];
      if (campaign.id !== "diabetes-med-adherence") {
        campaign.autoEnroll = false;
      }
      saveCampaignState();
      renderCampaignDesigner(campaign);
      renderCampaignView(state.lastMemberFiltered || DATA);
    });
  }
}

function buildZipDriverTableRows(drivers) {
  if (!drivers || !drivers.length) {
    return "<div class='zip-driver-note'>No driver data available.</div>";
  }
  var html =
    "<ul class='zip-driver-list'>" +
      "<li class='zip-driver-item header'>" +
        "<span>Driver</span><span>Net</span><span>Positive</span><span>Negative</span>" +
      "</li>";
  drivers.forEach(function(driver) {
    html +=
      "<li class='zip-driver-item'>" +
        "<span class='zip-driver-name'>" + prettifyDriverName(driver.name) + "</span>" +
        "<span class='zip-driver-value'>" + fmtSignedNumber(driver.sum, 4) + "</span>" +
        "<span class='zip-driver-value'>" + fmtNumber(driver.posAbs, 4) + "</span>" +
        "<span class='zip-driver-value'>" + fmtNumber(driver.negAbs, 4) + "</span>" +
      "</li>";
  });
  html += "</ul>";
  return html;
}

function averageOf(list, key) {
  if (!list || !list.length) return null;
  var sum = 0;
  var count = 0;
  list.forEach(function(item) {
    var val = item[key];
    if (val === null || val === undefined || isNaN(val)) return;
    sum += Number(val);
    count += 1;
  });
  if (!count) return null;
  return sum / count;
}

function initCollapsibles() {
  var coll = document.getElementById("zip-collapsible");
  if (!coll) return;

  var header = coll.querySelector(".collapsible-header");
  if (header) {
    header.addEventListener("click", function() {
      coll.classList.toggle("closed");
    });
  }

  var toggleBtn = document.getElementById("btn-collapse-all");
  if (toggleBtn) {
    toggleBtn.addEventListener("click", function() {
      coll.classList.toggle("closed");
    });
  }
}

function initExportButtons() {
  var memberBtn = document.getElementById("btn-export-member-table");
  if (memberBtn) {
    memberBtn.addEventListener("click", exportMemberCohortCsv);
  }
  var memberCopy = document.getElementById("btn-copy-member-table");
  if (memberCopy) {
    memberCopy.addEventListener("click", function() {
      copyTableToClipboard("member-table", memberCopy);
    });
  }
  var detailBtn = document.getElementById("btn-export-member-detail");
  if (detailBtn) {
    detailBtn.addEventListener("click", exportSelectedMemberDetailCsv);
  }
  var zipBtn = document.getElementById("btn-export-zip-table");
  if (zipBtn) {
    zipBtn.addEventListener("click", exportZipSummaryCsv);
  }
  var zipCopy = document.getElementById("btn-copy-zip-table");
  if (zipCopy) {
    zipCopy.addEventListener("click", function() {
      copyTableToClipboard("zip-table", zipCopy);
    });
  }
  var contractBtn = document.getElementById("btn-export-contract");
  if (contractBtn) {
    contractBtn.addEventListener("click", exportContractSummaryCsv);
  }
  var contractCopy = document.getElementById("btn-copy-contract-table");
  if (contractCopy) {
    contractCopy.addEventListener("click", function() {
      copyTableToClipboard("contract-table", contractCopy);
    });
  }
  var campaignCopy = document.getElementById("btn-copy-campaign-table");
  if (campaignCopy) {
    campaignCopy.addEventListener("click", function() {
      copyTableToClipboard("campaign-table", campaignCopy);
    });
  }

  var campaignOutreachBtn = document.getElementById("btn-trigger-campaign-outreach");
  if (campaignOutreachBtn) {
    campaignOutreachBtn.addEventListener("click", function() {
      var campaign = getCampaignById(state.selectedCampaignId);
      var name = campaign ? campaign.name : "campaign";
      alert("Outreach triggered for " + name + ".");
    });
  }
}

// ==========================
//  MEMBER MODAL & PRINT
// ==========================
function initMemberModal() {
  var trigger = document.getElementById("btn-print-member-detail");
  var modal = document.getElementById("member-modal");
  var closeBtn = document.getElementById("member-modal-close");
  var backdrop = modal ? modal.querySelector(".member-modal-backdrop") : null;
  var modalPrintBtn = document.getElementById("member-modal-print");

  if (trigger) {
    trigger.addEventListener("click", function() {
      var member = getSelectedMember();
      if (!member) {
        alert("Select a member to open the printable detail view.");
        return;
      }
      openMemberModal(member);
    });
  }

  if (closeBtn) {
    closeBtn.addEventListener("click", closeMemberModal);
  }

  if (backdrop) {
    backdrop.addEventListener("click", closeMemberModal);
  }

  if (modalPrintBtn) {
    modalPrintBtn.addEventListener("click", function() {
      if (!state.isMemberModalOpen) {
        var member = getSelectedMember();
        if (!member) {
          alert("Select a member before printing.");
          return;
        }
        openMemberModal(member);
      }
      prepareModalPrint();
    });
  }

  document.addEventListener("keydown", function(evt) {
    if (evt.key === "Escape" && state.isMemberModalOpen) {
      closeMemberModal();
    }
  });
}

function openMemberModal(member) {
  if (!member) return;
  var modal = document.getElementById("member-modal");
  if (!modal) return;
  populateMemberModal(member);
  modal.classList.add("open");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("modal-open");
  state.isMemberModalOpen = true;
}

function closeMemberModal() {
  var modal = document.getElementById("member-modal");
  if (!modal) return;
  modal.classList.remove("open");
  modal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("modal-open");
  document.body.classList.remove("print-modal");
  state.isMemberModalOpen = false;
}

// ==========================
//  ZIP MODAL & PRINT
// ==========================
function initZipModal() {
  var modal = document.getElementById("zip-modal");
  if (!modal) return;
  var closeBtn = document.getElementById("zip-modal-close");
  var backdrop = modal.querySelector(".member-modal-backdrop");
  var printBtn = document.getElementById("zip-modal-print");

  if (closeBtn) {
    closeBtn.addEventListener("click", closeZipModal);
  }
  if (backdrop) {
    backdrop.addEventListener("click", closeZipModal);
  }
  if (printBtn) {
    printBtn.addEventListener("click", function() {
      if (!modal.classList.contains("open")) {
        if (!state.selectedZip) {
          alert("Select a ZIP before printing.");
          return;
        }
        openZipModal(state.selectedZip);
      }
      prepareZipModalPrint();
    });
  }

  document.addEventListener("keydown", function(evt) {
    if (evt.key === "Escape" && modal.classList.contains("open")) {
      closeZipModal();
    }
  });
}

function openZipModal(zip) {
  if (!zip) return;
  if (state.isMemberModalOpen) {
    closeMemberModal();
  }
  var modal = document.getElementById("zip-modal");
  if (!modal) return;
  state.selectedZip = zip;
  populateZipModal(zip);
  modal.classList.add("open");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("modal-open");
}

function closeZipModal() {
  var modal = document.getElementById("zip-modal");
  if (!modal) return;
  modal.classList.remove("open");
  modal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("modal-open");
  document.body.classList.remove("print-zip-modal");
  destroyZipModalCharts();
}

function prepareZipModalPrint() {
  document.body.classList.add("print-zip-modal");
  setTimeout(function() {
    window.print();
  }, 30);
  setTimeout(function() {
    document.body.classList.remove("print-zip-modal");
  }, 1200);
}

function destroyZipModalCharts() {
  if (zipModalSdohChart) {
    zipModalSdohChart.destroy();
    zipModalSdohChart = null;
  }
  if (zipModalNonSdohChart) {
    zipModalNonSdohChart.destroy();
    zipModalNonSdohChart = null;
  }
}

function createZipModalRadar(canvas, labels, values, color, fillColor) {
  if (!canvas || !labels.length) return null;
  var ctx = canvas.getContext("2d");
  return new Chart(ctx, {
    type: "radar",
    data: {
      labels: labels.map(prettifyDriverName),
      datasets: [{
        data: normalizeAbs(values),
        borderColor: color,
        backgroundColor: fillColor,
        pointBackgroundColor: color,
        pointBorderColor: color,
        borderWidth: 1.6,
        pointRadius: 2.2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        r: {
          suggestedMin: 0,
          suggestedMax: 1,
          ticks: { display: false },
          grid: { color: "rgba(148, 163, 184, 0.3)" },
          angleLines: { color: "rgba(148, 163, 184, 0.4)" },
          pointLabels: {
            color: "#374151",
            font: { size: 10 }
          }
        }
      },
      plugins: {
        legend: { display: false }
      }
    }
  });
}

function populateZipModal(zip) {
  var container = document.getElementById("zip-modal-body");
  if (!container) return;
  destroyZipModalCharts();
  if (!zip) {
    container.innerHTML = "<p style='font-size:12px; color:var(--text-muted);'>Select a ZIP to view its driver profile.</p>";
    return;
  }

  var members = (state.lastMemberFiltered || []).filter(function(m) {
    return String(m.zip) === String(zip);
  });
  var summary = (state.lastZipRows || []).find(function(row) {
    return String(row.zip) === String(zip);
  });
  var riskZone = summary ? (summary.riskZone || "Unknown risk") : "Unknown risk";
  var avgRisk = summary ? fmtNumber(summary.avgRiskFull, 3) : "-";
  var avgLift = summary ? fmtSignedNumber(summary.avgLift, 3) : "-";
  var memberCount = summary ? summary.members : members.length;
  var topDrivers = collectTopDrivers(members, "sdoh");
  var topNonDrivers = collectTopDrivers(members, "nonsdoh");
  var riskCls = summary ? riskBandClass(summary.riskZone || "Unknown risk") : "risk-unknown";

  var heroHtml =
    "<div class='zip-modal-hero'>" +
      "<div>" +
        "<div class='zip-modal-title'>ZIP " + zip + "</div>" +
        "<div class='zip-modal-sub'>" + (summary ? (summary.county + ", " + summary.state) : "Geography unavailable") + "</div>" +
      "</div>" +
      "<div class='zip-modal-tags'>" +
        "<span class='modal-pill " + riskCls + "'>" + riskZone + "</span>" +
        "<span class='modal-pill'>Lift " + avgLift + "</span>" +
      "</div>" +
    "</div>";

  var metaHtml =
    "<div class='zip-driver-meta'>" +
      "<span><strong>Members</strong> " + String(memberCount) + "</span>" +
      "<span><strong>Avg risk</strong> " + avgRisk + "</span>" +
      "<span><strong>Avg SDOH lift</strong> " + avgLift + "</span>" +
    "</div>";

  var radarHtml =
    "<div class='zip-modal-radar-grid'>" +
      "<div class='zip-modal-radar-card'>" +
        "<div class='zip-modal-radar-label'>SDOH driver profile</div>" +
        "<div class='zip-modal-radar-canvas'><canvas id='zipModalSdohRadar'></canvas></div>" +
      "</div>" +
      "<div class='zip-modal-radar-card'>" +
        "<div class='zip-modal-radar-label'>Non-SDOH driver profile</div>" +
        "<div class='zip-modal-radar-canvas'><canvas id='zipModalNonSdohRadar'></canvas></div>" +
      "</div>" +
    "</div>";

  var bodyHtml =
    "<div class='zip-modal-body'>" +
      "<div class='zip-modal-meta-card'>" +
        "<div class='zip-modal-meta-title'>ZIP overview</div>" +
        metaHtml +
      "</div>" +
      radarHtml +
    "</div>";

  container.innerHTML = heroHtml + bodyHtml;

  zipModalSdohChart = createZipModalRadar(
    document.getElementById("zipModalSdohRadar"),
    topDrivers.map(function(d) { return d.name; }),
    topDrivers.map(function(d) { return d.sumAbs; }),
    "#FF6B4A",
    "rgba(255, 107, 74, 0.18)"
  );
  zipModalNonSdohChart = createZipModalRadar(
    document.getElementById("zipModalNonSdohRadar"),
    topNonDrivers.map(function(d) { return d.name; }),
    topNonDrivers.map(function(d) { return d.sumAbs; }),
    "#2563EB",
    "rgba(37, 99, 235, 0.18)"
  );
}

function buildModalDriverList(drivers) {
  if (!drivers || !drivers.length) {
    return "<li class='driver-empty'><span>No drivers captured</span><span>--</span></li>";
  }
  var maxAbs = 0;
  drivers.forEach(function(item) {
    if (item.value !== null && item.value !== undefined) {
      var abs = Math.abs(item.value);
      if (abs > maxAbs) maxAbs = abs;
    }
  });
  if (maxAbs === 0) {
    maxAbs = 1;
  }
  return drivers.map(function(item) {
    var value = item.value !== null && item.value !== undefined ? item.value : 0;
    var pct = Math.min(Math.abs(value) / maxAbs * 100, 100);
    var signClass = value >= 0 ? "positive" : "negative";
    return "<li>" +
      "<span class='driver-name'>" + prettifyDriverName(item.name) + "</span>" +
      "<div class='driver-bar'><span class='driver-bar-fill " + signClass + "' style='width:" + pct.toFixed(1) + "%;'></span></div>" +
      "<span class='driver-value'>" + fmtSignedNumber(value, 4) + "</span>" +
    "</li>";
  }).join("");
}

function buildModalNarrative(member, plan, sdohDrivers) {
  var parts = [];
  var riskLabel = riskBandFromValue(member.risk_full) || "Risk pending";
  parts.push(member.member_name + " (" + member.member + ") is classified as " + riskLabel.toLowerCase() + " with predicted risk " + fmtNumber(member.risk_full, 3) + ".");
  if (member.contract) {
    parts.push("Member is aligned to contract " + member.contract + (member.plan ? " (" + member.plan + ")" : "") + ".");
  }
  if (member.sdoh_lift !== null && member.sdoh_lift !== undefined) {
    var liftDescriptor = member.sdoh_lift > 0 ? "environmental factors amplifying medication risk" : "protective community supports lowering risk";
    parts.push("SDOH lift of " + fmtSignedNumber(member.sdoh_lift, 3) + " indicates " + liftDescriptor + ".");
  }
  var driverNames = (sdohDrivers || []).slice(0, 2).map(function(driver) {
    return prettifyDriverName(driver.name);
  });
  if (driverNames.length) {
    parts.push("Key drivers: " + driverNames.join(" and ") + ".");
  }
  if (plan) {
    parts.push("Recommended plan: " + plan.title + " — " + plan.summary);
  }
  var navigatorId = state.careNavigatorAssignments[member.member];
  var navigator = getNavigatorById(navigatorId);
  if (navigator) {
    parts.push(navigator.name + " (" + navigator.specialty + ") will coordinate execution.");
  }
  var outreachDate = state.outreachSchedules[member.member];
  if (outreachDate) {
    parts.push("Next outreach scheduled for " + formatDateHuman(outreachDate) + ".");
  }
  return parts.join(" ");
}

function populateMemberModal(member) {
  var container = document.getElementById("member-modal-body");
  if (!container) return;
  destroyModalRadarCharts();
  if (!member) {
    container.innerHTML = "<p style='font-size:12px; color:var(--text-muted);'>Select a member to review the printable spotlight.</p>";
    return;
  }

  var planMeta = resolveActivePlan(member);
  var plan = planMeta.plan;
  var riskLabel = riskBandFromValue(member.risk_full) || "Risk pending";
  var liftText = fmtSignedNumber(member.sdoh_lift, 3);
  var sdohDrivers = extractDrivers(member, "sdoh_driver_");
  var nonDrivers = extractDrivers(member, "nonsdoh_driver_");
  var navigatorId = state.careNavigatorAssignments[member.member];
  var navigator = getNavigatorById(navigatorId);
  var outreachDate = state.outreachSchedules[member.member];
  var navigatorLine = navigator ? navigator.name + " • " + navigator.specialty : "No navigator assigned";
  var outreachLine = outreachDate ? "Next touchpoint " + formatDateHuman(outreachDate) : "No outreach scheduled";
  var liftBadge = sdohBadgeClass(member.sdoh_lift_level);
  var liftPillClass = "modal-pill";
  if (member.sdoh_lift !== null && member.sdoh_lift > 0) {
    liftPillClass += " positive";
  }
  var planTitle = plan ? plan.title : "Preferred intervention";
  var planSummary = plan ? plan.summary : "No recommendation available.";
  var planActions = plan && plan.actions ? plan.actions : [];
  var planBadge = planMeta.overrideActive ? "<span class='modal-pill override'>Override applied</span>" : "";
  var sdohLevelLabel = member.sdoh_lift_level || "SDOH level pending";
  var contractLabel = member.contract ? " • Contract " + member.contract : "";

  var heroHtml =
    "<div class='member-modal-hero'>" +
      "<div>" +
        "<div class='member-modal-name'>" + member.member_name + "</div>" +
        "<div class='member-modal-meta'>" + member.member + " • " + (member.plan || "Plan N/A") + contractLabel + " • " + (member.segment || "Segment") + "</div>" +
        "<div class='member-modal-meta'>" + (member.age !== null && member.age !== undefined ? member.age + " yrs" : "Age N/A") + " • " + (member.gender || member.sex || "Gender N/A") + " • " + (member.race || "Race N/A") + " • " + (member.county || "") + (member.state ? ", " + member.state : "") + "</div>" +
      "</div>" +
      "<div class='member-modal-tags'>" +
        "<span class='modal-pill'>" + riskLabel + "</span>" +
        "<span class='" + liftPillClass + "'>Lift " + liftText + "</span>" +
        "<span class='badge-level" + (liftBadge ? " " + liftBadge : "") + "'>" + sdohLevelLabel + "</span>" +
      "</div>" +
    "</div>";

  var metricsHtml =
    "<div class='modal-section modal-metrics-grid'>" +
      "<div class='modal-metric'>" +
        "<span class='modal-metric-label'>Predicted (with SDOH)</span>" +
        "<span class='modal-metric-value'>" + fmtNumber(member.risk_full, 3) + "</span>" +
        "<span class='modal-metric-sub'>Model with SDOH features</span>" +
      "</div>" +
      "<div class='modal-metric'>" +
        "<span class='modal-metric-label'>Predicted (no SDOH)</span>" +
        "<span class='modal-metric-value'>" + fmtNumber(member.risk_no_sdoh, 3) + "</span>" +
        "<span class='modal-metric-sub'>Model without SDOH</span>" +
      "</div>" +
      "<div class='modal-metric'>" +
        "<span class='modal-metric-label'>SDOH lift</span>" +
        "<span class='modal-metric-value'>" + liftText + "</span>" +
        "<span class='modal-metric-sub'>risk_with_sdoh - risk_no_sdoh</span>" +
      "</div>" +
    "</div>";

  var planActionsHtml = planActions.length
    ? planActions.map(function(action) { return "<li>" + action + "</li>"; }).join("")
    : "<li>No action steps defined.</li>";

  var planHtml =
    "<div class='modal-section modal-plan'>" +
      "<div class='modal-plan-header'>" +
        "<div>" +
          "<div class='modal-section-title'>Preferred intervention</div>" +
          "<div class='modal-plan-title'>" + planTitle + "</div>" +
          planBadge +
          "<p>" + planSummary + "</p>" +
        "</div>" +
        "<div class='modal-plan-meta'>" +
          "<div><span>Navigator</span><strong>" + navigatorLine + "</strong></div>" +
          "<div><span>Outreach</span><strong>" + outreachLine + "</strong></div>" +
        "</div>" +
      "</div>" +
      "<ul class='modal-plan-actions'>" + planActionsHtml + "</ul>" +
    "</div>";

  var radarHtml =
    "<div class='modal-section modal-radar'>" +
      "<div class='modal-radar-head'>" +
        "<div class='modal-section-title'>Driver Profile (Magnitude Radar)</div>" +
        "<button type='button' class='btn-ghost btn-small modal-radar-toggle'>Show magnitude radar</button>" +
      "</div>" +
      "<div class='modal-radar-body is-collapsed'>" +
        "<div class='modal-radar-grid'>" +
          "<div class='modal-radar-card'>" +
            "<div class='modal-radar-label'>SDOH impact magnitude (normalized)</div>" +
            "<div class='modal-radar-canvas'><canvas id='modalSdohRadar'></canvas></div>" +
          "</div>" +
          "<div class='modal-radar-card'>" +
            "<div class='modal-radar-label'>Non-SDOH impact magnitude (normalized)</div>" +
            "<div class='modal-radar-canvas'><canvas id='modalNonSdohRadar'></canvas></div>" +
          "</div>" +
        "</div>" +
        "<div class='modal-radar-caption'>Shape shows relative driver strength (0-1). Direction (+/-) remains in the bar chart list.</div>" +
      "</div>" +
    "</div>";

  var narrativeHtml =
    "<div class='modal-section modal-narrative'>" +
      "<div class='modal-section-title'>Action narrative</div>" +
      "<p>" + buildModalNarrative(member, plan, sdohDrivers) + "</p>" +
    "</div>";

  container.innerHTML = heroHtml + metricsHtml + planHtml + radarHtml + narrativeHtml;

  var radarToggle = container.querySelector(".modal-radar-toggle");
  var radarBody = container.querySelector(".modal-radar-body");
  if (radarToggle && radarBody) {
    radarToggle.addEventListener("click", function() {
      var isCollapsed = radarBody.classList.toggle("is-collapsed");
      radarToggle.textContent = isCollapsed ? "Show magnitude radar" : "Hide magnitude radar";
      if (!isCollapsed) {
        renderModalRadarCharts(member);
      } else {
        destroyModalRadarCharts();
      }
    });
  }
}

function prepareModalPrint() {
  document.body.classList.add("print-modal");
  setTimeout(function() {
    window.print();
  }, 30);
  setTimeout(function() {
    document.body.classList.remove("print-modal");
  }, 1200);
}

// ==========================
//  PAGE NAVIGATION & PRINT
// ==========================
function initCampaignView() {
  var select = document.getElementById("campaign-select");
  if (select) {
    select.addEventListener("change", function() {
      state.selectedCampaignId = select.value;
      saveCampaignState();
      renderAll();
    });
  }

  var createBtn = document.getElementById("btn-create-campaign");
  var nameInput = document.getElementById("campaign-create-name");
  var deleteBtn = document.getElementById("btn-delete-campaign");
  if (createBtn && nameInput) {
    createBtn.addEventListener("click", function() {
      var name = String(nameInput.value || "").trim();
      if (!name) return;
      var id = slugify(name);
      if (!id) return;
      if (state.campaigns.some(function(c){ return c.id === id; })) {
        alert("Campaign already exists.");
        return;
      }
      state.campaigns.push({
        id: id,
        name: name,
        description: "Manual outreach campaign.",
        autoEnroll: false,
        outreachMethods: ["Mail", "Phone/SMS", "Email"],
        rules: []
      });
      state.selectedCampaignId = id;
      nameInput.value = "";
      saveCampaignState();
      renderAll();
    });
  }
  if (deleteBtn) {
    deleteBtn.addEventListener("click", function() {
      var active = getCampaignById(state.selectedCampaignId) || state.campaigns[0];
      if (!active) return;
      var protectedIds = (state.campaigns || []).slice(0, 5).map(function(c){ return c.id; });
      if (protectedIds.indexOf(active.id) !== -1) {
        alert("Top 5 campaigns cannot be deleted.");
        return;
      }
      var nextCampaigns = state.campaigns.filter(function(c) { return c.id !== active.id; });
      state.campaigns = nextCampaigns.length ? nextCampaigns : state.campaigns;
      if (state.selectedCampaignId === active.id) {
        state.selectedCampaignId = state.campaigns[0] ? state.campaigns[0].id : "";
      }
      delete state.campaignEnrollments[active.id];
      saveCampaignState();
      renderAll();
    });
  }

  var exportBtn = document.getElementById("btn-export-campaign");
  if (exportBtn) {
    exportBtn.addEventListener("click", function() {
      var campaign = getCampaignById(state.selectedCampaignId) || state.campaigns[0];
      if (!campaign) return;
      var rows = buildCampaignRows(state.lastMemberFiltered || DATA, campaign).filter(function(r){ return r.enrolled; });
      var columns = [
        { key: "member", label: "Member ID" },
        { key: "member_name", label: "Member Name" },
        { key: "zip", label: "ZIP" },
        { key: "risk_class", label: "Risk Level" },
        { key: "outreach_priority", label: "Outreach Priority" },
        { key: "sdoh_lift", label: "SDOH Lift" },
        { key: "sdoh_lift_level", label: "SDOH Level" },
        { key: "intervention", label: "Intervention" },
        { key: "channel", label: "Channel" }
      ];
      downloadCsv("campaign_" + campaign.id + ".csv", rows, columns);
    });
  }
}

function initNav() {
  var tabs = document.querySelectorAll(".nav-tab");
  tabs.forEach(function(tab) {
    tab.addEventListener("click", function() {
      var page = tab.getAttribute("data-page");
      state.page = page;
      tabs.forEach(function(t){ t.classList.remove("active"); });
      tab.classList.add("active");
      renderAll();
    });
  });

  document.getElementById("btn-print").addEventListener("click", function() {
    setFooter("Preparing print view...");
    setTimeout(function() {
      window.print();
      setFooter("Ready.");
    }, 150);
  });

  var contractPrint = document.getElementById("btn-print-contract");
  if (contractPrint) {
    contractPrint.addEventListener("click", function() {
      state.page = "contract-page";
      renderAll();
      setTimeout(function() {
        window.print();
      }, 150);
    });
  }
}

function updateNavTabsForPage(page) {
  var tabs = document.querySelectorAll(".nav-tab");
  tabs.forEach(function(t){ t.classList.remove("active"); });
  var targetPage = page === "zip-map-page" ? "zip-page" : page;
  var active = document.querySelector(".nav-tab[data-page='" + targetPage + "']");
  if (active) active.classList.add("active");
}

function initZipMapToggle() {
  var btn = document.getElementById("btn-zip-map-view");
  if (!btn) return;
  btn.addEventListener("click", function() {
    state.page = "zip-map-page";
    renderAll();
  });
}

function initZipMapControls() {
  var toggle = document.getElementById("btn-map-theme");
  if (toggle) {
    toggle.addEventListener("click", function() {
      state.mapFuturistic = !state.mapFuturistic;
      applyZipMapTheme();
      renderZipMapView(state.lastZipRows || []);
    });
  }
  var slider = document.getElementById("map-intensity");
  if (slider) {
    slider.addEventListener("input", function() {
      state.mapIntensity = Number(slider.value) / 100;
      renderZipMapView(state.lastZipRows || []);
    });
  }
  var riskSelect = document.getElementById("map-risk-level");
  var baseSelect = document.getElementById("filter-risk-level");
  if (riskSelect && baseSelect && !riskSelect.options.length) {
    riskSelect.innerHTML = baseSelect.innerHTML;
    riskSelect.value = baseSelect.value || "";
    riskSelect.addEventListener("change", function() {
      state.filters.risk_level = riskSelect.value;
      baseSelect.value = riskSelect.value;
      renderAll();
    });
  }
}

function applyZipMapTheme() {
  var page = document.getElementById("zip-map-page");
  if (!page) return;
  page.classList.toggle("futuristic", state.mapFuturistic);
  var toggle = document.getElementById("btn-map-theme");
  if (toggle) {
    toggle.textContent = state.mapFuturistic ? "Day Mode" : "Dark Mode";
  }
  if (zipLeafletMap && zipMapBaseLayer && zipMapDarkLayer) {
    if (state.mapFuturistic) {
      if (zipLeafletMap.hasLayer(zipMapBaseLayer)) zipLeafletMap.removeLayer(zipMapBaseLayer);
      if (!zipLeafletMap.hasLayer(zipMapDarkLayer)) zipLeafletMap.addLayer(zipMapDarkLayer);
    } else {
      if (zipLeafletMap.hasLayer(zipMapDarkLayer)) zipLeafletMap.removeLayer(zipMapDarkLayer);
      if (!zipLeafletMap.hasLayer(zipMapBaseLayer)) zipLeafletMap.addLayer(zipMapBaseLayer);
    }
  }
}

// ==========================
//  RENDER ENTRY POINT
// ==========================
function renderAll() {
  var pageMember = document.getElementById("member-page");
  var pageZip = document.getElementById("zip-page");
  var pageZipMap = document.getElementById("zip-map-page");
  var pageCampaign = document.getElementById("campaign-page");
  var pageContract = document.getElementById("contract-page");
  pageMember.classList.toggle("active", state.page === "member-page");
  pageZip.classList.toggle("active", state.page === "zip-page");
  if (pageZipMap) pageZipMap.classList.toggle("active", state.page === "zip-map-page");
  if (pageCampaign) pageCampaign.classList.toggle("active", state.page === "campaign-page");
  if (pageContract) pageContract.classList.toggle("active", state.page === "contract-page");
  updateNavTabsForPage(state.page);

  var baseFiltered = applyFilters();
  if (state.focusHighActive) {
    baseFiltered = baseFiltered.filter(function(d){ return isHighBurden(d.sdoh_lift_level); });
  }
  var filtered = baseFiltered;
  if (state.sdohDistributionFilter) {
    filtered = baseFiltered.filter(function(d) {
      return sdohBadgeClass(d.sdoh_lift_level) === state.sdohDistributionFilter;
    });
  }
  state.lastDistributionBase = baseFiltered.slice();
  state.lastMemberFiltered = filtered.slice();
  var zipAggResult = aggregateByZip(filtered);
  state.lastZipRows = zipAggResult.rows.slice();
  var zipRowsFiltered = zipAggResult.rows.slice();
  if (state.filters.risk_level) {
    zipRowsFiltered = zipRowsFiltered.filter(function(z) {
      return String(z.riskZone || "") === String(state.filters.risk_level);
    });
  }

  if (state.page === "member-page") {
    renderMemberKpis(filtered);
    renderDistributionBars(filtered, baseFiltered);
    renderMemberTable(filtered);
  } else if (state.page === "zip-page") {
    renderZipKpis(zipRowsFiltered, zipAggResult.summary);
    renderZipTable(zipRowsFiltered);
    renderZipMap(zipRowsFiltered);
  } else if (state.page === "zip-map-page") {
    applyZipMapTheme();
    renderZipMapView(zipRowsFiltered);
  } else if (state.page === "campaign-page") {
    renderCampaignView(filtered);
  } else if (state.page === "contract-page") {
    var contractRows = aggregateByContract(filtered);
    renderContractTable(contractRows);
    renderContractSidePanel(contractRows);
  }

  var pageLabel = "ZIP";
  if (state.page === "member-page") pageLabel = "Member";
  if (state.page === "zip-map-page") pageLabel = "ZIP Map";
  if (state.page === "campaign-page") pageLabel = "Campaign";
  if (state.page === "contract-page") pageLabel = "Contract";
  var footerMsg = "Members in filter: " + filtered.length + " | Page: " + pageLabel;
  if (state.focusHighActive) {
    footerMsg += " | Focus: High SDOH";
  }
  setFooter(footerMsg);
}

// ==========================
//  INITIALIZE
// ==========================
async function bootstrapDashboard() {
  try {
    setFooter("Loading data...");
    await loadConfig();
    loadCampaignState();
    const rawRows = await loadDashboardData();
    DATA = rawRows.map(normalizeRecord);
    buildInterventionChoices();
    setFooter("Data ready. Members loaded: " + DATA.length);
    initFilters();
    initNav();
    initCollapsibles();
    initZipMapToggle();
    initZipMapControls();
    initExportButtons();
    initMemberModal();
    initInterventionModal();
    initZipModal();
    initCampaignView();
    renderAll();
  } catch (err) {
    console.error("Failed to load SDOH data", err);
    setFooter("Error loading data. Check console.");
  }
}

window.addEventListener("afterprint", function() {
  document.body.classList.remove("print-modal");
  document.body.classList.remove("print-zip-modal");
});

window.addEventListener("load", bootstrapDashboard);
