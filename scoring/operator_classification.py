"""
Operator classification gate — three-class taxonomy for ACRIS buyer names.

Pipeline path from ACRIS row to public operator page:
  1. scrapers/ownership.py pulls ACRIS deed/assignment records into ownership_raw.
     party_type='2' rows are buyers (grantees).

  2. scripts/operator_network_analysis.py reads ownership_raw, runs _operator_root()
     on each party_name_normalized to extract a brand token, then groups acquisitions
     by that token. Per-entity suppression via classify_operator_candidate() filters
     lender/bank names before they can contaminate otherwise-legitimate clusters
     (e.g. RIDGEWOOD SAVINGS BANK was clustering with RIDGEWOOD real estate LLCs).
     Outputs the top 20 qualifying clusters to operator_network_analysis.json.

  3. scripts/backfill_operators.py reads operator_network_analysis.json and upserts
     rows into the operators table. classify_operator_candidate() is called again
     here at the cluster level with DB-derived stats. Only PUBLIC_OPERATOR clusters
     are written. Suppressed clusters that already have rows are deleted (operator
     records only; raw ACRIS rows in ownership_raw are never touched).

  4. api/routes/operators.py serves GET /api/operators and GET /api/operators/{slug}
     from the operators table. The table is the gate — no runtime filtering needed.

  5. operator.html renders the profile page fetched from the API.

Classification gate summary:
  - SUPPRESSED: behavioral OR name-based hit. No DB row. Existing rows deleted.
  - PUBLIC_OPERATOR: clean name, passes stats thresholds, LLC/Corp entity structure.
  - REVIEW: doesn't clearly fail or pass. No DB row; surfaced in audit for humans.
"""

import re
from dataclasses import dataclass, field
from enum import Enum


class OperatorClass(str, Enum):
    PUBLIC_OPERATOR = "public_operator"
    SUPPRESSED = "suppressed"
    REVIEW = "review"


@dataclass
class OperatorClassification:
    operator_class: OperatorClass
    is_public_operator: bool
    reasons: list = field(default_factory=list)
    confidence: float = 0.5


# Clusters that pass the gate unconditionally.
# Each must stay PUBLIC_OPERATOR regardless of behavioral signals.
# MTEK: established multi-LLC acquisition network, extensively investigated.
# PHANTOM: same; confirmed displacement operator (PHANTOM CAPITAL cluster).
# BREDIF: bulk-transfer history looks pipeline-ish but is confirmed real estate operator.
KNOWN_OPERATOR_ALLOWLIST: frozenset = frozenset({"MTEK", "PHANTOM", "BREDIF"})

# Promotion thresholds — match operator_network_analysis.py so the gate is consistent.
_MIN_ACQUISITIONS = 5
_MIN_PROPERTIES = 5

# Entity structure patterns that indicate a real operator.
# "CORP" is included because address-named corps (e.g. "232 RIDGEWOOD AVE CORP") are
# legitimate owner entities that appear in ACRIS without "LLC" suffix.
_LLC_PATTERN = re.compile(
    r"\b(?:LLC|L\.L\.C\.?|CORP(?:ORATION)?|INC(?:ORPORATED)?|LP|LLP|REALTY|ASSOCIATES?)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Name-based suppression patterns
# Multi-word phrases first so they claim reason codes before single-word checks.
# All compiled case-insensitive; applied to name.upper() for speed.
# ---------------------------------------------------------------------------

# (compiled_pattern, reason_code)
_NAME_CHECKS: list[tuple] = []


def _p(pattern: str, reason: str) -> None:
    _NAME_CHECKS.append((re.compile(pattern, re.IGNORECASE), reason))


# GSEs and federal mortgage entities
_p(r"\bFEDERAL NATIONAL MORTGAGE ASSOCIATION\b", "gse_fnma")
_p(r"\bFEDERAL HOME LOAN MORTGAGE\b", "gse_fhlmc")
_p(r"\bFANNIE MAE\b", "gse_fnma")
_p(r"\bFREDDIE MAC\b", "gse_fhlmc")

# Named lenders (phrase patterns before single-word BANK check)
_p(r"\bBANK OF AMERICA\b", "named_bank")
_p(r"\bWELLS FARGO\b", "named_bank")
_p(r"JP\s*MORGAN", "named_bank")
_p(r"\bCHASE BANK\b", "named_bank")
_p(r"\bCITI\s*BANK\b", "named_bank")
_p(r"\bDEUTSCHE BANK\b", "named_bank")
_p(r"U\.S\.\s+BANK", "named_bank")
_p(r"\bUS BANK\b", "named_bank")
_p(r"M&T\s+BANK", "named_bank")
_p(r"\bTD BANK\b", "named_bank")
_p(r"\bPNC BANK\b", "named_bank")
_p(r"\bCAPITAL ONE\b", "named_bank")
_p(r"\bHSBC\b", "named_bank")
_p(r"\bSANTANDER\b", "named_bank")

# Named servicers
_p(r"\bSELECT PORTFOLIO SERVICING\b", "named_servicer")
_p(r"\bSPECIALIZED LOAN SERVICING\b", "named_servicer")
_p(r"\bPHH MORTGAGE\b", "named_servicer")
_p(r"\bMR\.?\s*COOPER\b", "named_servicer")
_p(r"\bNEWREZ\b", "named_servicer")
_p(r"\bNATIONSTAR\b", "named_servicer")
_p(r"\bSHELLPOINT\b", "named_servicer")
_p(r"\bRUSHMORE\b", "named_servicer")
_p(r"\bCARRINGTON\b", "named_servicer")
_p(r"\bOCWEN\b", "named_servicer")

# Multi-word bank/lender phrases
_p(r"\bSAVINGS BANK\b", "bank_keyword")
_p(r"\bNATIONAL BANK\b", "bank_keyword")
_p(r"\bTRUST COMPANY\b", "bank_keyword")
_p(r"\bFEDERAL SAVINGS\b", "bank_keyword")
_p(r"\bCREDIT UNION\b", "bank_keyword")
_p(r"\bLOAN SERVICING\b", "mortgage_keyword")

# Single-word bank/mortgage/servicer tokens (word boundaries)
_p(r"\bBANK\b", "bank_keyword")
_p(r"\bFSB\b", "bank_keyword")
_p(r"\bJPMORGAN\b", "named_bank")
_p(r"\bCITIBANK\b", "named_bank")
_p(r"\bMORTGAGE\b", "mortgage_keyword")
_p(r"\bSERVICING\b", "mortgage_keyword")
_p(r"\bISAOA\b", "isaoa_atima")
_p(r"\bATIMA\b", "isaoa_atima")

# Legal intermediaries
_p(r"\bSUCCESSOR TRUSTEE\b", "legal_intermediary")
_p(r"\bAS TRUSTEE\b", "legal_intermediary")
_p(r"\bPUBLIC ADMINISTRATOR\b", "legal_intermediary")
_p(r"\bBOARD OF MANAGERS\b", "legal_intermediary")
_p(r"\bCONDO ASSOCIATION\b", "legal_intermediary")
_p(r"\bHOMEOWNERS ASSOCIATION\b", "legal_intermediary")
_p(r"\bCONDOMINIUM\b", "legal_intermediary")
_p(r"\bTRUSTEE\b", "legal_intermediary")
_p(r"\bNOMINEE\b", "legal_intermediary")
_p(r"\bCUSTODIAN\b", "legal_intermediary")
_p(r"\bASSIGNEE\b", "legal_intermediary")
_p(r"\bREFEREE\b", "legal_intermediary")
_p(r"\bSHERIFF\b", "legal_intermediary")
_p(r"\bESTATE OF\b", "legal_intermediary")
_p(r"\bHOA\b", "legal_intermediary")

# Title / escrow / exchange
_p(r"\bTITLE INSURANCE\b", "title_escrow")
_p(r"\bTITLE COMPANY\b", "title_escrow")
_p(r"\bTITLE AGENCY\b", "title_escrow")
_p(r"\bQUALIFIED INTERMEDIARY\b", "title_escrow")
_p(r"\bEXCHANGE ACCOMMODATION\b", "title_escrow")
_p(r"\b1031 EXCHANGE\b", "title_escrow")
_p(r"\bESCROW\b", "title_escrow")

# Government / utility
_p(r"\bNEW YORK CITY\b", "government")
_p(r"\bNYC HOUSING\b", "government")
_p(r"\bNYCHA\b", "government")
_p(r"\bNYC\s+HDC\b", "government")
_p(r"\bMTA\b", "government")
_p(r"\bPORT AUTHORITY\b", "government")
_p(r"\bDEPARTMENT OF\b", "government")
_p(r"\bCOMMISSIONER OF\b", "government")


def _check_name(name: str) -> list[str]:
    """Return list of reason codes triggered by name-based patterns."""
    reasons = []
    seen = set()
    for pattern, reason in _NAME_CHECKS:
        if reason not in seen and pattern.search(name):
            reasons.append(reason)
            seen.add(reason)
    return reasons


def _check_behavioral(stats: dict) -> list[str]:
    """Return list of reason codes triggered by behavioral signals.

    Any single behavioral signal is sufficient for suppression.
    Stats keys: acquisition_count, property_count, null_amount_ratio,
    null_amount_count, median_doc_amount, median_holding_days,
    distinct_grantor_count, borough_spread.
    """
    reasons = []
    acq = stats.get("acquisition_count") or 0

    null_ratio = stats.get("null_amount_ratio")
    if acq >= 5 and null_ratio is not None and null_ratio >= 0.80:
        reasons.append("behavioral_null_amounts")

    # Only fire when the key is explicitly present (None means "confirmed no data",
    # absent means the caller didn't compute it — different signals).
    if acq >= 10 and "median_doc_amount" in stats:
        median_amount = stats["median_doc_amount"]
        if median_amount is None or median_amount < 100:
            reasons.append("behavioral_minimal_amounts")

    holding_days = stats.get("median_holding_days")
    if holding_days is not None and holding_days < 180 and acq >= 5:
        reasons.append("behavioral_short_holding")

    grantor_count = stats.get("distinct_grantor_count")
    if grantor_count is not None and grantor_count <= 3 and acq >= 10:
        reasons.append("behavioral_concentrated_grantors")

    return reasons


def _has_entity_structure(name: str) -> bool:
    """True if the name carries a recognized corporate entity suffix."""
    return bool(_LLC_PATTERN.search(name))


def classify_operator_candidate(name, stats=None):
    """
    Classify a raw buyer name string from ACRIS.

    name: party_name_normalized value from ownership_raw, or the operator's
          display name / root token when classifying at the cluster level.
    stats: dict with any subset of: acquisition_count, property_count,
           null_amount_count, null_amount_ratio, median_doc_amount,
           median_holding_days, distinct_grantor_count, borough_spread.

    Returns OperatorClassification.
    """
    if not name:
        return OperatorClassification(
            operator_class=OperatorClass.SUPPRESSED,
            is_public_operator=False,
            reasons=["empty_name"],
            confidence=1.0,
        )

    upper = name.upper().strip()
    s = stats or {}

    # Allowlist checked before all gates — these operators are confirmed.
    # The root token is the key (e.g. "MTEK", not "MTEK NYC LLC").
    root_token = upper.split()[0] if upper.split() else upper
    if root_token in KNOWN_OPERATOR_ALLOWLIST or upper in KNOWN_OPERATOR_ALLOWLIST:
        return OperatorClassification(
            operator_class=OperatorClass.PUBLIC_OPERATOR,
            is_public_operator=True,
            reasons=["allowlist"],
            confidence=1.0,
        )

    # Behavioral signals — these run first because they catch non-obvious
    # lenders that have unusual names (e.g. private-label servicer SPVs).
    behavioral_reasons = _check_behavioral(s)
    if behavioral_reasons:
        return OperatorClassification(
            operator_class=OperatorClass.SUPPRESSED,
            is_public_operator=False,
            reasons=behavioral_reasons,
            confidence=0.90,
        )

    # Name-based patterns — hard deny list for known non-operator categories.
    name_reasons = _check_name(upper)
    if name_reasons:
        return OperatorClassification(
            operator_class=OperatorClass.SUPPRESSED,
            is_public_operator=False,
            reasons=name_reasons,
            confidence=0.95,
        )

    # Promotion gate — all conditions must pass.
    acq = s.get("acquisition_count") or 0
    props = s.get("property_count") or 0
    has_entity = _has_entity_structure(upper)

    if acq < _MIN_ACQUISITIONS or props < _MIN_PROPERTIES:
        # Doesn't suppress (not a lender) but doesn't meet volume threshold.
        return OperatorClassification(
            operator_class=OperatorClass.REVIEW,
            is_public_operator=False,
            reasons=["below_threshold"],
            confidence=0.60,
        )

    if not has_entity:
        # Name has no recognized entity suffix. Could be a person, a DBA, or
        # an informal entity — worth human review before public promotion.
        return OperatorClassification(
            operator_class=OperatorClass.REVIEW,
            is_public_operator=False,
            reasons=["no_entity_structure"],
            confidence=0.55,
        )

    return OperatorClassification(
        operator_class=OperatorClass.PUBLIC_OPERATOR,
        is_public_operator=True,
        reasons=["passes_all_gates"],
        confidence=0.85,
    )


# ===========================================================================
# Public operator gate — institutional taxonomy
#
# A coarser, safety-first classifier that decides whether a cluster appears as
# a real estate OPERATOR on public pages, or is screened out as a lender, GSE,
# government body, or HDFC. Distinct from classify_operator_candidate() above
# (which drives the nightly promotion pipeline); this answers the narrower
# public-surface question and writes operators.operator_class.
#
# Classes:
#   operator              - real acquisition operator; the ONLY class shown publicly
#   financial_institution - bank, servicer, GSE, fund, insurer, foreclosure buyer
#   government            - city/state/federal housing body
#   nonprofit_hdfc        - HDFC or housing development fund corporation
#   unclassified          - default; not enough signal to promote to operator
#
# The separation that matters in the data: real operators pay market price for
# deeds; lenders, funds, and foreclosure buyers acquire via nominal/$0
# consideration or a single institutional entity name. That behavioral split
# does most of the work; the lexical term lists and the curated finance-root
# backstop catch the rest.
# ===========================================================================

OPERATOR = "operator"
FINANCIAL_INSTITUTION = "financial_institution"
GOVERNMENT = "government"
NONPROFIT_HDFC = "nonprofit_hdfc"
UNCLASSIFIED = "unclassified"

# Confirmed operators. Stay class 'operator' regardless of signals — e.g. BREDIF
# acquired a distressed-note portfolio in one $0 bulk transfer, which would
# otherwise trip the nominal-consideration behavioral check.
OPERATOR_ALLOWLIST: frozenset = frozenset(
    {"MTEK", "PHANTOM", "BREDIF", "TOWNHOUSE", "MELO", "HABIB"}
)

# Human-curated finance/lender roots. Behavioral checks already catch these, but
# they are kept as an explicit backstop so a data refresh can never quietly
# promote a known lender onto a public operator page.
KNOWN_FINANCE_ROOTS: frozenset = frozenset(
    {"ICECAP", "ICE", "BROAD", "BROADVIEW", "ARBOR", "STANDARD",
     "SYMETRA", "COMMUNITY", "OCEANVIEW", "VALLEY"}
)

# Behavioral thresholds.
_NOMINAL_MAJORITY = 0.50   # >50% of acquisitions at nominal/$0 consideration
_DOMINANT_MAJORITY = 0.50  # >50% of acquisitions under one entity name

# Lexical term lists, checked against the root plus every entity name.
# Government and HDFC are checked before financial so a housing body or HDFC is
# never mislabeled a bank.
_GOVERNMENT_TERMS = [
    "HUD", "SECRETARY OF HOUSING", "CITY OF NEW YORK", "HPD", "NYCHA",
    "NEW YORK CITY", "NYC HOUSING", "HOUSING PRESERVATION",
    "DEPARTMENT OF", "COMMISSIONER OF", "PUBLIC ADMINISTRATOR",
]
_NONPROFIT_TERMS = [
    "HDFC", "HOUSING DEVELOPMENT FUND",
]
# Financial includes the task's institutional list plus the servicer/bank names
# already maintained in _NAME_CHECKS (reused via _check_name below).
_FINANCIAL_TERMS = [
    "BANK", "SAVINGS", "FEDERAL", "CREDIT UNION", "NATIONAL ASSOCIATION",
    "TRUST COMPANY", "FANNIE MAE", "FREDDIE MAC", "FSB", "MORTGAGE",
    "SERVICING", "LOAN",
]

# Reason codes from _check_name() that mean "financial institution".
_FINANCIAL_REASONS = {
    "gse_fnma", "gse_fhlmc", "named_bank", "named_servicer",
    "bank_keyword", "mortgage_keyword", "isaoa_atima",
    "legal_intermediary", "title_escrow",
}


def _terms_hit(blob: str, terms: list[str]) -> list[str]:
    """Return the subset of terms present in blob as whole words."""
    hits = []
    for term in terms:
        if re.search(r"\b" + re.escape(term) + r"\b", blob):
            hits.append(term.lower().replace(" ", "_"))
    return hits


def _behavioral_institution(stats: dict) -> list[str]:
    """Reason codes for lender/foreclosure acquisition behavior."""
    reasons = []
    acq = stats.get("acquisition_count") or 0

    nominal_ratio = stats.get("nominal_ratio")
    if acq >= 5 and nominal_ratio is not None and nominal_ratio > _NOMINAL_MAJORITY:
        reasons.append("behavioral_majority_nominal")

    share = stats.get("dominant_entity_share")
    is_llc = stats.get("dominant_entity_is_llc")
    if (
        acq >= 5
        and share is not None and share > _DOMINANT_MAJORITY
        and is_llc is False
    ):
        reasons.append("behavioral_single_non_llc_entity")

    return reasons


def classify_operator(root: str, entity_names=None, stats=None):
    """
    Assign a cluster to the public operator taxonomy.

    root:         operator_root token (e.g. "RIDGEWOOD").
    entity_names: list of acquiring entity / LLC names in the cluster.
    stats:        dict with any of acquisition_count, property_count,
                  nominal_ratio, dominant_entity_share, dominant_entity_is_llc.

    Returns (operator_class: str, reasons: list[str]).
    """
    root_u = (root or "").upper().strip()
    names = [root_u] + [str(n).upper() for n in (entity_names or []) if n]
    blob = " ".join(names)
    s = stats or {}

    # 1. Confirmed operators win outright.
    if root_u in OPERATOR_ALLOWLIST:
        return OPERATOR, ["allowlist"]

    # 2. Curated finance-root backstop.
    if root_u in KNOWN_FINANCE_ROOTS:
        return FINANCIAL_INSTITUTION, ["known_finance_cluster"]

    # 3. Government, then HDFC — before financial so they are never mislabeled.
    gov = _terms_hit(blob, _GOVERNMENT_TERMS)
    if gov:
        return GOVERNMENT, gov

    hdfc = _terms_hit(blob, _NONPROFIT_TERMS)
    if hdfc:
        return NONPROFIT_HDFC, hdfc

    # 4. Financial: task term list + the maintained named-bank/servicer patterns.
    fin = _terms_hit(blob, _FINANCIAL_TERMS)
    fin += [r for r in _check_name(blob) if r in _FINANCIAL_REASONS]
    if fin:
        # A government reason from _check_name routes to government, not financial.
        if "government" in _check_name(blob):
            return GOVERNMENT, ["government"]
        return FINANCIAL_INSTITUTION, sorted(set(fin))

    # 5. Behavioral lender/foreclosure signature.
    behavioral = _behavioral_institution(s)
    if behavioral:
        return FINANCIAL_INSTITUTION, behavioral

    # 6. Positive operator: real entity structure and enough volume, no
    #    institutional signal above. Otherwise leave it for human review.
    acq = s.get("acquisition_count") or 0
    props = s.get("property_count") or 0
    if _has_entity_structure(blob) and acq >= _MIN_ACQUISITIONS and props >= _MIN_PROPERTIES:
        return OPERATOR, ["acquisition_operator"]

    return UNCLASSIFIED, ["insufficient_signal"]
