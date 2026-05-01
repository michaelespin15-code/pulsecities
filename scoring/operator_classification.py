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
