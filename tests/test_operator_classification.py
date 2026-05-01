"""
Unit tests for scoring/operator_classification.py.

These run without a DB connection.
"""

import pytest

from scoring.operator_classification import (
    KNOWN_OPERATOR_ALLOWLIST,
    OperatorClass,
    classify_operator_candidate,
)


# ---------------------------------------------------------------------------
# Known operators — must always be PUBLIC_OPERATOR regardless of stats.
# ---------------------------------------------------------------------------

class TestKnownOperatorAllowlist:
    def test_mtek_is_public_operator(self):
        result = classify_operator_candidate("MTEK")
        assert result.operator_class == OperatorClass.PUBLIC_OPERATOR
        assert result.is_public_operator is True
        assert "allowlist" in result.reasons

    def test_phantom_is_public_operator(self):
        result = classify_operator_candidate("PHANTOM")
        assert result.operator_class == OperatorClass.PUBLIC_OPERATOR
        assert result.is_public_operator is True

    def test_bredif_is_public_operator(self):
        result = classify_operator_candidate("BREDIF")
        assert result.operator_class == OperatorClass.PUBLIC_OPERATOR
        assert result.is_public_operator is True

    def test_mtek_with_suppression_stats_still_public(self):
        # MTEK should not be blocked even if stats look pipeline-ish.
        stats = {
            "acquisition_count": 50,
            "property_count": 40,
            "null_amount_ratio": 0.90,
            "median_holding_days": 90,
        }
        result = classify_operator_candidate("MTEK", stats)
        assert result.operator_class == OperatorClass.PUBLIC_OPERATOR

    def test_allowlist_membership(self):
        assert "MTEK" in KNOWN_OPERATOR_ALLOWLIST
        assert "PHANTOM" in KNOWN_OPERATOR_ALLOWLIST
        assert "BREDIF" in KNOWN_OPERATOR_ALLOWLIST


# ---------------------------------------------------------------------------
# Bank / lender pattern suppression
# ---------------------------------------------------------------------------

class TestBankPatterns:
    def test_ridgewood_savings_bank_suppressed(self):
        result = classify_operator_candidate("RIDGEWOOD SAVINGS BANK")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "bank_keyword" in result.reasons

    def test_ridgewood_savings_bank_isaoa_suppressed(self):
        result = classify_operator_candidate("RIDGEWOOD SAVINGS BANK ISAOA/ATIMA")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_valley_national_bank_suppressed(self):
        result = classify_operator_candidate("VALLEY NATIONAL BANK ISAOA ATIMA")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "bank_keyword" in result.reasons or "isaoa_atima" in result.reasons

    def test_ridgewood_llc_not_suppressed(self):
        # Word-boundary check: RIDGEWOOD alone has no bank token.
        result = classify_operator_candidate("6702 RIDGEWOOD LLC", {
            "acquisition_count": 10, "property_count": 8,
            "null_amount_ratio": 0.05, "median_doc_amount": 600000,
        })
        assert result.operator_class != OperatorClass.SUPPRESSED

    def test_ridgewood_407_llc_not_suppressed(self):
        result = classify_operator_candidate("RIDGEWOOD 407 LLC", {
            "acquisition_count": 10, "property_count": 8,
            "null_amount_ratio": 0.05, "median_doc_amount": 600000,
        })
        assert result.operator_class != OperatorClass.SUPPRESSED

    def test_wells_fargo_suppressed(self):
        result = classify_operator_candidate("WELLS FARGO BANK NA")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "named_bank" in result.reasons

    def test_jpmorgan_chase_suppressed(self):
        result = classify_operator_candidate("JPMORGAN CHASE BANK NATIONAL ASSOCIATION")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_jp_morgan_two_words_suppressed(self):
        result = classify_operator_candidate("JP MORGAN CHASE BANK NA")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_fannie_mae_suppressed(self):
        result = classify_operator_candidate("FEDERAL NATIONAL MORTGAGE ASSOCIATION")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "gse_fnma" in result.reasons

    def test_freddie_mac_suppressed(self):
        result = classify_operator_candidate("FREDDIE MAC")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "gse_fhlmc" in result.reasons

    def test_bank_of_america_suppressed(self):
        result = classify_operator_candidate("BANK OF AMERICA NA")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_newrez_suppressed(self):
        result = classify_operator_candidate("NEWREZ LLC")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "named_servicer" in result.reasons

    def test_nationstar_suppressed(self):
        result = classify_operator_candidate("NATIONSTAR MORTGAGE LLC")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_ocwen_suppressed(self):
        result = classify_operator_candidate("OCWEN LOAN SERVICING LLC")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_select_portfolio_servicing_suppressed(self):
        result = classify_operator_candidate("SELECT PORTFOLIO SERVICING INC")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "named_servicer" in result.reasons

    def test_credit_union_suppressed(self):
        result = classify_operator_candidate("NYC METROPOLITAN FEDERAL CREDIT UNION")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "bank_keyword" in result.reasons

    def test_trust_company_suppressed(self):
        result = classify_operator_candidate("WILMINGTON TRUST COMPANY")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_isaoa_alone_suppressed(self):
        result = classify_operator_candidate("SOME LENDER LLC ISAOA")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "isaoa_atima" in result.reasons

    def test_fsb_suppressed(self):
        result = classify_operator_candidate("STANDARD FEDERAL BANK FSB")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_mortgage_word_suppressed(self):
        result = classify_operator_candidate("ACME MORTGAGE CORP")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "mortgage_keyword" in result.reasons

    def test_hsbc_suppressed(self):
        result = classify_operator_candidate("HSBC BANK USA NA")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_capital_one_suppressed(self):
        # "CAPITAL ONE" as phrase; "PHANTOM CAPITAL LLC" must not match.
        result = classify_operator_candidate("CAPITAL ONE NA")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "named_bank" in result.reasons

    def test_phantom_capital_not_suppressed_by_capital_one(self):
        # "CAPITAL ONE" phrase pattern must not fire on "PHANTOM CAPITAL LLC"
        result = classify_operator_candidate("PHANTOM CAPITAL LLC", {
            "acquisition_count": 20, "property_count": 15
        })
        assert result.operator_class != OperatorClass.SUPPRESSED

    def test_mr_cooper_suppressed(self):
        result = classify_operator_candidate("MR COOPER GROUP INC")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_us_bank_suppressed(self):
        result = classify_operator_candidate("US BANK NATIONAL ASSOCIATION")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_us_bank_dot_notation_suppressed(self):
        result = classify_operator_candidate("U.S. BANK TRUST NA")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_mt_bank_suppressed(self):
        result = classify_operator_candidate("M&T BANK")
        assert result.operator_class == OperatorClass.SUPPRESSED


# ---------------------------------------------------------------------------
# Legal intermediary suppression
# ---------------------------------------------------------------------------

class TestLegalIntermediaries:
    def test_trustee_suppressed(self):
        result = classify_operator_candidate("WILMINGTON SAVINGS FUND SOCIETY FSB AS TRUSTEE")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_as_trustee_suppressed(self):
        result = classify_operator_candidate("DEUTSCHE BANK NATIONAL TRUST COMPANY AS TRUSTEE")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_referee_suppressed(self):
        result = classify_operator_candidate("REFEREE IN FORECLOSURE")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "legal_intermediary" in result.reasons

    def test_sheriff_suppressed(self):
        result = classify_operator_candidate("SHERIFF OF THE CITY OF NEW YORK")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_public_administrator_suppressed(self):
        result = classify_operator_candidate("PUBLIC ADMINISTRATOR BRONX COUNTY")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_condominium_suppressed(self):
        result = classify_operator_candidate("THE CONDOMINIUM AT PARK PLACE")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_hoa_suppressed(self):
        result = classify_operator_candidate("SUNRISE HOA INC")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_homeowners_association_suppressed(self):
        result = classify_operator_candidate("PARK SLOPE HOMEOWNERS ASSOCIATION")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_board_of_managers_suppressed(self):
        result = classify_operator_candidate("BOARD OF MANAGERS OF 45 PARK PLACE CONDOMINIUM")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_nominee_suppressed(self):
        result = classify_operator_candidate("MERS AS NOMINEE")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_estate_of_suppressed(self):
        result = classify_operator_candidate("ESTATE OF JOHN DOE")
        assert result.operator_class == OperatorClass.SUPPRESSED


# ---------------------------------------------------------------------------
# Title / escrow / exchange suppression
# ---------------------------------------------------------------------------

class TestTitleEscrow:
    def test_title_insurance_suppressed(self):
        result = classify_operator_candidate("FIRST AMERICAN TITLE INSURANCE COMPANY")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "title_escrow" in result.reasons

    def test_title_company_suppressed(self):
        result = classify_operator_candidate("CHICAGO TITLE COMPANY")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_escrow_suppressed(self):
        result = classify_operator_candidate("ACME ESCROW SERVICES LLC")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_1031_exchange_suppressed(self):
        result = classify_operator_candidate("IPX 1031 EXCHANGE INC")
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "title_escrow" in result.reasons

    def test_qualified_intermediary_suppressed(self):
        result = classify_operator_candidate("ACCRUIT LLC QUALIFIED INTERMEDIARY")
        assert result.operator_class == OperatorClass.SUPPRESSED


# ---------------------------------------------------------------------------
# Government / utility suppression
# ---------------------------------------------------------------------------

class TestGovernmentPatterns:
    def test_nycha_suppressed(self):
        result = classify_operator_candidate("NEW YORK CITY HOUSING AUTHORITY NYCHA")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_nyc_housing_suppressed(self):
        result = classify_operator_candidate("NYC HOUSING PRESERVATION AND DEVELOPMENT")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_department_of_suppressed(self):
        result = classify_operator_candidate("DEPARTMENT OF HOUSING AND URBAN DEVELOPMENT")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_commissioner_of_suppressed(self):
        result = classify_operator_candidate("COMMISSIONER OF FINANCE CITY OF NEW YORK")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_port_authority_suppressed(self):
        result = classify_operator_candidate("PORT AUTHORITY OF NEW YORK AND NEW JERSEY")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_mta_suppressed(self):
        result = classify_operator_candidate("MTA BRIDGES AND TUNNELS")
        assert result.operator_class == OperatorClass.SUPPRESSED


# ---------------------------------------------------------------------------
# Behavioral signal suppression
# ---------------------------------------------------------------------------

class TestBehavioralSignals:
    def test_high_null_amount_ratio_suppressed(self):
        stats = {
            "acquisition_count": 20,
            "property_count": 18,
            "null_amount_ratio": 0.90,
        }
        result = classify_operator_candidate("ACME HOLDINGS LLC", stats)
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "behavioral_null_amounts" in result.reasons

    def test_null_ratio_below_threshold_not_suppressed(self):
        stats = {
            "acquisition_count": 20,
            "property_count": 18,
            "null_amount_ratio": 0.70,
            "median_doc_amount": 500000,
        }
        result = classify_operator_candidate("ACME HOLDINGS LLC", stats)
        assert result.operator_class != OperatorClass.SUPPRESSED

    def test_minimal_doc_amounts_suppressed(self):
        stats = {
            "acquisition_count": 15,
            "property_count": 14,
            "median_doc_amount": 10,
        }
        result = classify_operator_candidate("PIPELINE FUND LLC", stats)
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "behavioral_minimal_amounts" in result.reasons

    def test_null_median_doc_amount_at_threshold_suppressed(self):
        stats = {
            "acquisition_count": 12,
            "property_count": 10,
            "median_doc_amount": None,
        }
        result = classify_operator_candidate("PIPELINE FUND LLC", stats)
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_short_holding_period_suppressed(self):
        stats = {
            "acquisition_count": 10,
            "property_count": 9,
            "median_holding_days": 90,
        }
        result = classify_operator_candidate("FLIP CAPITAL LLC", stats)
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "behavioral_short_holding" in result.reasons

    def test_holding_period_above_threshold_not_suppressed(self):
        stats = {
            "acquisition_count": 10,
            "property_count": 9,
            "median_holding_days": 365,
            "null_amount_ratio": 0.10,
            "median_doc_amount": 800000,
        }
        result = classify_operator_candidate("STEADFAST REALTY LLC", stats)
        assert result.operator_class != OperatorClass.SUPPRESSED

    def test_concentrated_grantors_suppressed(self):
        stats = {
            "acquisition_count": 15,
            "property_count": 14,
            "distinct_grantor_count": 2,
        }
        result = classify_operator_candidate("FORECLOSURE PIPELINE LLC", stats)
        assert result.operator_class == OperatorClass.SUPPRESSED
        assert "behavioral_concentrated_grantors" in result.reasons

    def test_diverse_grantors_not_suppressed(self):
        stats = {
            "acquisition_count": 15,
            "property_count": 14,
            "distinct_grantor_count": 10,
            "null_amount_ratio": 0.10,
            "median_doc_amount": 700000,
        }
        result = classify_operator_candidate("DIVERSE BUYS LLC", stats)
        assert result.operator_class != OperatorClass.SUPPRESSED


# ---------------------------------------------------------------------------
# Promotion criteria
# ---------------------------------------------------------------------------

class TestPromotionCriteria:
    def test_public_operator_all_gates_pass(self):
        stats = {
            "acquisition_count": 15,
            "property_count": 12,
            "null_amount_ratio": 0.10,
            "median_doc_amount": 850000,
            "median_holding_days": 600,
            "distinct_grantor_count": 8,
        }
        result = classify_operator_candidate("BROWNSTONE EQUITIES LLC", stats)
        assert result.operator_class == OperatorClass.PUBLIC_OPERATOR
        assert result.is_public_operator is True

    def test_below_acquisition_threshold_is_review(self):
        stats = {"acquisition_count": 3, "property_count": 3}
        result = classify_operator_candidate("SMALL REALTY LLC", stats)
        assert result.operator_class == OperatorClass.REVIEW
        assert result.is_public_operator is False

    def test_below_property_threshold_is_review(self):
        stats = {"acquisition_count": 10, "property_count": 2}
        result = classify_operator_candidate("THIN PORTFOLIO LLC", stats)
        assert result.operator_class == OperatorClass.REVIEW

    def test_no_entity_structure_is_review(self):
        stats = {"acquisition_count": 15, "property_count": 12}
        result = classify_operator_candidate("JOHN SMITH", stats)
        assert result.operator_class == OperatorClass.REVIEW
        assert "no_entity_structure" in result.reasons

    def test_no_stats_is_review_not_suppressed(self):
        # Clean name with no stats defaults to REVIEW (can't verify volume).
        result = classify_operator_candidate("URBAN GROWTH REALTY LLC")
        assert result.operator_class == OperatorClass.REVIEW

    def test_empty_name_suppressed(self):
        result = classify_operator_candidate("")
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_none_name_suppressed(self):
        result = classify_operator_candidate(None)
        assert result.operator_class == OperatorClass.SUPPRESSED

    def test_review_is_not_public(self):
        result = classify_operator_candidate("SOME CLEAN LLC")
        assert result.is_public_operator is False

    def test_corp_suffix_accepted_as_entity_structure(self):
        # Address-named corps (e.g. "232 RIDGEWOOD AVE CORP") must pass entity check.
        stats = {"acquisition_count": 10, "property_count": 8}
        result = classify_operator_candidate("232 RIDGEWOOD AVE CORP", stats)
        # Should not be suppressed by name patterns.
        assert "bank_keyword" not in (result.reasons or [])
        assert result.operator_class != OperatorClass.SUPPRESSED
