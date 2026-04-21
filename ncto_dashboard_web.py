from __future__ import annotations

import os
from urllib.parse import urlencode, parse_qs, quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from dash import Dash, html, dcc, Input, Output, State
from waitress import serve

load_dotenv()

# =========================================================
# CONFIG
# =========================================================

SERVER = os.getenv("DB_HOST", "102.164.37.69")
PORT_DB = int(os.getenv("DB_PORT", "5432"))
DATABASE = os.getenv("DB_NAME", "ben_db")
USER = os.getenv("DB_USER", "ben_user")
PASSWORD = os.getenv("BEN_DB_PASSWORD", "")
SCHEMA = os.getenv("DB_SCHEMA", "ben")

APP_TITLE = os.getenv("APP_TITLE", "Executive Dashboard")

FIRST_TABLE = os.getenv("FIRST_TABLE", "itblDistinctPaidBeneficiaries")
SECOND_TABLE = os.getenv("SECOND_TABLE", "itblDistinctSecondTranche")
THIRD_TABLE = os.getenv("THIRD_TABLE", "itblDistinctThirdTranche")

FIRST_AMOUNT_COL = os.getenv("FIRST_AMOUNT_COL", "AmountPaid")
SECOND_AMOUNT_COL = os.getenv("SECOND_AMOUNT_COL", "AmountPaid")
THIRD_AMOUNT_COL = os.getenv("THIRD_AMOUNT_COL", "AmountPaid")

FIRST_STATE_COL = os.getenv("FIRST_STATE_COL", "State")
SECOND_STATE_COL = os.getenv("SECOND_STATE_COL", "State")
THIRD_STATE_COL = os.getenv("THIRD_STATE_COL", "State")

FIRST_LGA_COL = os.getenv("FIRST_LGA_COL", "LGA")
SECOND_LGA_COL = os.getenv("SECOND_LGA_COL", "LGA")
THIRD_LGA_COL = os.getenv("THIRD_LGA_COL", "LGA")

FIRST_WARD_COL = os.getenv("FIRST_WARD_COL", "Ward")
SECOND_WARD_COL = os.getenv("SECOND_WARD_COL", "Ward")
THIRD_WARD_COL = os.getenv("THIRD_WARD_COL", "Ward")

FIRST_COMMUNITY_COL = os.getenv("FIRST_COMMUNITY_COL", "Community")
SECOND_COMMUNITY_COL = os.getenv("SECOND_COMMUNITY_COL", "Community")
THIRD_COMMUNITY_COL = os.getenv("THIRD_COMMUNITY_COL", "Community")

FIRST_NIDHH_COL = os.getenv("FIRST_NIDHH_COL", "nidhh")
SECOND_NIDHH_COL = os.getenv("SECOND_NIDHH_COL", "nidhh")
THIRD_NIDHH_COL = os.getenv("THIRD_NIDHH_COL", "nidhh")

FIRST_GENDER_COL = os.getenv("FIRST_GENDER_COL", "Gender")
SECOND_GENDER_COL = os.getenv("SECOND_GENDER_COL", "Gender")
THIRD_GENDER_COL = os.getenv("THIRD_GENDER_COL", "Gender")

FIRST_AGE_COL = os.getenv("FIRST_AGE_COL", "Age")
SECOND_AGE_COL = os.getenv("SECOND_AGE_COL", "Age")
THIRD_AGE_COL = os.getenv("THIRD_AGE_COL", "Age")

FIRST_NINBVN_COL = os.getenv("FIRST_NINBVN_COL", "NINBVN")
SECOND_NINBVN_COL = os.getenv("SECOND_NINBVN_COL", "NINBVN")
THIRD_NINBVN_COL = os.getenv("THIRD_NINBVN_COL", "NINBVN")

FIRST_TELEPHONE_COL = os.getenv("FIRST_TELEPHONE_COL", "TelephoneNo")
SECOND_TELEPHONE_COL = os.getenv("SECOND_TELEPHONE_COL", "TelephoneNo")
THIRD_TELEPHONE_COL = os.getenv("THIRD_TELEPHONE_COL", "TelephoneNo")

FIRST_HADDRESS_COL = os.getenv("FIRST_HADDRESS_COL", "HAddress")
SECOND_HADDRESS_COL = os.getenv("SECOND_HADDRESS_COL", "HAddress")
THIRD_HADDRESS_COL = os.getenv("THIRD_HADDRESS_COL", "HAddress")

FIRST_ACCOUNT_USED_COL = os.getenv("FIRST_ACCOUNT_USED_COL", "AccountUsed")
SECOND_ACCOUNT_USED_COL = os.getenv("SECOND_ACCOUNT_USED_COL", "AccountUsed")
THIRD_ACCOUNT_USED_COL = os.getenv("THIRD_ACCOUNT_USED_COL", "AccountUsed")

FIRST_ACCOUNT_NAME_COL = os.getenv("FIRST_ACCOUNT_NAME_COL", "AccountName")
SECOND_ACCOUNT_NAME_COL = os.getenv("SECOND_ACCOUNT_NAME_COL", "AccountName")
THIRD_ACCOUNT_NAME_COL = os.getenv("THIRD_ACCOUNT_NAME_COL", "AccountName")

FIRST_ACCOUNT_NUMBER_COL = os.getenv("FIRST_ACCOUNT_NUMBER_COL", "AccountNumber")
SECOND_ACCOUNT_NUMBER_COL = os.getenv("SECOND_ACCOUNT_NUMBER_COL", "AccountNumber")
THIRD_ACCOUNT_NUMBER_COL = os.getenv("THIRD_ACCOUNT_NUMBER_COL", "AccountNumber")

FIRST_BANK_NAME_COL = os.getenv("FIRST_BANK_NAME_COL", "BankName")
SECOND_BANK_NAME_COL = os.getenv("SECOND_BANK_NAME_COL", "BankName")
THIRD_BANK_NAME_COL = os.getenv("THIRD_BANK_NAME_COL", "BankName")

FIRST_PAYMENT_DATE_COL = os.getenv("FIRST_PAYMENT_DATE_COL", "PaymentDate")
SECOND_PAYMENT_DATE_COL = os.getenv("SECOND_PAYMENT_DATE_COL", "PaymentDate")
THIRD_PAYMENT_DATE_COL = os.getenv("THIRD_PAYMENT_DATE_COL", "PaymentDate")

WEB_HOST = "0.0.0.0"
WEB_PORT = int(os.getenv("PORT", "8050"))

BANK_OPTIONS_CACHE: list[str] | None = None


# =========================================================
# DATABASE
# =========================================================

def pg_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def qtable(schema: str, table: str) -> str:
    return f"{pg_ident(schema)}.{pg_ident(table)}"


def build_engine():
    if not PASSWORD:
        raise RuntimeError("Database password not set. Set BEN_DB_PASSWORD first.")

    conn_str = (
        f"postgresql+psycopg2://{USER}:{quote_plus(PASSWORD)}"
        f"@{SERVER}:{PORT_DB}/{DATABASE}"
    )

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True,
    )


engine = build_engine()


# =========================================================
# HELPERS
# =========================================================

def safe_int(v):
    if pd.isna(v):
        return 0
    return int(v)


def safe_float(v):
    if pd.isna(v):
        return 0.0
    return float(v)


def format_currency(v):
    return f"₦{safe_float(v):,.2f}"


def read_sql_df(sql: str, params=None) -> pd.DataFrame:
    try:
        with engine.begin() as conn:
            return pd.read_sql_query(text(sql), conn, params=params)
    except Exception as e:
        print("\nSQL FAILED:\n")
        print(sql)
        print("\nPARAMS:", params)
        print("\nERROR:", repr(e))
        raise


def get_physical_col(logical_col: str, state_col: str, lga_col: str, ward_col: str, community_col: str) -> str:
    mapping = {
        "State": state_col,
        "Lga": lga_col,
        "Ward": ward_col,
        "Community": community_col,
    }
    return mapping[logical_col]


def make_error_panel(title: str, exc: Exception):
    import traceback
    err_text = "".join(traceback.format_exception_only(type(exc), exc)).strip()
    return html.Div(
        [
            html.Div(title, className="error-title"),
            html.Pre(err_text, className="error-pre"),
        ],
        className="error-panel",
    )


# =========================================================
# SQL BUILDERS - SUMMARY
# =========================================================

def build_union_summary_sql(group_cols: list[str], filters: list[str]) -> str:
    if not group_cols:
        raise ValueError("group_cols cannot be empty")

    first_table = qtable(SCHEMA, FIRST_TABLE)
    second_table = qtable(SCHEMA, SECOND_TABLE)
    third_table = qtable(SCHEMA, THIRD_TABLE)

    def select_cols_expr(group_cols_, state_col, lga_col, ward_col, community_col):
        parts = []
        for logical_col in group_cols_:
            physical = get_physical_col(logical_col, state_col, lga_col, ward_col, community_col)
            parts.append(f"TRIM({pg_ident(physical)}) AS {pg_ident(logical_col)}")
        return ",\n            ".join(parts)

    def group_by_cols_expr(group_cols_, state_col, lga_col, ward_col, community_col):
        parts = []
        for logical_col in group_cols_:
            physical = get_physical_col(logical_col, state_col, lga_col, ward_col, community_col)
            parts.append(f"TRIM({pg_ident(physical)})")
        return ", ".join(parts)

    def outer_select_cols(cols):
        return ",\n        ".join([f'x.{pg_ident(c)}' for c in cols])

    def outer_group_by_cols(cols):
        return ", ".join([f'x.{pg_ident(c)}' for c in cols])

    def order_by_cols_expr(cols):
        return ", ".join([f'x.{pg_ident(c)} ASC' for c in cols])

    def build_filter_sql(filters_, state_col, lga_col, ward_col, community_col):
        if not filters_:
            return ""

        replaced = []
        for expr in filters_:
            x = expr.replace("[State]", pg_ident(state_col))
            x = x.replace("[Lga]", pg_ident(lga_col))
            x = x.replace("[Ward]", pg_ident(ward_col))
            x = x.replace("[Community]", pg_ident(community_col))
            replaced.append(x)

        return "\n          AND " + "\n          AND ".join(replaced)

    outer_select = outer_select_cols(group_cols)
    outer_group_by = outer_group_by_cols(group_cols)
    order_by_cols = order_by_cols_expr(group_cols)

    first_select_cols = select_cols_expr(group_cols, FIRST_STATE_COL, FIRST_LGA_COL, FIRST_WARD_COL, FIRST_COMMUNITY_COL)
    second_select_cols = select_cols_expr(group_cols, SECOND_STATE_COL, SECOND_LGA_COL, SECOND_WARD_COL, SECOND_COMMUNITY_COL)
    third_select_cols = select_cols_expr(group_cols, THIRD_STATE_COL, THIRD_LGA_COL, THIRD_WARD_COL, THIRD_COMMUNITY_COL)

    first_group_by = group_by_cols_expr(group_cols, FIRST_STATE_COL, FIRST_LGA_COL, FIRST_WARD_COL, FIRST_COMMUNITY_COL)
    second_group_by = group_by_cols_expr(group_cols, SECOND_STATE_COL, SECOND_LGA_COL, SECOND_WARD_COL, SECOND_COMMUNITY_COL)
    third_group_by = group_by_cols_expr(group_cols, THIRD_STATE_COL, THIRD_LGA_COL, THIRD_WARD_COL, THIRD_COMMUNITY_COL)

    first_filter_sql = build_filter_sql(filters, FIRST_STATE_COL, FIRST_LGA_COL, FIRST_WARD_COL, FIRST_COMMUNITY_COL)
    second_filter_sql = build_filter_sql(filters, SECOND_STATE_COL, SECOND_LGA_COL, SECOND_WARD_COL, SECOND_COMMUNITY_COL)
    third_filter_sql = build_filter_sql(filters, THIRD_STATE_COL, THIRD_LGA_COL, THIRD_WARD_COL, THIRD_COMMUNITY_COL)

    return f"""
    SELECT
        {outer_select},
        SUM(x."FirstTrancheCount") AS "FirstTrancheCount",
        SUM(x."FirstTrancheAmount") AS "FirstTrancheAmount",
        SUM(x."SecondTrancheCount") AS "SecondTrancheCount",
        SUM(x."SecondTrancheAmount") AS "SecondTrancheAmount",
        SUM(x."ThirdTrancheCount") AS "ThirdTrancheCount",
        SUM(x."ThirdTrancheAmount") AS "ThirdTrancheAmount",
        SUM(x."FirstTrancheAmount" + x."SecondTrancheAmount" + x."ThirdTrancheAmount") AS "TotalAmount"
    FROM (
        SELECT
            {first_select_cols},
            COUNT(*) AS "FirstTrancheCount",
            SUM(CAST(COALESCE({pg_ident(FIRST_AMOUNT_COL)}, 0) AS NUMERIC(18,2))) AS "FirstTrancheAmount",
            0 AS "SecondTrancheCount",
            CAST(0 AS NUMERIC(18,2)) AS "SecondTrancheAmount",
            0 AS "ThirdTrancheCount",
            CAST(0 AS NUMERIC(18,2)) AS "ThirdTrancheAmount"
        FROM {first_table}
        WHERE {pg_ident(FIRST_STATE_COL)} IS NOT NULL
          AND TRIM({pg_ident(FIRST_STATE_COL)}) <> ''
          {first_filter_sql}
        GROUP BY {first_group_by}

        UNION ALL

        SELECT
            {second_select_cols},
            0 AS "FirstTrancheCount",
            CAST(0 AS NUMERIC(18,2)) AS "FirstTrancheAmount",
            COUNT(*) AS "SecondTrancheCount",
            SUM(CAST(COALESCE({pg_ident(SECOND_AMOUNT_COL)}, 0) AS NUMERIC(18,2))) AS "SecondTrancheAmount",
            0 AS "ThirdTrancheCount",
            CAST(0 AS NUMERIC(18,2)) AS "ThirdTrancheAmount"
        FROM {second_table}
        WHERE {pg_ident(SECOND_STATE_COL)} IS NOT NULL
          AND TRIM({pg_ident(SECOND_STATE_COL)}) <> ''
          {second_filter_sql}
        GROUP BY {second_group_by}

        UNION ALL

        SELECT
            {third_select_cols},
            0 AS "FirstTrancheCount",
            CAST(0 AS NUMERIC(18,2)) AS "FirstTrancheAmount",
            0 AS "SecondTrancheCount",
            CAST(0 AS NUMERIC(18,2)) AS "SecondTrancheAmount",
            COUNT(*) AS "ThirdTrancheCount",
            SUM(CAST(COALESCE({pg_ident(THIRD_AMOUNT_COL)}, 0) AS NUMERIC(18,2))) AS "ThirdTrancheAmount"
        FROM {third_table}
        WHERE {pg_ident(THIRD_STATE_COL)} IS NOT NULL
          AND TRIM({pg_ident(THIRD_STATE_COL)}) <> ''
          {third_filter_sql}
        GROUP BY {third_group_by}
    ) x
    GROUP BY {outer_group_by}
    ORDER BY {order_by_cols};
    """


# =========================================================
# SQL BUILDERS - DETAILS / SEARCH
# =========================================================

def build_beneficiary_cte_sql(first_where: str, second_where: str, third_where: str) -> str:
    first_table = qtable(SCHEMA, FIRST_TABLE)
    second_table = qtable(SCHEMA, SECOND_TABLE)
    third_table = qtable(SCHEMA, THIRD_TABLE)

    def c(col: str) -> str:
        return pg_ident(col)

    return f"""
    WITH first_src AS (
        SELECT
            TRIM({c(FIRST_STATE_COL)}) AS "State",
            TRIM({c(FIRST_LGA_COL)}) AS "Lga",
            TRIM({c(FIRST_WARD_COL)}) AS "Ward",
            TRIM({c(FIRST_COMMUNITY_COL)}) AS "Community",
            CAST({c(FIRST_NIDHH_COL)} AS TEXT) AS "nidhh",
            CAST({c(FIRST_GENDER_COL)} AS TEXT) AS "Gender",
            CAST({c(FIRST_AGE_COL)} AS TEXT) AS "Age",
            CAST({c(FIRST_NINBVN_COL)} AS TEXT) AS "NINBVN",
            CAST({c(FIRST_TELEPHONE_COL)} AS TEXT) AS "Telephone",
            CAST({c(FIRST_HADDRESS_COL)} AS TEXT) AS "HAddress",
            CAST({c(FIRST_ACCOUNT_USED_COL)} AS TEXT) AS "AccountUsed",
            CAST({c(FIRST_ACCOUNT_NAME_COL)} AS TEXT) AS "FirstTrancheAccountName",
            CAST({c(FIRST_ACCOUNT_NUMBER_COL)} AS TEXT) AS "FirstTrancheAccountNumber",
            CAST({c(FIRST_BANK_NAME_COL)} AS TEXT) AS "FirstTrancheBankName",
            CAST({c(FIRST_PAYMENT_DATE_COL)} AS TEXT) AS "FirstTranchePaymentDate",
            CAST(COALESCE({c(FIRST_AMOUNT_COL)}, 0) AS NUMERIC(18,2)) AS "FirstTrancheAmount"
        FROM {first_table}
        WHERE {first_where}
    ),
    second_src AS (
        SELECT
            TRIM({c(SECOND_STATE_COL)}) AS "State",
            TRIM({c(SECOND_LGA_COL)}) AS "Lga",
            TRIM({c(SECOND_WARD_COL)}) AS "Ward",
            TRIM({c(SECOND_COMMUNITY_COL)}) AS "Community",
            CAST({c(SECOND_NIDHH_COL)} AS TEXT) AS "nidhh",
            CAST({c(SECOND_GENDER_COL)} AS TEXT) AS "Gender",
            CAST({c(SECOND_AGE_COL)} AS TEXT) AS "Age",
            CAST({c(SECOND_NINBVN_COL)} AS TEXT) AS "NINBVN",
            CAST({c(SECOND_TELEPHONE_COL)} AS TEXT) AS "Telephone",
            CAST({c(SECOND_HADDRESS_COL)} AS TEXT) AS "HAddress",
            CAST({c(SECOND_ACCOUNT_USED_COL)} AS TEXT) AS "AccountUsed",
            CAST({c(SECOND_ACCOUNT_NAME_COL)} AS TEXT) AS "SecondTrancheAccountName",
            CAST({c(SECOND_ACCOUNT_NUMBER_COL)} AS TEXT) AS "SecondTrancheAccountNumber",
            CAST({c(SECOND_BANK_NAME_COL)} AS TEXT) AS "SecondTrancheBankName",
            CAST({c(SECOND_PAYMENT_DATE_COL)} AS TEXT) AS "SecondTranchePaymentDate",
            CAST(COALESCE({c(SECOND_AMOUNT_COL)}, 0) AS NUMERIC(18,2)) AS "SecondTrancheAmount"
        FROM {second_table}
        WHERE {second_where}
    ),
    third_src AS (
        SELECT
            TRIM({c(THIRD_STATE_COL)}) AS "State",
            TRIM({c(THIRD_LGA_COL)}) AS "Lga",
            TRIM({c(THIRD_WARD_COL)}) AS "Ward",
            TRIM({c(THIRD_COMMUNITY_COL)}) AS "Community",
            CAST({c(THIRD_NIDHH_COL)} AS TEXT) AS "nidhh",
            CAST({c(THIRD_GENDER_COL)} AS TEXT) AS "Gender",
            CAST({c(THIRD_AGE_COL)} AS TEXT) AS "Age",
            CAST({c(THIRD_NINBVN_COL)} AS TEXT) AS "NINBVN",
            CAST({c(THIRD_TELEPHONE_COL)} AS TEXT) AS "Telephone",
            CAST({c(THIRD_HADDRESS_COL)} AS TEXT) AS "HAddress",
            CAST({c(THIRD_ACCOUNT_USED_COL)} AS TEXT) AS "AccountUsed",
            CAST({c(THIRD_ACCOUNT_NAME_COL)} AS TEXT) AS "ThirdTrancheAccountName",
            CAST({c(THIRD_ACCOUNT_NUMBER_COL)} AS TEXT) AS "ThirdTrancheAccountNumber",
            CAST({c(THIRD_BANK_NAME_COL)} AS TEXT) AS "ThirdTrancheBankName",
            CAST({c(THIRD_PAYMENT_DATE_COL)} AS TEXT) AS "ThirdTranchePaymentDate",
            CAST(COALESCE({c(THIRD_AMOUNT_COL)}, 0) AS NUMERIC(18,2)) AS "ThirdTrancheAmount"
        FROM {third_table}
        WHERE {third_where}
    ),
    keys AS (
        SELECT "nidhh" FROM first_src
        UNION
        SELECT "nidhh" FROM second_src
        UNION
        SELECT "nidhh" FROM third_src
    )
    SELECT
        COALESCE(f."State", s."State", t."State") AS "State",
        COALESCE(f."Lga", s."Lga", t."Lga") AS "Lga",
        COALESCE(f."Ward", s."Ward", t."Ward") AS "Ward",
        COALESCE(f."Community", s."Community", t."Community") AS "Community",
        k."nidhh",
        COALESCE(f."Gender", s."Gender", t."Gender") AS "Gender",
        COALESCE(f."Age", s."Age", t."Age") AS "Age",
        COALESCE(f."NINBVN", s."NINBVN", t."NINBVN") AS "NINBVN",
        COALESCE(f."Telephone", s."Telephone", t."Telephone") AS "Telephone",
        COALESCE(f."HAddress", s."HAddress", t."HAddress") AS "HAddress",
        CASE
            WHEN f."nidhh" IS NOT NULL AND s."nidhh" IS NOT NULL AND t."nidhh" IS NOT NULL THEN 'First, Second, Third'
            WHEN f."nidhh" IS NOT NULL AND s."nidhh" IS NOT NULL THEN 'First, Second'
            WHEN f."nidhh" IS NOT NULL AND t."nidhh" IS NOT NULL THEN 'First, Third'
            WHEN s."nidhh" IS NOT NULL AND t."nidhh" IS NOT NULL THEN 'Second, Third'
            WHEN f."nidhh" IS NOT NULL THEN 'First'
            WHEN s."nidhh" IS NOT NULL THEN 'Second'
            WHEN t."nidhh" IS NOT NULL THEN 'Third'
            ELSE NULL
        END AS "TrancheStatus",
        COALESCE(f."FirstTrancheAmount", 0)
          + COALESCE(s."SecondTrancheAmount", 0)
          + COALESCE(t."ThirdTrancheAmount", 0) AS "TotalAmount",
        COALESCE(t."AccountUsed", s."AccountUsed", f."AccountUsed") AS "AccountUsed",
        f."FirstTrancheAccountName",
        f."FirstTrancheAccountNumber",
        f."FirstTrancheBankName",
        f."FirstTranchePaymentDate",
        s."SecondTrancheAccountName",
        s."SecondTrancheAccountNumber",
        s."SecondTrancheBankName",
        s."SecondTranchePaymentDate",
        t."ThirdTrancheAccountName",
        t."ThirdTrancheAccountNumber",
        t."ThirdTrancheBankName",
        t."ThirdTranchePaymentDate"
    FROM keys k
    LEFT JOIN first_src f ON k."nidhh" = f."nidhh"
    LEFT JOIN second_src s ON k."nidhh" = s."nidhh"
    LEFT JOIN third_src t ON k."nidhh" = t."nidhh"
    ORDER BY
        COALESCE(f."State", s."State", t."State") ASC,
        COALESCE(f."Lga", s."Lga", t."Lga") ASC,
        COALESCE(f."Ward", s."Ward", t."Ward") ASC,
        COALESCE(f."Community", s."Community", t."Community") ASC,
        k."nidhh" ASC;
    """


def build_beneficiary_detail_sql() -> str:
    return build_beneficiary_cte_sql(
        'TRIM("State") = TRIM(:state) AND TRIM("LGA") = TRIM(:lga) AND TRIM("Ward") = TRIM(:ward) AND TRIM("Community") = TRIM(:community)',
        'TRIM("State") = TRIM(:state) AND TRIM("LGA") = TRIM(:lga) AND TRIM("Ward") = TRIM(:ward) AND TRIM("Community") = TRIM(:community)',
        'TRIM("State") = TRIM(:state) AND TRIM("LGA") = TRIM(:lga) AND TRIM("Ward") = TRIM(:ward) AND TRIM("Community") = TRIM(:community)',
    )


def build_beneficiary_search_sql(mode: str) -> str:
    if mode == "nin":
        return build_beneficiary_cte_sql(
            'TRIM(CAST("NINBVN" AS TEXT)) = TRIM(:nin)',
            'TRIM(CAST("NINBVN" AS TEXT)) = TRIM(:nin)',
            'TRIM(CAST("NINBVN" AS TEXT)) = TRIM(:nin)',
        )
    if mode == "account":
        return build_beneficiary_cte_sql(
            'TRIM(CAST("AccountNumber" AS TEXT)) = TRIM(:account_number) AND UPPER(TRIM(CAST("BankName" AS TEXT))) = UPPER(TRIM(:bank_name))',
            'TRIM(CAST("AccountNumber" AS TEXT)) = TRIM(:account_number) AND UPPER(TRIM(CAST("BankName" AS TEXT))) = UPPER(TRIM(:bank_name))',
            'TRIM(CAST("AccountNumber" AS TEXT)) = TRIM(:account_number) AND UPPER(TRIM(CAST("BankName" AS TEXT))) = UPPER(TRIM(:bank_name))',
        )
    raise ValueError("Unsupported search mode")


# =========================================================
# DATA ACCESS
# =========================================================

def fetch_state_summary() -> pd.DataFrame:
    return read_sql_df(build_union_summary_sql(["State"], []))


def fetch_lga_summary(state_name: str) -> pd.DataFrame:
    return read_sql_df(
        build_union_summary_sql(["State", "Lga"], ["TRIM([State]) = TRIM(:state)"]),
        params={"state": state_name},
    )


def fetch_ward_summary(state_name: str, lga_name: str) -> pd.DataFrame:
    return read_sql_df(
        build_union_summary_sql(
            ["State", "Lga", "Ward"],
            ["TRIM([State]) = TRIM(:state)", "TRIM([Lga]) = TRIM(:lga)"],
        ),
        params={"state": state_name, "lga": lga_name},
    )


def fetch_community_summary(state_name: str, lga_name: str, ward_name: str) -> pd.DataFrame:
    return read_sql_df(
        build_union_summary_sql(
            ["State", "Lga", "Ward", "Community"],
            [
                "TRIM([State]) = TRIM(:state)",
                "TRIM([Lga]) = TRIM(:lga)",
                "TRIM([Ward]) = TRIM(:ward)",
            ],
        ),
        params={"state": state_name, "lga": lga_name, "ward": ward_name},
    )


def fetch_beneficiary_details(state_name: str, lga_name: str, ward_name: str, community_name: str) -> pd.DataFrame:
    return read_sql_df(
        build_beneficiary_detail_sql(),
        params={
            "state": state_name,
            "lga": lga_name,
            "ward": ward_name,
            "community": community_name,
        },
    )


def fetch_beneficiary_search(nin: str | None, account_number: str | None, bank_name: str | None) -> pd.DataFrame:
    nin = (nin or "").strip()
    account_number = (account_number or "").strip()
    bank_name = (bank_name or "").strip()

    if nin:
        return read_sql_df(build_beneficiary_search_sql("nin"), params={"nin": nin})

    if account_number and bank_name:
        return read_sql_df(
            build_beneficiary_search_sql("account"),
            params={"account_number": account_number, "bank_name": bank_name},
        )

    return pd.DataFrame(columns=[
        "State", "Lga", "Ward", "Community", "nidhh", "Gender", "Age", "NINBVN",
        "Telephone", "HAddress", "TrancheStatus", "TotalAmount", "AccountUsed",
        "FirstTrancheAccountName", "FirstTrancheAccountNumber", "FirstTrancheBankName", "FirstTranchePaymentDate",
        "SecondTrancheAccountName", "SecondTrancheAccountNumber", "SecondTrancheBankName", "SecondTranchePaymentDate",
        "ThirdTrancheAccountName", "ThirdTrancheAccountNumber", "ThirdTrancheBankName", "ThirdTranchePaymentDate"
    ])


def fetch_bank_names() -> list[str]:
    global BANK_OPTIONS_CACHE

    if BANK_OPTIONS_CACHE is not None:
        return BANK_OPTIONS_CACHE

    sql = f"""
    SELECT DISTINCT bank_name
    FROM (
        SELECT NULLIF(TRIM(CAST({pg_ident(FIRST_BANK_NAME_COL)} AS TEXT)), '') AS bank_name
        FROM {qtable(SCHEMA, FIRST_TABLE)}
        UNION
        SELECT NULLIF(TRIM(CAST({pg_ident(SECOND_BANK_NAME_COL)} AS TEXT)), '') AS bank_name
        FROM {qtable(SCHEMA, SECOND_TABLE)}
        UNION
        SELECT NULLIF(TRIM(CAST({pg_ident(THIRD_BANK_NAME_COL)} AS TEXT)), '') AS bank_name
        FROM {qtable(SCHEMA, THIRD_TABLE)}
    ) x
    WHERE bank_name IS NOT NULL
    ORDER BY bank_name ASC;
    """
    df = read_sql_df(sql)
    BANK_OPTIONS_CACHE = df["bank_name"].tolist() if not df.empty else []
    return BANK_OPTIONS_CACHE


# =========================================================
# CARD HELPERS
# =========================================================

def make_card(title: str, value: str, subtitle: str = ""):
    return html.Div(
        [
            html.Div(title, className="card-title"),
            html.Div(value, className="card-value"),
            html.Div(subtitle, className="card-subtitle"),
        ],
        className="summary-card",
    )


def build_top_cards(df: pd.DataFrame, subtitle: str):
    return [
        make_card(
            "First Tranche",
            f"{safe_int(df['FirstTrancheCount'].sum()):,}",
            format_currency(df["FirstTrancheAmount"].sum()),
        ),
        make_card(
            "Second Tranche",
            f"{safe_int(df['SecondTrancheCount'].sum()):,}",
            format_currency(df["SecondTrancheAmount"].sum()),
        ),
        make_card(
            "Third Tranche",
            f"{safe_int(df['ThirdTrancheCount'].sum()):,}",
            format_currency(df["ThirdTrancheAmount"].sum()),
        ),
        make_card(
            "Grand Total Amount",
            format_currency(df["TotalAmount"].sum()),
            subtitle,
        ),
    ]


def build_detail_top_cards(df: pd.DataFrame, subtitle: str):
    return [
        make_card("Beneficiaries", f"{len(df):,}", subtitle),
        make_card("Total Amount", format_currency(df["TotalAmount"].sum() if "TotalAmount" in df.columns else 0), subtitle),
    ]


# =========================================================
# UI HELPERS
# =========================================================

def build_clickable_table(df: pd.DataFrame, current_level: str, state_val=None, lga_val=None, ward_val=None):
    link_col = None
    if current_level == "state":
        link_col = "State"
    elif current_level == "lga":
        link_col = "Lga"
    elif current_level == "ward":
        link_col = "Ward"
    elif current_level == "community":
        link_col = "Community"

    columns = list(df.columns)
    header = html.Thead(html.Tr([html.Th(col, className="th-cell") for col in columns]))
    body_rows = []

    for _, row in df.iterrows():
        tds = []
        for col in columns:
            val = row[col]

            if col == link_col:
                if current_level == "state":
                    href = "/?" + urlencode({"state": str(val)})
                elif current_level == "lga":
                    href = "/?" + urlencode({"state": state_val, "lga": str(val)})
                elif current_level == "ward":
                    href = "/?" + urlencode({"state": state_val, "lga": lga_val, "ward": str(val)})
                elif current_level == "community":
                    href = "/?" + urlencode({
                        "state": state_val,
                        "lga": lga_val,
                        "ward": ward_val,
                        "community": str(val),
                        "view": "details",
                    })
                else:
                    href = "#"

                shown = html.A(str(val), href=href, className="link-anchor")
            else:
                if col in ["FirstTrancheCount", "SecondTrancheCount", "ThirdTrancheCount"]:
                    shown = f"{safe_int(val):,}"
                elif col in ["FirstTrancheAmount", "SecondTrancheAmount", "ThirdTrancheAmount", "TotalAmount"]:
                    shown = format_currency(val)
                else:
                    shown = val

            tds.append(html.Td(shown, className="td-cell"))
        body_rows.append(html.Tr(tds))

    return html.Div(
        html.Table([header, html.Tbody(body_rows)], className="data-table summary-table"),
        className="table-wrapper",
    )


def build_detail_table(df: pd.DataFrame):
    columns = list(df.columns)
    header = html.Thead(html.Tr([html.Th(col, className="th-cell") for col in columns]))
    body_rows = []

    for _, row in df.iterrows():
        tds = []
        for col in columns:
            val = row[col]
            shown = format_currency(val) if col == "TotalAmount" else ("" if pd.isna(val) else str(val))
            tds.append(html.Td(shown, className="td-cell"))
        body_rows.append(html.Tr(tds))

    return html.Div(
        html.Table([header, html.Tbody(body_rows)], className="data-table detail-table"),
        className="table-wrapper detail-table-wrapper",
    )


def build_search_panel():
    return html.Div(
        [
            html.Div("Beneficiary Search", className="search-title"),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label("NIN"),
                            dcc.Input(
                                id="search-nin",
                                type="text",
                                placeholder="Search by NIN",
                                className="search-input",
                            ),
                        ],
                        className="search-field",
                    ),
                    html.Div(
                        [
                            html.Label("Account Number"),
                            dcc.Input(
                                id="search-account-number",
                                type="text",
                                placeholder="Search by Account Number",
                                className="search-input",
                            ),
                        ],
                        className="search-field",
                    ),
                    html.Div(
                        [
                            html.Label("Bank Name"),
                            dcc.Dropdown(
                                id="search-bank-name",
                                options=[],
                                placeholder="Click Load Banks first",
                                className="search-dropdown",
                                clearable=True,
                            ),
                        ],
                        className="search-field",
                    ),
                    html.Div(
                        [
                            html.Button("Load Banks", id="load-banks-btn", n_clicks=0, className="clear-button"),
                            html.Button("Search", id="search-btn", n_clicks=0, className="search-button"),
                            html.A("Clear", href="/", className="clear-button"),
                        ],
                        className="search-actions",
                    ),
                ],
                className="search-grid",
            ),
            dcc.Loading(
                id="search-loading",
                type="circle",
                delay_show=250,
                children=html.Div(id="search-status", className="search-help"),
            ),
        ],
        className="search-panel",
    )


# =========================================================
# DASH APP
# =========================================================

app = Dash(__name__)
app.title = APP_TITLE

app.index_string = """
<!DOCTYPE html>
<html>
<head>
    {%metas%}
    <title>{%title%}</title>
    {%favicon%}
    {%css%}
    <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, viewport-fit=cover">
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0; padding: 0;
            font-family: "Segoe UI", Arial, sans-serif;
            background: #f8fafc; color: #0f172a;
        }
        .app-shell { min-height: 100vh; padding: 18px; }
        .hero {
            padding: 22px 24px;
            background: linear-gradient(135deg, #0f172a, #1d4ed8);
            border-radius: 24px;
            box-shadow: 0 12px 30px rgba(0,0,0,0.15);
            margin-bottom: 18px;
        }
        .hero h1 { margin: 0; color: white; font-size: 32px; }

        .search-panel {
            background: white;
            border-radius: 20px;
            padding: 16px;
            box-shadow: 0 8px 24px rgba(15,23,42,0.08);
            border: 1px solid #e2e8f0;
            margin-bottom: 16px;
        }
        .search-title {
            font-size: 18px;
            font-weight: 700;
            margin-bottom: 12px;
            color: #0f172a;
        }
        .search-grid {
            display: grid;
            grid-template-columns: 1.2fr 1.2fr 1.2fr auto;
            gap: 12px;
            align-items: end;
        }
        .search-field label {
            display: block;
            font-size: 12px;
            color: #64748b;
            margin-bottom: 6px;
        }
        .search-input {
            width: 100%;
            padding: 10px 12px;
            border: 1px solid #cbd5e1;
            border-radius: 12px;
            font-size: 14px;
            background: white;
        }
        .search-dropdown {
            font-size: 14px;
        }
        .search-actions {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }
        .search-button, .clear-button {
            padding: 10px 16px;
            border-radius: 12px;
            font-size: 14px;
            text-decoration: none;
            cursor: pointer;
        }
        .search-button {
            border: none;
            background: #1d4ed8;
            color: white;
            font-weight: 600;
        }
        .clear-button {
            border: 1px solid #cbd5e1;
            background: white;
            color: #0f172a;
        }
        .search-help {
            margin-top: 10px;
            font-size: 12px;
            color: #64748b;
            min-height: 18px;
        }

        .breadcrumb {
            margin-bottom: 14px;
            font-size: 15px;
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 6px;
        }
        .crumb-btn {
            color: #2563eb;
            font-weight: 600;
            cursor: pointer;
            font-size: 15px;
            text-decoration: none;
        }
        .crumb-btn:hover { text-decoration: underline; }
        .crumb-sep { color: #94a3b8; }
        .crumb-current { color: #0f172a; font-weight: 700; }

        .cards-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 16px;
            margin-bottom: 20px;
        }
        .summary-card {
            background: white;
            border-radius: 18px;
            padding: 18px 20px;
            box-shadow: 0 8px 24px rgba(15,23,42,0.08);
            border: 1px solid #e2e8f0;
            min-width: 0;
        }
        .card-title { font-size: 14px; color: #64748b; margin-bottom: 6px; }
        .card-value { font-size: 26px; font-weight: 700; color: #0f172a; word-break: break-word; }
        .card-subtitle { font-size: 12px; color: #94a3b8; margin-top: 4px; }

        .panel {
            background: white;
            border-radius: 22px;
            padding: 18px;
            box-shadow: 0 8px 24px rgba(15,23,42,0.08);
            border: 1px solid #e2e8f0;
            overflow: visible;
            position: relative;
        }
        .panel-title { font-size: 20px; font-weight: 700; margin-bottom: 12px; color: #0f172a; }

        .table-wrapper {
            width: 100%;
            overflow-x: auto;
            overflow-y: auto;
            max-height: 70vh;
            -webkit-overflow-scrolling: touch;
            border: 1px solid #e2e8f0;
            border-radius: 14px;
        }
        .detail-table-wrapper { max-height: 72vh; }

        .data-table {
            width: max-content;
            min-width: 100%;
            border-collapse: collapse;
            background: white;
        }
        .summary-table { min-width: 820px; }
        .detail-table { min-width: 3200px; }

        .th-cell {
            background: #0f172a; color: white; padding: 12px 10px; text-align: left;
            border: 1px solid #1e293b; font-size: 13px; position: sticky; top: 0; z-index: 2;
            white-space: nowrap;
        }
        .td-cell {
            padding: 10px; border: 1px solid #e2e8f0;
            font-size: 13px; color: #0f172a; vertical-align: top; background: white;
            white-space: nowrap;
        }
        .link-anchor {
            color: #2563eb;
            cursor: pointer;
            font-weight: 600;
            font-size: 13px;
            text-decoration: none;
        }
        .link-anchor:hover { text-decoration: underline; }

        .error-panel {
            background: #fff7ed;
            border: 1px solid #fdba74;
            border-radius: 14px;
            padding: 16px;
        }
        .error-title {
            font-size: 18px;
            font-weight: 700;
            color: #9a3412;
            margin-bottom: 10px;
        }
        .error-pre {
            white-space: pre-wrap;
            word-break: break-word;
            font-family: Consolas, monospace;
            font-size: 13px;
            color: #7c2d12;
            margin: 0;
        }

        @media (max-width: 1200px) {
            .cards-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .search-grid { grid-template-columns: 1fr 1fr; }
            .detail-table-wrapper { max-height: 68vh; }
        }
        @media (max-width: 768px) {
            .app-shell { padding: 12px; }
            .hero { padding: 18px 16px; border-radius: 18px; }
            .hero h1 { font-size: 24px; line-height: 1.2; }
            .cards-grid { grid-template-columns: 1fr; gap: 12px; }
            .search-grid { grid-template-columns: 1fr; }
            .search-actions { justify-content: flex-start; }
            .summary-card { padding: 16px; }
            .card-value { font-size: 22px; }
            .panel { padding: 14px; border-radius: 18px; }
            .panel-title { font-size: 18px; }
            .table-wrapper { max-height: 65vh; }
            .th-cell, .td-cell, .link-anchor, .crumb-btn, .search-input, .search-button, .clear-button {
                font-size: 12px;
            }
        }
    </style>
</head>
<body>
    {%app_entry%}
    <footer>
        {%config%}
        {%scripts%}
        {%renderer%}
    </footer>
</body>
</html>
"""

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=True),
        html.Div(
            [
                html.Div([html.H1(APP_TITLE)], className="hero"),
                build_search_panel(),
                dcc.Loading(
                    id="page-loading",
                    type="circle",
                    delay_show=250,
                    children=html.Div(
                        [
                            html.Div(id="breadcrumb", className="breadcrumb"),
                            html.Div(id="kpi-cards", className="cards-grid"),
                            html.Div(
                                [
                                    html.Div(id="table-title", className="panel-title"),
                                    html.Div(id="detail-table"),
                                ],
                                className="panel",
                            ),
                        ]
                    ),
                ),
            ]
        ),
    ],
    className="app-shell",
)


# =========================================================
# SEARCH CALLBACKS
# =========================================================

@app.callback(
    Output("search-bank-name", "options"),
    Output("search-status", "children"),
    Input("load-banks-btn", "n_clicks"),
    prevent_initial_call=True,
)
def load_bank_dropdown(n_clicks):
    try:
        banks = fetch_bank_names()
        options = [{"label": bank, "value": bank} for bank in banks]
        return options, f"{len(options):,} banks loaded."
    except Exception as exc:
        return [], f"Failed to load banks: {exc}"


@app.callback(
    Output("url", "search", allow_duplicate=True),
    Output("search-status", "children", allow_duplicate=True),
    Input("search-btn", "n_clicks"),
    State("search-nin", "value"),
    State("search-account-number", "value"),
    State("search-bank-name", "value"),
    prevent_initial_call=True,
)
def trigger_search(n_clicks, nin, account_number, bank_name):
    nin = (nin or "").strip()
    account_number = (account_number or "").strip()
    bank_name = (bank_name or "").strip()

    if nin:
        return "?" + urlencode({"view": "search", "nin": nin}), "Searching by NIN..."

    if account_number and bank_name:
        return "?" + urlencode({
            "view": "search",
            "account_number": account_number,
            "bank_name": bank_name,
        }), "Searching by Account Number and Bank Name..."

    return "?view=search", "Enter NIN, or Account Number with Bank Name."


# =========================================================
# PAGE CALLBACK
# =========================================================

@app.callback(
    Output("breadcrumb", "children"),
    Output("kpi-cards", "children"),
    Output("table-title", "children"),
    Output("detail-table", "children"),
    Input("url", "search"),
)
def render_page(search):
    params = parse_qs((search or "").lstrip("?"))
    state_val = params.get("state", [None])[0]
    lga_val = params.get("lga", [None])[0]
    ward_val = params.get("ward", [None])[0]
    community_val = params.get("community", [None])[0]
    view_val = params.get("view", [None])[0]
    nin_val = params.get("nin", [""])[0]
    account_val = params.get("account_number", [""])[0]
    bank_val = params.get("bank_name", [""])[0]

    breadcrumb_parts = [html.A("National", href="/", className="crumb-btn")]

    if view_val == "search":
        breadcrumb_parts += [
            html.Span(" / ", className="crumb-sep"),
            html.Span("Search Results", className="crumb-current")
        ]
    else:
        if state_val:
            breadcrumb_parts += [
                html.Span(" / ", className="crumb-sep"),
                html.A(state_val, href="/?" + urlencode({"state": state_val}), className="crumb-btn")
            ]
        if lga_val:
            breadcrumb_parts += [
                html.Span(" / ", className="crumb-sep"),
                html.A(lga_val, href="/?" + urlencode({"state": state_val, "lga": lga_val}), className="crumb-btn")
            ]
        if ward_val:
            breadcrumb_parts += [
                html.Span(" / ", className="crumb-sep"),
                html.A(ward_val, href="/?" + urlencode({"state": state_val, "lga": lga_val, "ward": ward_val}), className="crumb-btn")
            ]
        if community_val:
            breadcrumb_parts += [
                html.Span(" / ", className="crumb-sep"),
                html.A(
                    community_val,
                    href="/?" + urlencode({
                        "state": state_val,
                        "lga": lga_val,
                        "ward": ward_val,
                        "community": community_val
                    }),
                    className="crumb-btn"
                )
            ]
        if view_val == "details":
            breadcrumb_parts += [
                html.Span(" / ", className="crumb-sep"),
                html.Span("Beneficiaries", className="crumb-current")
            ]

    try:
        if view_val == "search":
            if not (nin_val.strip() or (account_val.strip() and bank_val.strip())):
                return (
                    breadcrumb_parts,
                    [],
                    "Search Results",
                    html.Div("Enter NIN, or Account Number with Bank Name, then click Search.", className="search-help")
                )

            df = fetch_beneficiary_search(nin_val, account_val, bank_val)

            return (
                breadcrumb_parts,
                build_detail_top_cards(df, "Search Results"),
                "Search Results",
                build_detail_table(df[[
                    "State", "Lga", "Ward", "Community", "nidhh", "Gender", "Age", "NINBVN",
                    "Telephone", "HAddress", "TrancheStatus", "TotalAmount", "AccountUsed",
                    "FirstTrancheAccountName", "FirstTrancheAccountNumber", "FirstTrancheBankName", "FirstTranchePaymentDate",
                    "SecondTrancheAccountName", "SecondTrancheAccountNumber", "SecondTrancheBankName", "SecondTranchePaymentDate",
                    "ThirdTrancheAccountName", "ThirdTrancheAccountNumber", "ThirdTrancheBankName", "ThirdTranchePaymentDate"
                ]]) if not df.empty else html.Div("No beneficiary found for the supplied search.", className="search-help")
            )

        if view_val == "details" and state_val and lga_val and ward_val and community_val:
            df = fetch_beneficiary_details(state_val, lga_val, ward_val, community_val)
            return (
                breadcrumb_parts,
                build_detail_top_cards(df, community_val),
                f"Beneficiary Details — {community_val}",
                build_detail_table(df[[
                    "State", "Lga", "Ward", "Community", "nidhh", "Gender", "Age", "NINBVN",
                    "Telephone", "HAddress", "TrancheStatus", "TotalAmount", "AccountUsed",
                    "FirstTrancheAccountName", "FirstTrancheAccountNumber", "FirstTrancheBankName", "FirstTranchePaymentDate",
                    "SecondTrancheAccountName", "SecondTrancheAccountNumber", "SecondTrancheBankName", "SecondTranchePaymentDate",
                    "ThirdTrancheAccountName", "ThirdTrancheAccountNumber", "ThirdTrancheBankName", "ThirdTranchePaymentDate"
                ]])
            )

        if not state_val:
            df = fetch_state_summary().sort_values(["State"], ascending=[True], na_position="last")
            return (
                breadcrumb_parts,
                build_top_cards(df, "National"),
                "State Summary",
                build_clickable_table(df[[
                    "State",
                    "FirstTrancheCount", "FirstTrancheAmount",
                    "SecondTrancheCount", "SecondTrancheAmount",
                    "ThirdTrancheCount", "ThirdTrancheAmount",
                    "TotalAmount",
                ]], "state")
            )

        if state_val and not lga_val:
            df = fetch_lga_summary(state_val).sort_values(["State", "Lga"], ascending=[True, True], na_position="last")
            return (
                breadcrumb_parts,
                build_top_cards(df, state_val),
                f"LGA Summary — {state_val}",
                build_clickable_table(df[[
                    "Lga",
                    "FirstTrancheCount", "FirstTrancheAmount",
                    "SecondTrancheCount", "SecondTrancheAmount",
                    "ThirdTrancheCount", "ThirdTrancheAmount",
                    "TotalAmount",
                ]], "lga", state_val=state_val)
            )

        if state_val and lga_val and not ward_val:
            df = fetch_ward_summary(state_val, lga_val).sort_values(["State", "Lga", "Ward"], ascending=[True, True, True], na_position="last")
            return (
                breadcrumb_parts,
                build_top_cards(df, lga_val),
                f"Ward Summary — {state_val} / {lga_val}",
                build_clickable_table(df[[
                    "Ward",
                    "FirstTrancheCount", "FirstTrancheAmount",
                    "SecondTrancheCount", "SecondTrancheAmount",
                    "ThirdTrancheCount", "ThirdTrancheAmount",
                    "TotalAmount",
                ]], "ward", state_val=state_val, lga_val=lga_val)
            )

        if state_val and lga_val and ward_val and not community_val:
            df = fetch_community_summary(state_val, lga_val, ward_val).sort_values(
                ["State", "Lga", "Ward", "Community"],
                ascending=[True, True, True, True],
                na_position="last"
            )
            return (
                breadcrumb_parts,
                build_top_cards(df, ward_val),
                f"Community Summary — {state_val} / {lga_val} / {ward_val}",
                build_clickable_table(df[[
                    "Community",
                    "FirstTrancheCount", "FirstTrancheAmount",
                    "SecondTrancheCount", "SecondTrancheAmount",
                    "ThirdTrancheCount", "ThirdTrancheAmount",
                    "TotalAmount",
                ]], "community", state_val=state_val, lga_val=lga_val, ward_val=ward_val)
            )

        if state_val and lga_val and ward_val and community_val:
            df = fetch_beneficiary_details(state_val, lga_val, ward_val, community_val)
            return (
                breadcrumb_parts,
                build_detail_top_cards(df, community_val),
                f"Beneficiary Details — {community_val}",
                build_detail_table(df[[
                    "State", "Lga", "Ward", "Community", "nidhh", "Gender", "Age", "NINBVN",
                    "Telephone", "HAddress", "TrancheStatus", "TotalAmount", "AccountUsed",
                    "FirstTrancheAccountName", "FirstTrancheAccountNumber", "FirstTrancheBankName", "FirstTranchePaymentDate",
                    "SecondTrancheAccountName", "SecondTrancheAccountNumber", "SecondTrancheBankName", "SecondTranchePaymentDate",
                    "ThirdTrancheAccountName", "ThirdTrancheAccountNumber", "ThirdTrancheBankName", "ThirdTranchePaymentDate"
                ]])
            )

        raise RuntimeError("Invalid navigation state.")
    except Exception as exc:
        return (
            breadcrumb_parts,
            [],
            "Application Error",
            make_error_panel("Failed to load this page", exc)
        )


# if __name__ == "__main__":
#     print(f"Starting {APP_TITLE} on http://0.0.0.0:{WEB_PORT}")
#     serve(app.server, host=WEB_HOST, port=WEB_PORT, threads=8)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    serve(app.server, host="0.0.0.0", port=port, threads=8)