import os
import asyncio
import httpx
import duckdb
import logging
from typing import Any
from datetime import datetime
from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _parse_accepted(raw: str, fallback: str) -> datetime:
    s = raw or fallback
    try:
        return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S") if len(s) > 10 else datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        return datetime.strptime(fallback[:10], "%Y-%m-%d")


def _f(d: dict, key: str) -> float:
    v = d.get(key)
    return float(v) if v is not None else 0.0


class FMPIngester:
    def __init__(self):
        self.api_key = settings.FMP_API_KEY
        self.base_url = "https://financialmodelingprep.com/stable"
        self.semaphore = asyncio.Semaphore(4)
        self.client = httpx.AsyncClient(timeout=60.0)

    async def _fetch(self, endpoint: str, params: dict) -> Any:
        params['apikey'] = self.api_key
        url = f"{self.base_url}/{endpoint}"
        for i in range(5):
            async with self.semaphore:
                try:
                    resp = await self.client.get(url, params=params)
                    if resp.status_code == 429:
                        await asyncio.sleep(2 ** (i + 1))
                        continue
                    if resp.status_code in (402, 403, 404):
                        return []
                    resp.raise_for_status()
                    data = resp.json()
                    return data if isinstance(data, list) else [data]
                except Exception as e:
                    if i == 4:
                        logger.error(f"Failed {url}: {e}")
                        return []
                    await asyncio.sleep(2 ** i)
        return []

    async def get_tickers(self, limit: int = 100) -> list[str]:
        # Vector 1 expansion: widened to $5-$30 / $50M-$5B (was $20 / $2B). NOTE: re-running
        # ingest.py is DESTRUCTIVE (CREATE OR REPLACE wipes delisted + expansion names). Use
        # ingest_expansion.py for incremental, append-only universe growth instead.
        data = await self._fetch("company-screener", {
            "marketCapMoreThan": 50_000_000,
            "marketCapLowerThan": 5_000_000_000,
            "priceLowerThan": 30,
            "isEtf": "false",
            "isActivelyTrading": "true",
            "limit": limit,
        })
        return [item['symbol'] for item in data if 'symbol' in item]

    async def get_historical_prices(self, ticker: str) -> list[tuple]:
        data = await self._fetch("historical-price-eod/full", {"symbol": ticker})
        if not data:
            return []
        records = data[0]['historical'] if ('historical' in data[0]) else data
        rows = []
        for r in records:
            if 'date' not in r:
                continue
            try:
                rows.append((
                    ticker, r['date'][:10],
                    _f(r, 'open'), _f(r, 'close'),
                    r.get('adjClose') or _f(r, 'close'),
                    _f(r, 'volume'),
                ))
            except Exception:
                pass
        return rows

    async def get_all_fundamentals(self, ticker: str) -> tuple[list, list, list, list]:
        """Returns (income_rows, balance_rows, cashflow_rows, filing_rows)."""
        q = settings.HISTORY_QUARTERS
        income_data, balance_data, cashflow_data = await asyncio.gather(
            self._fetch("income-statement",       {"symbol": ticker, "period": "quarter", "limit": q}),
            self._fetch("balance-sheet-statement", {"symbol": ticker, "period": "quarter", "limit": q}),
            self._fetch("cash-flow-statement",     {"symbol": ticker, "period": "quarter", "limit": q}),
        )

        income_map   = {r['date']: r for r in income_data   if 'date' in r}
        balance_map  = {r['date']: r for r in balance_data  if 'date' in r}
        cashflow_map = {r['date']: r for r in cashflow_data if 'date' in r}

        income_rows, balance_rows, cashflow_rows, filing_rows = [], [], [], []

        for date, inc in income_map.items():
            acc_dt = _parse_accepted(inc.get('acceptedDate', inc.get('fillingDate', date)), date)
            acc_str = acc_dt.strftime("%Y-%m-%d %H:%M:%S")
            period = inc.get('period', '')
            fy = inc.get('fiscalYear', '')

            income_rows.append((
                ticker, date, acc_str, period, fy,
                _f(inc, 'revenue'), _f(inc, 'costOfRevenue'), _f(inc, 'grossProfit'),
                _f(inc, 'researchAndDevelopmentExpenses'),
                _f(inc, 'sellingGeneralAndAdministrativeExpenses'),
                _f(inc, 'operatingExpenses'), _f(inc, 'operatingIncome'),
                _f(inc, 'ebitda'), _f(inc, 'ebit'),
                _f(inc, 'interestIncome'), _f(inc, 'interestExpense'),
                _f(inc, 'incomeBeforeTax'), _f(inc, 'incomeTaxExpense'),
                _f(inc, 'netIncome'), _f(inc, 'eps'), _f(inc, 'epsDiluted'),
                _f(inc, 'weightedAverageShsOut'), _f(inc, 'weightedAverageShsOutDil'),
                _f(inc, 'depreciationAndAmortization'),
            ))

        for date, bal in balance_map.items():
            acc_dt = _parse_accepted(bal.get('acceptedDate', bal.get('fillingDate', date)), date)
            acc_str = acc_dt.strftime("%Y-%m-%d %H:%M:%S")
            period = bal.get('period', '')
            fy = bal.get('fiscalYear', '')

            balance_rows.append((
                ticker, date, acc_str, period, fy,
                _f(bal, 'cashAndCashEquivalents'), _f(bal, 'shortTermInvestments'),
                _f(bal, 'netReceivables'), _f(bal, 'inventory'),
                _f(bal, 'otherCurrentAssets'), _f(bal, 'totalCurrentAssets'),
                _f(bal, 'propertyPlantEquipmentNet'), _f(bal, 'goodwill'),
                _f(bal, 'intangibleAssets'), _f(bal, 'totalNonCurrentAssets'),
                _f(bal, 'totalAssets'), _f(bal, 'accountPayables'),
                _f(bal, 'shortTermDebt'), _f(bal, 'otherCurrentLiabilities'),
                _f(bal, 'totalCurrentLiabilities'), _f(bal, 'longTermDebt'),
                _f(bal, 'totalNonCurrentLiabilities'), _f(bal, 'totalLiabilities'),
                _f(bal, 'retainedEarnings'), _f(bal, 'totalStockholdersEquity'),
                _f(bal, 'totalEquity'), _f(bal, 'totalDebt'), _f(bal, 'netDebt'),
            ))

        for date, cf in cashflow_map.items():
            acc_dt = _parse_accepted(cf.get('acceptedDate', cf.get('fillingDate', date)), date)
            acc_str = acc_dt.strftime("%Y-%m-%d %H:%M:%S")
            period = cf.get('period', '')
            fy = cf.get('fiscalYear', '')

            cashflow_rows.append((
                ticker, date, acc_str, period, fy,
                _f(cf, 'netIncome'), _f(cf, 'depreciationAndAmortization'),
                _f(cf, 'stockBasedCompensation'), _f(cf, 'changeInWorkingCapital'),
                _f(cf, 'netCashProvidedByOperatingActivities'),
                _f(cf, 'capitalExpenditure'), _f(cf, 'freeCashFlow'),
                _f(cf, 'netStockIssuance'), _f(cf, 'commonStockIssuance'),
                _f(cf, 'commonStockRepurchased'), _f(cf, 'netDebtIssuance'),
                _f(cf, 'netCashProvidedByInvestingActivities'),
                _f(cf, 'netCashProvidedByFinancingActivities'),
                _f(cf, 'netChangeInCash'), _f(cf, 'cashAtEndOfPeriod'),
            ))

        # Backward-compat financial_filings: join income + balance on date
        for date, inc in income_map.items():
            if date in balance_map:
                bal = balance_map[date]
                acc_dt = _parse_accepted(inc.get('acceptedDate', inc.get('fillingDate', date)), date)
                filing_rows.append((
                    ticker,
                    acc_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    _f(inc, 'grossProfit'),
                    _f(bal, 'totalAssets'),
                ))

        return income_rows, balance_rows, cashflow_rows, filing_rows

    async def get_earnings(self, ticker: str) -> list[tuple]:
        data = await self._fetch("earnings", {"symbol": ticker, "limit": settings.HISTORY_QUARTERS})
        rows = []
        for r in data:
            if 'date' not in r:
                continue
            rows.append((
                ticker, r['date'][:10],
                r.get('epsActual') or 0.0, r.get('epsEstimated') or 0.0,
                r.get('revenueActual') or 0.0, r.get('revenueEstimated') or 0.0,
            ))
        return rows

    async def get_shares_float(self, ticker: str) -> list[tuple]:
        data = await self._fetch("shares-float", {"symbol": ticker})
        rows = []
        for r in data:
            if 'date' not in r:
                continue
            rows.append((
                ticker, r['date'][:10],
                r.get('freeFloat') or 0.0,
                r.get('floatShares') or 0.0,
                r.get('outstandingShares') or 0.0,
            ))
        return rows

    async def get_insider_trades(self, ticker: str) -> list[tuple]:
        data = await self._fetch("insider-trading/search", {"symbol": ticker, "limit": 200})
        rows = []
        for r in data:
            fd = (r.get('filingDate') or r.get('transactionDate') or '')[:10]
            td = (r.get('transactionDate') or fd)[:10]
            if not fd:
                continue
            rows.append((
                ticker, fd, td,
                r.get('acquisitionOrDisposition', ''),
                r.get('transactionType', ''),
                float(r.get('securitiesTransacted') or 0),
                float(r.get('price') or 0),
                r.get('reportingName', ''),
                r.get('typeOfOwner', ''),
            ))
        return rows

    async def get_short_interest(self, ticker: str) -> list[tuple]:
        data = await self._fetch("short-of-float", {"symbol": ticker})
        rows = []
        for r in data:
            d = (r.get('date') or '')[:10]
            if not d:
                continue
            rows.append((
                ticker, d,
                float(r.get('shortPercent') or 0),
                float(r.get('sharesShort') or 0),
                float(r.get('floatShares') or 0),
            ))
        return rows

    async def get_profile(self, ticker: str) -> tuple | None:
        data = await self._fetch("profile", {"symbol": ticker})
        if not data:
            return None
        r = data[0]
        return (
            ticker,
            r.get('sector') or '',
            r.get('industry') or '',
            r.get('beta') or 0.0,
            r.get('averageVolume') or 0.0,
            r.get('ipoDate') or '',
            r.get('fullTimeEmployees') or 0,
        )

    async def close(self):
        await self.client.aclose()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = [
    """CREATE OR REPLACE TABLE historical_prices (
        ticker VARCHAR, date DATE,
        open DOUBLE, close DOUBLE, adj_close DOUBLE, volume DOUBLE
    )""",
    """CREATE OR REPLACE TABLE income_statements (
        ticker VARCHAR, date DATE, accepted_date TIMESTAMP,
        period VARCHAR, fiscal_year VARCHAR,
        revenue DOUBLE, cost_of_revenue DOUBLE, gross_profit DOUBLE,
        research_and_development DOUBLE, sga_expenses DOUBLE,
        operating_expenses DOUBLE, operating_income DOUBLE,
        ebitda DOUBLE, ebit DOUBLE,
        interest_income DOUBLE, interest_expense DOUBLE,
        income_before_tax DOUBLE, income_tax_expense DOUBLE,
        net_income DOUBLE, eps DOUBLE, eps_diluted DOUBLE,
        weighted_avg_shares DOUBLE, weighted_avg_shares_diluted DOUBLE,
        depreciation_amortization DOUBLE
    )""",
    """CREATE OR REPLACE TABLE balance_sheets (
        ticker VARCHAR, date DATE, accepted_date TIMESTAMP,
        period VARCHAR, fiscal_year VARCHAR,
        cash_and_equivalents DOUBLE, short_term_investments DOUBLE,
        net_receivables DOUBLE, inventory DOUBLE,
        other_current_assets DOUBLE, total_current_assets DOUBLE,
        property_plant_equipment DOUBLE, goodwill DOUBLE,
        intangible_assets DOUBLE, total_non_current_assets DOUBLE,
        total_assets DOUBLE, accounts_payable DOUBLE,
        short_term_debt DOUBLE, other_current_liabilities DOUBLE,
        total_current_liabilities DOUBLE, long_term_debt DOUBLE,
        total_non_current_liabilities DOUBLE, total_liabilities DOUBLE,
        retained_earnings DOUBLE, total_stockholders_equity DOUBLE,
        total_equity DOUBLE, total_debt DOUBLE, net_debt DOUBLE
    )""",
    """CREATE OR REPLACE TABLE cash_flow_statements (
        ticker VARCHAR, date DATE, accepted_date TIMESTAMP,
        period VARCHAR, fiscal_year VARCHAR,
        net_income DOUBLE, depreciation_amortization DOUBLE,
        stock_based_compensation DOUBLE, change_in_working_capital DOUBLE,
        operating_cash_flow DOUBLE, capital_expenditure DOUBLE,
        free_cash_flow DOUBLE, net_stock_issuance DOUBLE,
        common_stock_issuance DOUBLE, common_stock_repurchased DOUBLE,
        net_debt_issuance DOUBLE, net_cash_investing DOUBLE,
        net_cash_financing DOUBLE, net_change_in_cash DOUBLE,
        cash_at_end_of_period DOUBLE
    )""",
    """CREATE OR REPLACE TABLE earnings_history (
        ticker VARCHAR, date DATE,
        eps_actual DOUBLE, eps_estimated DOUBLE,
        revenue_actual DOUBLE, revenue_estimated DOUBLE
    )""",
    """CREATE OR REPLACE TABLE shares_float_history (
        ticker VARCHAR, date DATE,
        free_float DOUBLE, float_shares DOUBLE, outstanding_shares DOUBLE
    )""",
    """CREATE OR REPLACE TABLE ticker_metadata (
        ticker VARCHAR, sector VARCHAR, industry VARCHAR,
        beta DOUBLE, average_volume DOUBLE, ipo_date VARCHAR,
        full_time_employees INTEGER
    )""",
    """CREATE OR REPLACE TABLE financial_filings (
        ticker VARCHAR, accepted_date TIMESTAMP,
        gross_profit DOUBLE, total_assets DOUBLE
    )""",
    """CREATE OR REPLACE TABLE insider_trades (
        ticker VARCHAR, filing_date DATE, transaction_date DATE,
        acquisition_or_disposition VARCHAR, transaction_type VARCHAR,
        shares_transacted DOUBLE, price_per_share DOUBLE,
        reporting_name VARCHAR, type_of_owner VARCHAR
    )""",
    """CREATE OR REPLACE TABLE short_interest (
        ticker VARCHAR, date DATE,
        short_percent DOUBLE, shares_short DOUBLE, float_shares DOUBLE
    )""",
]

CLEAR_SQL = [
    "DELETE FROM historical_prices",
    "DELETE FROM income_statements",
    "DELETE FROM balance_sheets",
    "DELETE FROM cash_flow_statements",
    "DELETE FROM earnings_history",
    "DELETE FROM shares_float_history",
    "DELETE FROM ticker_metadata",
    "DELETE FROM financial_filings",
    "DELETE FROM insider_trades",
    "DELETE FROM short_interest",
]


async def main():
    logger.info("Initializing DuckDB...")
    conn = duckdb.connect(settings.DUCKDB_PATH)

    # GUARD: ingest.py is a full destructive rebuild (CREATE OR REPLACE). If delisted/expansion
    # names have been appended, a naive re-run would WIPE them and re-introduce survivorship bias.
    for t in ('delisted_universe', 'expansion_universe'):
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            n = 0
        if n and os.getenv('NM_FORCE_REBUILD') != '1':
            raise SystemExit(
                f"ABORT: {t} has {n} rows — a full ingest.py rebuild would DELETE the delisted/"
                f"expansion universe (survivorship bias). Use ingest_expansion.py to add names "
                f"incrementally, or set NM_FORCE_REBUILD=1 to override and rebuild from scratch.")

    for sql in SCHEMA_SQL:
        conn.execute(sql)

    ingester = FMPIngester()
    try:
        logger.info("Fetching target universe (MAX_PRICE=$%.2f)...", settings.MAX_PRICE)
        tickers = await ingester.get_tickers(settings.UNIVERSE_SIZE)
        logger.info(f"Targeting {len(tickers)} tickers.")

        for idx, ticker in enumerate(tickers):
            logger.info(f"[{idx+1}/{len(tickers)}] Ingesting {ticker}...")

            prices, fundamentals, earnings, float_data, profile, insider, short_int = \
                await asyncio.gather(
                    ingester.get_historical_prices(ticker),
                    ingester.get_all_fundamentals(ticker),
                    ingester.get_earnings(ticker),
                    ingester.get_shares_float(ticker),
                    ingester.get_profile(ticker),
                    ingester.get_insider_trades(ticker),
                    ingester.get_short_interest(ticker),
                )

            income_rows, balance_rows, cashflow_rows, filing_rows = fundamentals

            if prices:
                conn.executemany("INSERT INTO historical_prices VALUES (?,?,?,?,?,?)", prices)
            if income_rows:
                conn.executemany("INSERT INTO income_statements VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", income_rows)
            if balance_rows:
                conn.executemany("INSERT INTO balance_sheets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", balance_rows)
            if cashflow_rows:
                conn.executemany("INSERT INTO cash_flow_statements VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", cashflow_rows)
            if filing_rows:
                conn.executemany("INSERT INTO financial_filings VALUES (?,?,?,?)", filing_rows)
            if earnings:
                conn.executemany("INSERT INTO earnings_history VALUES (?,?,?,?,?,?)", earnings)
            if float_data:
                conn.executemany("INSERT INTO shares_float_history VALUES (?,?,?,?,?)", float_data)
            if profile:
                conn.execute("INSERT INTO ticker_metadata VALUES (?,?,?,?,?,?,?)", list(profile))
            if insider:
                conn.executemany("INSERT INTO insider_trades VALUES (?,?,?,?,?,?,?,?,?)", insider)
            if short_int:
                conn.executemany("INSERT INTO short_interest VALUES (?,?,?,?,?)", short_int)

        logger.info("Ingestion complete!")
    finally:
        await ingester.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
