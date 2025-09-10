"""
Oracle Database Data Processor

This module provides functions to fetch and process ticketing data from Oracle database,
including CBR (Caller Can Be Reached) information processing with parallel execution.

Author: Generated from provided script
Dependencies: pandas, numpy, oracledb, concurrent.futures
"""

import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
import logging

try:
    import oracledb
    ORACLEDB_AVAILABLE = True
except ImportError:
    ORACLEDB_AVAILABLE = False
    oracledb = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def batch_list(lst: List, batch_size: int = 900) -> List:
    """
    Split a list into batches of specified size.
    
    Args:
        lst: List to be batched
        batch_size: Size of each batch (default: 900)
        
    Yields:
        List batches of specified size
    """
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]


def create_oracle_connection(user: str, password: str, dsn: str, 
                           instant_client_path: Optional[str] = None) -> Optional[object]:
    """
    Create Oracle database connection.
    
    Args:
        user: Database username
        password: Database password
        dsn: Data Source Name
        instant_client_path: Path to Oracle instant client (optional)
        
    Returns:
        Oracle database connection object or None if oracledb not available
        
    Raises:
        Exception: If connection fails or oracledb not available
    """
    if not ORACLEDB_AVAILABLE:
        raise ImportError("oracledb module is not available. Please install it with: pip install oracledb")
    
    try:
        if instant_client_path:
            oracledb.init_oracle_client(lib_dir=instant_client_path)
        
        conn = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn
        )
        logger.info("Successfully connected to Oracle database")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Oracle database: {e}")
        raise


def fetch_main_data(conn: object, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch main ticketing data with customer product information.
    
    Args:
        conn: Oracle database connection
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        
    Returns:
        DataFrame with main ticketing data
    """
    query_combined = f"""
    SELECT 
        ext.RXPS_SESSION_AI,
        cust.CKTID_OR_TN,
        cust.PRODUCT_TYPE,
        cust.PRODUCT_DESC,
        ext.TICKETING_JOB_AI
    FROM DWODSORA.TKT_CREATED_EXTERNAL_TICKET ext
    JOIN DWODSORA.DOC_RXPS_CUSTOMER_PRODUCT cust
        ON ext.RXPS_SESSION_AI = cust.RXPS_SESSION_AI
    WHERE ext.RXPS_SESSION_AI IS NOT NULL
    AND ext.META_UPD_DTTM BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') AND TO_DATE('{end_date}', 'YYYY-MM-DD')
    """
    
    logger.info("Fetching main ticketing data...")
    final_df = pd.read_sql(query_combined, conn)
    final_df_clean = final_df.drop_duplicates()
    
    logger.info(f"Fetched {len(final_df_clean)} unique records")
    return final_df_clean


def group_by_session(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group data by RXPS_SESSION_AI and aggregate product information.
    
    Args:
        df: Input DataFrame with ticketing data
        
    Returns:
        DataFrame grouped by RXPS_SESSION_AI
    """
    logger.info("Grouping data by RXPS_SESSION_AI...")
    grouped_df = df.groupby('RXPS_SESSION_AI').agg({
        'PRODUCT_TYPE': lambda x: ','.join(sorted(set(x.astype(str)))),
        'PRODUCT_DESC': lambda x: ','.join(sorted(set(x.astype(str))))
    }).reset_index()
    
    return grouped_df


def fetch_ticketing_job_ai(conn: object, ckt_batch: List[str]) -> pd.DataFrame:
    """
    Fetch TICKETING_JOB_AI for a batch of circuit IDs.
    
    Args:
        conn: Oracle database connection
        ckt_batch: List of circuit IDs
        
    Returns:
        DataFrame with circuit IDs and their TICKETING_JOB_AI
    """
    format_ckts = ",".join(f"'{ckt}'" for ckt in ckt_batch)
    query = f"""
        SELECT CKTID_OR_TN, TICKETING_JOB_AI
        FROM DWODSORA.TKT_TICKETING_JOB
        WHERE CKTID_OR_TN IN ({format_ckts})
    """
    return pd.read_sql(query, conn)


def parallel_fetch_ticketing_jobs(conn: object, ckt_list: List[str], 
                                max_workers: int = 8) -> pd.DataFrame:
    """
    Fetch TICKETING_JOB_AI data in parallel for multiple circuit IDs.
    
    Args:
        conn: Oracle database connection
        ckt_list: List of circuit IDs
        max_workers: Maximum number of parallel workers
        
    Returns:
        DataFrame with all TICKETING_JOB_AI data
    """
    logger.info(f"Fetching TICKETING_JOB_AI for {len(ckt_list)} circuits in parallel...")
    
    ticketing_job_df_list = []
    if ckt_list:
        ckt_batches = list(batch_list(ckt_list))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_ticketing_job_ai, conn, batch) for batch in ckt_batches]
            for future in as_completed(futures):
                ticketing_job_df_list.append(future.result())
        ticketing_job_df = pd.concat(ticketing_job_df_list, ignore_index=True)
    else:
        ticketing_job_df = pd.DataFrame(columns=['CKTID_OR_TN', 'TICKETING_JOB_AI'])
    
    logger.info(f"Fetched TICKETING_JOB_AI for {len(ticketing_job_df)} records")
    return ticketing_job_df


def run_cbr_query(conn: object, job_ai_batch: List[str]) -> pd.DataFrame:
    """
    Run CBR queries for a batch of job AIs across multiple tables.
    
    Args:
        conn: Oracle database connection
        job_ai_batch: List of TICKETING_JOB_AI values
        
    Returns:
        DataFrame with CBR data from all tables
    """
    format_job_ais = ",".join(f"'{ai}'" for ai in job_ai_batch)
    queries = [
        f"""SELECT TICKETING_JOB_AI, CONTACT_TN AS CBR, META_UPD_DTTM FROM DWODSORA.TKT_WFAC_TICKETING_JOB WHERE TICKETING_JOB_AI IN ({format_job_ais}) AND CONTACT_TN IS NOT NULL""",
        f"""SELECT TICKETING_JOB_AI, CALLER_CAN_BE_REACHED AS CBR, META_UPD_DTTM FROM DWODSORA.TKT_TRACS_TICKETING_JOB WHERE TICKETING_JOB_AI IN ({format_job_ais}) AND CALLER_CAN_BE_REACHED IS NOT NULL""",
        f"""SELECT TICKETING_JOB_AI, CONTACT_TN AS CBR, META_UPD_DTTM FROM DWODSORA.TKT_NTM_TICKETING_JOB WHERE TICKETING_JOB_AI IN ({format_job_ais}) AND CONTACT_TN IS NOT NULL""",
        f"""SELECT TICKETING_JOB_AI, REACH_NBR AS CBR, META_UPD_DTTM FROM DWODSORA.TKT_LMOS_TICKETING_JOB WHERE TICKETING_JOB_AI IN ({format_job_ais}) AND REACH_NBR IS NOT NULL"""
    ]
    
    dfs = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        cbr_futures = [executor.submit(pd.read_sql, q, conn) for q in queries]
        for f in as_completed(cbr_futures):
            dfs.append(f.result())
    
    return pd.concat(dfs, ignore_index=True)


def parallel_fetch_cbr_data(conn: object, ticketing_job_ai_list: List[str], 
                          max_workers: int = 8) -> pd.DataFrame:
    """
    Fetch CBR data in parallel for multiple job AIs.
    
    Args:
        conn: Oracle database connection
        ticketing_job_ai_list: List of TICKETING_JOB_AI values
        max_workers: Maximum number of parallel workers
        
    Returns:
        DataFrame with all CBR data
    """
    logger.info(f"Fetching CBR data for {len(ticketing_job_ai_list)} job AIs in parallel...")
    
    cbr_df_list = []
    if ticketing_job_ai_list:
        job_ai_batches = list(batch_list(ticketing_job_ai_list))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(run_cbr_query, conn, batch) for batch in job_ai_batches]
            for future in as_completed(futures):
                cbr_df_list.append(future.result())
        cbr_df = pd.concat(cbr_df_list, ignore_index=True)
    else:
        cbr_df = pd.DataFrame(columns=['TICKETING_JOB_AI', 'CBR', 'META_UPD_DTTM'])
    
    logger.info(f"Fetched CBR data for {len(cbr_df)} records")
    return cbr_df


def aggregate_cbrs(group: pd.DataFrame) -> str:
    """
    Aggregate CBR values for a group, ordered by META_UPD_DTTM.
    
    Args:
        group: DataFrame group with CBR data
        
    Returns:
        Comma-separated string of CBR values
    """
    ordered = group.sort_values('META_UPD_DTTM', ascending=True)
    return ','.join(ordered['CBR'].astype(str))


def get_hardcoded_numbers() -> set:
    """
    Get set of hardcoded numbers to filter out.
    
    Returns:
        Set of hardcoded numbers
    """
    hardcoded_numbers = [
        "9999999991", "9999999999", "1999999999", "9000000000", "0000000000", "9999999993", "9999999991",
        "9709999999", "1111111111", "5059999999", "2539999999", "5209999999", "8889999999", "0999999999",
        "9999999992", "6129999999", "6515555555", "2180000000", "3333333333", "3609999999", "9999989999",
        "7639999999", "6029999999", "5099999999", "8888888888", "3689999999", "8000000000", "9529999999",
        "5555555555", "2539999999", "7777777777", "3089999999", "8019999999", "8888888889", "9529999999",
        "5099999999", "9705555555", "4069999999", "3605555555", "9700000000", "4809999999", "0000000001",
        "4029999999", "5209999999", "9999999994", "4807777777", "9285555555", "9999999994", "6519999999",
        "4069999999", "9999999992", "3039999999", "9999999993", "3209999999", "9999999993", "0999999999",
        "9909999999", "5555555555", "5099999999", "9000000000", "5209999999", "5202222222", "9999999990", 
        "1000000000", "0919999999", "0999999990", "0000000100", "3207777777", "5159999999", "2069999999",
        "5419999999", "2222222222", "0000590009"
    ]
    return set(hardcoded_numbers)


def process_value(val, hardcoded_set: set, special_words: set):
    """
    Process and filter CBR values, removing hardcoded numbers and special words.
    
    Args:
        val: Value to process
        hardcoded_set: Set of hardcoded numbers to filter
        special_words: Set of special words to filter
        
    Returns:
        Processed value or NaN if filtered out
    """
    if pd.isna(val):
        return np.nan
    val_str = str(val).strip().lower()
    if not val_str:  # Handle empty strings
        return np.nan
    if ',' not in val_str:
        if val_str in hardcoded_set or val_str in special_words:
            return np.nan
        else:
            return str(val).strip()
    parts = [p.strip().lower() for p in str(val).split(',')]
    filtered = [p for p in parts if p and p not in hardcoded_set and p not in special_words]
    return ', '.join(filtered) if filtered else np.nan


def process_column(df: pd.DataFrame, col: str = 'CBR_LIST') -> pd.DataFrame:
    """
    Process a column to filter out hardcoded numbers and special words.
    
    Args:
        df: DataFrame to process
        col: Column name to process
        
    Returns:
        DataFrame with processed column
    """
    hardcoded_set = get_hardcoded_numbers()
    special_words = {"none", "i dont have one"}
    
    df[col] = df[col].apply(lambda x: process_value(x, hardcoded_set, special_words))
    return df


def fetch_cbr_for_missing_jobs(conn: object, job_list: List[str], 
                             query: str, source_name: str) -> pd.DataFrame:
    """
    Fetch CBR data for missing jobs from a specific table.
    
    Args:
        conn: Oracle database connection
        job_list: List of job AIs
        query: SQL query template
        source_name: Name of the data source
        
    Returns:
        DataFrame with CBR data including source information
    """
    results = []
    for batch in batch_list(job_list, 900):
        format_job_ais = ",".join(f"'{job}'" for job in batch)
        batch_query = query.format(format_job_ais=format_job_ais)
        batch_df = pd.read_sql(batch_query, conn)
        batch_df["CBR_SOURCE"] = source_name
        results.append(batch_df)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def parallel_fetch_missing_cbrs(conn: object, missing_jobs: List[str], 
                               max_workers: int = 8) -> pd.DataFrame:
    """
    Fetch missing CBR data in parallel from all tables.
    
    Args:
        conn: Oracle database connection
        missing_jobs: List of job AIs with missing CBR data
        max_workers: Maximum number of parallel workers
        
    Returns:
        DataFrame with all missing CBR data
    """
    logger.info(f"Fetching missing CBR data for {len(missing_jobs)} jobs...")
    
    table_queries = [
        (
            """SELECT TICKETING_JOB_AI, CONTACT_TN AS CBR, META_UPD_DTTM FROM DWODSORA.TKT_WFAC_TICKETING_JOB WHERE TICKETING_JOB_AI IN ({format_job_ais}) AND CONTACT_TN IS NOT NULL""",
            "WFAC_CONTACT_TN"
        ),
        (
            """SELECT TICKETING_JOB_AI, CALLER_CAN_BE_REACHED AS CBR, META_UPD_DTTM FROM DWODSORA.TKT_TRACS_TICKETING_JOB WHERE TICKETING_JOB_AI IN ({format_job_ais}) AND CALLER_CAN_BE_REACHED IS NOT NULL""",
            "TRACS_CALLER_CAN_BE_REACHED"
        ),
        (
            """SELECT TICKETING_JOB_AI, CONTACT_TN AS CBR, META_UPD_DTTM FROM DWODSORA.TKT_NTM_TICKETING_JOB WHERE TICKETING_JOB_AI IN ({format_job_ais}) AND CONTACT_TN IS NOT NULL""",
            "NTM_CONTACT_TN"
        ),
        (
            """SELECT TICKETING_JOB_AI, REACH_NBR AS CBR, META_UPD_DTTM FROM DWODSORA.TKT_LMOS_TICKETING_JOB WHERE TICKETING_JOB_AI IN ({format_job_ais}) AND REACH_NBR IS NOT NULL""",
            "LMOS_REACH_NBR"
        ),
    ]
    
    all_cbrs = []
    if missing_jobs:
        fetch_args = []
        for query, source_name in table_queries:
            for batch in batch_list(missing_jobs, 900):
                fetch_args.append((list(batch), query, source_name))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_cbr_for_missing_jobs, conn, *args) for args in fetch_args]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    all_cbrs.append(df)
    
    return pd.concat(all_cbrs, ignore_index=True) if all_cbrs else pd.DataFrame()


def join_cbrs_keep_duplicates(group: pd.DataFrame) -> str:
    """
    Join CBR values with their sources, keeping duplicates.
    
    Args:
        group: DataFrame group with CBR data
        
    Returns:
        String with CBR values and their sources
    """
    return ','.join(group.apply(lambda row: f"{row['CBR']}[{row['CBR_SOURCE']}]", axis=1))


def process_ticket_data(user: str, password: str, dsn: str, start_date: str, end_date: str,
                       instant_client_path: Optional[str] = None, 
                       max_workers: int = 8) -> pd.DataFrame:
    """
    Main function to process ticket data and fetch CBR information.
    
    Args:
        user: Database username
        password: Database password
        dsn: Data Source Name
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        instant_client_path: Path to Oracle instant client (optional)
        max_workers: Maximum number of parallel workers
        
    Returns:
        DataFrame with processed ticket data including CBR information
        
    Raises:
        Exception: If database operations fail
    """
    conn = None
    try:
        # Step 1: Create database connection
        conn = create_oracle_connection(user, password, dsn, instant_client_path)
        
        # Step 2: Fetch main data
        final_df_clean = fetch_main_data(conn, start_date, end_date)
        
        # Step 3: Group by RXPS_SESSION_AI
        grouped_df = group_by_session(final_df_clean)
        
        # Step 4: Parallel fetch TICKETING_JOB_AI for circuits
        ckt_list = final_df_clean['CKTID_OR_TN'].dropna().unique().tolist()
        ticketing_job_df = parallel_fetch_ticketing_jobs(conn, ckt_list, max_workers)
        ticketing_job_ai_list = ticketing_job_df['TICKETING_JOB_AI'].dropna().unique().tolist()
        
        # Step 5: Parallel fetch all CBRs for all jobs
        cbr_df = parallel_fetch_cbr_data(conn, ticketing_job_ai_list, max_workers)
        
        # Step 6: Merge and aggregate CBRs
        cbr_df_with_circuit = cbr_df.merge(
            ticketing_job_df[['TICKETING_JOB_AI', 'CKTID_OR_TN']],
            on='TICKETING_JOB_AI',
            how='left'
        )
        
        cbr_per_circuit = (
            cbr_df_with_circuit
            .groupby('CKTID_OR_TN')
            .apply(aggregate_cbrs)
            .reset_index(name='CBR_LIST')
        )
        
        final_df_with_cbr = final_df_clean.merge(
            cbr_per_circuit,
            on='CKTID_OR_TN',
            how='left'
        )
        
        # Step 7: Filter out hardcoded numbers and special words
        final_df_with_cbr = process_column(final_df_with_cbr, 'CBR_LIST')
        
        # Step 8: Parallel fetch missing CBRs from all tables
        missing_cbr_mask = final_df_with_cbr['CBR_LIST'].isna()
        missing_jobs = final_df_with_cbr.loc[missing_cbr_mask, 'TICKETING_JOB_AI'].dropna().unique().tolist()
        
        all_cbrs_df = parallel_fetch_missing_cbrs(conn, missing_jobs, max_workers)
        
        if not all_cbrs_df.empty:
            all_cbrs_df['META_UPD_DTTM'] = pd.to_datetime(all_cbrs_df['META_UPD_DTTM'])
            all_cbrs_sorted = all_cbrs_df.sort_values(['TICKETING_JOB_AI', 'META_UPD_DTTM'])
            
            cbr_list_df = (
                all_cbrs_sorted
                .groupby('TICKETING_JOB_AI')
                .apply(join_cbrs_keep_duplicates)
                .reset_index(name='CBR_LIST')
            )
            
            # Fill NaN CBR_LIST only
            final_df_with_cbr = final_df_with_cbr.merge(
                cbr_list_df,
                on='TICKETING_JOB_AI',
                how='left',
                suffixes=('', '_new')
            )
            final_df_with_cbr['CBR_LIST'] = final_df_with_cbr['CBR_LIST'].combine_first(final_df_with_cbr['CBR_LIST_new'])
            final_df_with_cbr = final_df_with_cbr.drop(columns=['CBR_LIST_new'])
        
        logger.info(f"Processing completed. Final dataset has {len(final_df_with_cbr)} records")
        return final_df_with_cbr
        
    except Exception as e:
        logger.error(f"Error processing ticket data: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


def main():
    """
    Example usage of the ticket data processing function.
    
    Note: Update the connection parameters and dates as needed.
    """
    # Example usage - update these parameters as needed
    instant_client_path = r'C:\Users\AD58507\Downloads\instantclient-basic-windows.x64-23.9.0.25.07\instantclient_23_9'
    user = 'AD58507'
    password = 'C#ntury123'
    dsn = 'RACORAP33-SCAN.CORP.INTRANET:1521/DWDN01P_USERS'
    start_date = '2024-08-13'
    end_date = '2025-08-13'
    
    try:
        result_df = process_ticket_data(
            user=user,
            password=password,
            dsn=dsn,
            start_date=start_date,
            end_date=end_date,
            instant_client_path=instant_client_path,
            max_workers=8
        )
        
        print(f"Processing completed successfully. Result has {len(result_df)} records")
        print("\nFirst few rows:")
        print(result_df.head())
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()