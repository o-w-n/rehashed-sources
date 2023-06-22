import os
from datetime import date

import pandas as pd
from psycopg2 import sql
from loguru import logger

from config import read_config, timed, create_ssh_tunnel, connect_to_database, execute_sql_query, get_data_from_db


def generate_aff_source_pairs(df1: pd.DataFrame, df2: pd.DataFrame) -> list:
    """
    This function iterates over the rows of two DataFrames, `df1` and `df2`,
    and generates unique hash pairs based on specified conditions.
    :param df1: containing affiliation and source information
    :param df2: containing account, segment, and offer information
    :return: list of unique hash pairs generated based on the given conditions
    """
    df2.dropna(subset=['ssp_segment_id'], inplace=True)
    hash_pairs = set()
    for _, row_df1 in df1.iterrows():
        aff_id = row_df1['current_aff_id']
        source = row_df1['current_source']
        for _, row_df2 in df2.iterrows():
            try:
                account_id = str(row_df2['ssp_account_id'])
                segment_id = int(row_df2['ssp_segment_id'])
                offer_id = row_df2['offer_id']
                advertiser = row_df2['advertiser']
                segment_name = row_df2['name']
                w_list = row_df2['source_token_whitelist']
                b_list = row_df2['source_token_blacklist']

                hash_list = set(w_list) | set(b_list)
                if hash_list:
                    hash_pairs.add((aff_id, source, account_id, int(segment_id), offer_id, advertiser, segment_name))
            except Exception as ex:
                logger.error(ex)
    return list(hash_pairs)


@timed
def traffic_source_hash_pairs(data: list) -> pd.DataFrame:
    """
    This function generates hash values for traffic source pairs and retrieves the results from a database.
    :param data: list of tuples representing the traffic source pairs.
                 each tuple should contain (aff_id, source, account_id, segment_id, offer_id, advertiser, segment_name).
    :return: a df containing the original traffic source pairs along with their corresponding hash values.
    """
    ssh_params, db_params = read_config()
    with create_ssh_tunnel(ssh_params) as server:
        db_port = server.local_bind_port
        conn = connect_to_database(db_params, db_port)
        values = []
        for pair in data:
            try:
                values.append(sql.SQL('({}, {}, {}, {})').format(sql.Literal(int(pair[0])),
                                                                 sql.Literal(str(pair[1])),
                                                                 sql.Literal(str(pair[2])),
                                                                 sql.Literal(int(pair[3]))))
            except:
                pass
        query = sql.SQL(
            "SELECT hashtext("
            "CAST(COALESCE(aff_id, 0) AS VARCHAR) || '_' || "
            "CAST(COALESCE(source, '') AS VARCHAR) || '_' || "
            "CAST(COALESCE(account_id, '00000000-0000-0000-0000-000000000000') AS VARCHAR) || '_' || "
            "CAST(COALESCE(segment_id, 0) AS VARCHAR)"
            ")"
            "FROM (VALUES {}) AS vals (aff_id, source, account_id, segment_id)"
        ).format(sql.SQL(', ').join(values))

        results = execute_sql_query(conn, query)
        df_results = pd.DataFrame({'aff_id': [pair[0] for pair in pairs],
                                   'source': [pair[1] for pair in pairs],
                                   'account_id': [pair[2] for pair in pairs],
                                   'segment_id': [pair[3] for pair in pairs],
                                   'offer_id': [pair[4] for pair in pairs],
                                   'advertiser': [pair[5] for pair in pairs],
                                   'segment_name': [pair[6] for pair in pairs],
                                   'hash_value': results['hashtext'].tolist()})
    return df_results


def get_current_hashes(df: pd.DataFrame) -> set:
    """
       This function retrieves the unique hashed sources from the 'source_token_whitelist' and 'source_token_blacklist'
       columns of the provided DataFrame.
       :param df: df containing the data with 'ssp_account_id', 'source_token_whitelist', and 'source_token_blacklist'.
       :return: a set of unique hashed sources.
       """
    total_list_set = set()
    for _, row in df.iterrows():
        account_id = row['ssp_account_id']
        if pd.notnull(account_id):
            w_list = row['source_token_whitelist']
            b_list = row['source_token_blacklist']
            total_list_set.update(w_list)
            total_list_set.update(b_list)
    logger.info(f'All hashed sources: {len(total_list_set)}')
    return total_list_set


def rehashed_sources(hashed_list: set) -> pd.DataFrame:
    """
    This function filters the traffic source pairs based on whether the hash value exists in the provided hashed list.
    :param hashed_list: list of hashed values to filter the traffic source pairs.
    :return: df containing the rehashed traffic source pairs.
    """
    rows = []
    hashed_results = traffic_source_hash_pairs(pairs)
    for _, row in hashed_results.iterrows():
        if str(row['hash_value']) in hashed_list:
            row = {
                'hash_value': row['hash_value'],
                'offer_id': row['offer_id'],
                'aff_id': row['aff_id'],
                'source': row['source'],
                'account_id': row['account_id'],
                'segment_id': row['segment_id'],
                'advertiser': row['advertiser'],
                'segment_name': row['segment_name'],
            }
            rows.append(row)
    logger.info(f'All rehashed sources: {len(rows)}')
    return pd.DataFrame(rows)


if __name__ == '__main__':
    query_acc = """
        select o.id as offer_id,
               source_token_blacklist,
               source_token_whitelist,
               ssp_account_id,
               ssp_segment_id,
               ss.name,
               advertiser
        from offer o
        left join ssp_segment ss on o.ssp_segment_id = ss.id
    """
    query_aff = """
        select bc.current_aff_id as current_aff_id,
               bc.current_source
        from banner_click bc
        where bc.created::date >= current_date - interval '60 days'
         and bc.current_aff_id is not NULL
        group by 1,2
    """

    df_acc = get_data_from_db(query_acc)
    df_aff = get_data_from_db(query_aff)

    pairs = generate_aff_source_pairs(df_aff, df_acc)
    current_hashes = get_current_hashes(df_acc)

    rehashed_data = rehashed_sources(current_hashes)
    rehashed_data.to_csv(os.path.join(os.getcwd(), '', f'rehash_{date.today()}.csv'), index=False)
