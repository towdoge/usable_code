import pandas as pd
import numpy as np


def expand_df_with_wildcard_by_dict(
    df: pd.DataFrame, col_all_value_dict: dict, col1: str, col2: str, drop_list: list
):
    """
    some cells in the original sheet are * as a wildcard.
    We replace the * values to speed up the matching.
    expand col2 by col1
    :param col2: expand column
    :param col1: limit column
    :param df: the original dataframe
    :param col_all_value_dict: a dictionary with defining columns as the keys
     and the corresponding set of values as values in the dict.
    :param drop_list: cols for drop duplicates
    :return: a expanded dataframe.
    """
    if df.empty:
        return df
    df_wildcard = df.loc[df[col2] == "*"]
    df_no_wildcard = df.loc[df[col2] != "*"]
    tmp = pd.DataFrame(columns=[col1, col2])
    for k, v in col_all_value_dict.items():
        tmp = pd.concat([tmp, pd.DataFrame({col1: [k] * len(v), col2: v})])
    del df_wildcard[col2]
    df_wildcard = df_wildcard.merge(tmp, left_on=col1, right_on=col1)
    df_test = pd.concat([df_no_wildcard, df_wildcard]).drop_duplicates(
        subset=drop_list, keep="first"
    )
    return df_test


def expand_df_with_wildcard(
    df: pd.DataFrame, col_all_value_dict: dict, drop_list: list
):
    """
    some cells in the original sheet are * as a wildcard.
    We replace the * values to speed up the matching.
    :param df: the original dataframe
    :param col_all_value_dict: a dictionary with defining columns as the keys
     and the corresponding set of values as values in the dict.
    :param drop_list: list for drop_duplicates
    :return: a expanded dataframe.
    """
    if df.empty:
        return df
    # get star nums for each row
    df["num_star"] = df.apply(
        lambda x: np.sum([1 if x[c] == "*" else 0 for c in col_all_value_dict]), axis=1
    )
    df = df.sort_values("num_star")
    # get rows without star
    df_0 = df.loc[df.num_star == 0].copy()
    for num in sorted(set(df.loc[df.num_star > 0, "num_star"].values)):
        # get df with star by nums
        df_remain = df.loc[df.num_star == num].copy()
        for i in range(num):
            # remove one * each time
            # for example
            # first time [[*, *]] -> [  [a,*],[b,*],   [*,a],[*,b]  ]
            # second time [  [a,a],[a,b],  [b,a],[b,b],   [a,a],[b,a],  [a,b],[b,b]  ]
            df_remain = pd.concat(
                [
                    # replace * by merge new df
                    df_remain.loc[df_remain[c] == "*"]
                    .merge(
                        # new df with [columns_, *]
                        pd.DataFrame(
                            {
                                str(c) + "_": col_all_value_dict[c],
                                c: ["*"] * len(col_all_value_dict[c]),
                            }
                        )
                    )[[col for col in list(df.columns) + [str(c) + "_"] if col != c]]
                    .rename(columns={str(c) + "_": c})
                    for c in col_all_value_dict
                ],
                sort=False,
            )
        df_0 = pd.concat([df_0, df_remain], sort=False)
        df_0 = df_0.sort_values(
            [c for c in col_all_value_dict] + ["num_star"]
        ).drop_duplicates(subset=drop_list, keep="first")

    del df_0["num_star"]
    return df_0
