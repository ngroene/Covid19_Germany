def aggregate_to_state(df_to_be_aggregated, Bundesland, Bundesland_short):
    '''
    Adds the daily new Covid-19 cases for a specific German state to df_Covid in a new column.
    I.e., it aggregates the raw data on new Covid-19 cases from RKI by day and on state level
    and filters the numbers of the state specified.
    Daily numbers are then sorted in ascending order.
    A column is added with the 7-day moving average

    Arguments: name of dataframe to be aggregated, name of Bundesland and abbreviation of Bundesland as strings

    Output: dataframe
    '''
    df = df_to_be_aggregated.groupby(['Date', 'Bundesland']).AnzahlFall.agg(['sum']).reset_index().sort_values(['Date', 'Bundesland'],ascending=[True, True]).rename(columns = {'sum':'New_Covid19_cases'+'_'+Bundesland_short}).copy()
    df_Bundesland = df.loc[df['Bundesland'] == Bundesland].copy()
    # create 7-days moving average (with 1 day time lag to account for reporting delays)
    df_Bundesland.set_index("Date", inplace=True)
    df_Bundesland = df_Bundesland.sort_index().asfreq('D')
    df_Bundesland['New_Covid19_cases_' + Bundesland_short] = df_Bundesland['New_Covid19_cases_' + Bundesland_short].fillna(0)
    df_Bundesland['Shift_1'] = df_Bundesland['New_Covid19_cases_' + Bundesland_short].shift(1).fillna(0)
    df_Bundesland['nCovid_av_' + Bundesland_short] = df_Bundesland['Shift_1'].rolling(window=7).mean().fillna(0)
    del df_Bundesland['Bundesland'], df_Bundesland['Shift_1']

    return df_Bundesland


def aggregate_to_LK(df_to_be_aggregated, Landkreis, Landkreis_short):
    '''
    Adds the daily new Covid-19 cases for a specific German Landkreis to df_Covid in a new column.
    I.e., it aggregates the raw data on new Covid-19 cases from RKI by day and on state level
    and filters the numbers of the state specified.
    Daily numbers are then sorted in ascending order.
    A column is added with the 7-day moving average

    Arguments: name of dataframe to be aggregated, name of Landkreis and abbreviation of Landkreis as strings

    Output: dataframe
    '''
    df = df_to_be_aggregated.groupby(['Date', 'Landkreis']).AnzahlFall.agg(['sum']).reset_index().sort_values(['Date', 'Landkreis'],ascending=[True, True]).rename(columns = {'sum':'New_Covid19_cases'+'_'+Landkreis_short}).copy()
    df_Landkreis = df.loc[df['Landkreis'] == Landkreis].copy()
    # create 7-days moving average (with 1 day time lag to account for reporting delays)
    df_Landkreis.set_index("Date", inplace=True)
    df_Landkreis = df_Landkreis.sort_index().asfreq('D')
    df_Landkreis['New_Covid19_cases_' + Landkreis_short] = df_Landkreis['New_Covid19_cases_' + Landkreis_short].fillna(0)
    df_Landkreis['Shift_1'] = df_Landkreis['New_Covid19_cases_' + Landkreis_short].shift(1).fillna(0)
    df_Landkreis['nCovid_av_' + Landkreis_short] = df_Landkreis['Shift_1'].rolling(window=7).mean().fillna(0)
    del df_Landkreis['Landkreis'], df_Landkreis['Shift_1']

    return df_Landkreis
