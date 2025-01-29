import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt

def calculate_metrics(current_date, db_host, db_user, db_password):
    conn = psycopg2.connect(
        dbname='courses'
        ,user=db_user
        ,password=db_password
        ,host=db_host
        ,port='5432'
    )

    today = datetime.strptime(current_date, '%Y-%m-%d')
    
    today_str = today.strftime('%Y-%m-%d')
    
    yesterday = datetime.strptime((today-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')
    yesterday_last_month = datetime.strptime((today-relativedelta(months = 1)-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')
    yesterday_prev_month = datetime.strptime((today-relativedelta(months = 2)-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')
    cur = conn.cursor()
    
    cur.execute("SELECT user_id, created_at FROM python_for_da.central_cinema_users")
    users_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    users = pd.DataFrame(users_sql, columns = colnames)
    cur.close()
    
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM python_for_da.central_cinema_user_payments")
    user_payments_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    user_payments = pd.DataFrame(user_payments_sql, columns = colnames)
    cur.close()
    
    cur = conn.cursor()
    cur.execute("SELECT partner_commission_id, partner_id, commission FROM python_for_da.central_cinema_partner_commission")
    partner_comission_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    partner_comission = pd.DataFrame(partner_comission_sql, columns = colnames)
    cur.close()
    
    cur = conn.cursor()
    cur.execute("SELECT * FROM python_for_da.central_cinema_user_activity where play_start>'2024-09-16'")
    user_activity_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    user_activity = pd.DataFrame(user_activity_sql, columns = colnames)
    cur.close()
    
    cur = conn.cursor()
    cur.execute("""select distinct t.title_id, t.duration
                    from python_for_da.central_cinema_title t 
                    join python_for_da.central_cinema_user_activity a on 
                    t.title_id = a.title_id and play_start>'2024-09-16'
    """)
    title_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    title = pd.DataFrame(title_sql, columns = colnames)

    
    # Количество триалов
    users['cogort'] = users['created_at'].dt.to_period('M').dt.to_timestamp()
    user_payments['payment_day'] = pd.to_datetime(user_payments['payment_date']).dt.to_period('D').dt.to_timestamp()
    trial_yesterday = user_payments[(user_payments['is_trial']==1)&(user_payments['payment_day']==yesterday)]['user_payment_id'].count()
    trial_yesterday_last_month = user_payments[(user_payments['is_trial']==1)&(user_payments['payment_day']==yesterday_last_month)]['user_payment_id'].count()
    trial_delta = round((trial_yesterday)/trial_yesterday_last_month*100,2)
    symbol_trial_delta = "\U0001F4C8"+"\U00002705" if trial_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    
    
    # Количество оплат
    payment_count_yesterday = user_payments[(user_payments['is_trial']==0)&(user_payments['payment_day']==yesterday)]['user_payment_id'].count()
    payment_count_yesterday_last_month = user_payments[(user_payments['is_trial']==0)&(user_payments['payment_day']==yesterday_last_month)]['user_payment_id'].count()
    payment_count_delta = round((payment_count_yesterday)/payment_count_yesterday_last_month*100,2)
    symbol_payment_count_delta = "\U0001F4C8"+"\U00002705" if trial_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    
    
    # Конверсия в первую оплату
    # таблица не-триальщиков за вчера (прошлый месяц и вчера)
    trial_0_yesterday = user_payments[(user_payments['is_trial'] == 0) & (user_payments['payment_day']==yesterday)]
    trial_1_yesterday_last_month = user_payments[(user_payments['is_trial'] == 1) & (user_payments['payment_day']==yesterday_last_month)]
    merged_trial_users_yesterday = trial_1_yesterday_last_month.merge(trial_0_yesterday, how = 'inner', on='user_id')
    # таблица не-триальщиков за прошлый месяц (позапрошлый месяц и прошлый)
    trial_0_yesterday_last_month = user_payments[(user_payments['is_trial'] == 0) & (user_payments['payment_day']==yesterday_last_month)]
    trial_1_yesterday_prev_month = user_payments[(user_payments['is_trial'] == 1) & (user_payments['payment_day']==yesterday_prev_month)]
    merged_trial_users_yesterday_last_month = trial_1_yesterday_prev_month.merge(trial_0_yesterday_last_month, how = 'inner', on='user_id')
    # 1
    non_trial_yesterday = merged_trial_users_yesterday[(merged_trial_users_yesterday['is_trial_y']==0)&(merged_trial_users_yesterday['payment_day_y']==yesterday)]['user_payment_id_y'].count()
    trial_yesterday_last_month = trial_1_yesterday_last_month[(trial_1_yesterday_last_month['is_trial']==1)&(trial_1_yesterday_last_month['payment_day']==yesterday_last_month)]['user_payment_id'].count()
    conversion_yesterday = round((non_trial_yesterday / trial_yesterday_last_month), 2)
    # 2
    non_trial_yesterday_last_month = merged_trial_users_yesterday_last_month[(merged_trial_users_yesterday_last_month['is_trial_y']==0)&(merged_trial_users_yesterday_last_month['payment_day_y']==yesterday_last_month)]['user_payment_id_y'].count()
    trial_yesterday_prev_month = trial_1_yesterday_prev_month[(trial_1_yesterday_prev_month['is_trial']==1)&(trial_1_yesterday_prev_month['payment_day']==yesterday_prev_month)]['user_payment_id'].count()
    conversion_yesterday_last_month = round((non_trial_yesterday_last_month / trial_yesterday_prev_month), 2)
    # 3
    conversion_delta = round(conversion_yesterday/conversion_yesterday_last_month*100,2)
    symbol_conversion_delta = "\U0001F4C8"+"\U00002705" if conversion_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    
    
    # Cash_in
    arpu = 299
    cash_in_yesterday = payment_count_yesterday * arpu
    cash_in_yesterday_last_month = payment_count_yesterday_last_month * arpu
    cash_in_delta = round((cash_in_yesterday)/cash_in_yesterday_last_month*100,2)
    symbol_cash_in_delta = "\U0001F4C8"+"\U00002705" if cash_in_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    
    
    # CAC
    merged_payments_and_partners = user_payments.merge(partner_comission, how = 'inner', on='partner_id')
    cac_yesterday = round(merged_payments_and_partners[(merged_payments_and_partners['is_trial']==1)&(merged_payments_and_partners['payment_day']==yesterday)]['commission'].mean(), 2)
    cac_yesterday_last_month = round(merged_payments_and_partners[(merged_payments_and_partners['is_trial']==1)&(merged_payments_and_partners['payment_day']==yesterday_last_month)]['commission'].mean(), 2)
    cac_delta = round((cac_yesterday)/cac_yesterday_last_month*100,2)
    symbol_cac_delta = "\U0001F4C8"+"\U00002705" if cac_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    
    # Прогнозируемые LTR и LTV
    user_payments['payment_month'] = user_payments['payment_date'].dt.to_period('M')
    merged_users = user_payments.merge(users, how = 'inner', on='user_id')
    
    retention_abs = merged_users.groupby(['cogort', 'payment_month']).agg({'user_id': 'nunique'}).unstack(fill_value=0)
    retention_abs.columns = retention_abs.columns.droplevel()
    retention_abs = retention_abs.reset_index()
    retention_abs.columns.name = None
    cogort_sizes = users.groupby('cogort').agg({'user_id': 'size'})
    retention_rel = retention_abs.set_index('cogort').div(cogort_sizes['user_id'], axis=0)
    cogort_sizes = cogort_sizes.reset_index()
    
    retention_rel = retention_rel.rename(columns=lambda x: f"{x}_ret" if x != "cogort" else x)
    
    ltr_results_yesterday = []
    
    for i in range(11):
        start_col = i + 1
        sum_of_six = retention_rel.iloc[i, start_col:start_col+6].sum()
        ltr_results_yesterday.append(sum_of_six)
        
    avg_ltr_yesterday = sum(ltr_results_yesterday) / len(ltr_results_yesterday)
    pred_ltr_yesterday = round(avg_ltr_yesterday * arpu, 2)
    
    ltr_results_yesterday_last_month = []
    
    for j in range(10):
        start_col = j + 1
        sum_of_six = retention_rel.iloc[j, start_col:start_col+6].sum()
        ltr_results_yesterday_last_month.append(sum_of_six)
        
    avg_ltr_yesterday_last_month = sum(ltr_results_yesterday_last_month) / len(ltr_results_yesterday_last_month)
    pred_ltr_yesterday_last_month = round(avg_ltr_yesterday_last_month * arpu, 2)
    pred_ltr_delta = round((pred_ltr_yesterday)/pred_ltr_yesterday_last_month*100,2)
    symbol_pred_ltr_delta = "\U0001F4C8"+"\U00002705" if pred_ltr_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    
    
    pred_ltv_yesterday =  pred_ltr_yesterday - cac_yesterday
    pred_ltv_yesterday_last_month = pred_ltr_yesterday_last_month - cac_yesterday_last_month
    pred_ltv_delta = round((pred_ltv_yesterday)/pred_ltv_yesterday_last_month*100,2)
    symbol_pred_ltv_delta = "\U0001F4C8"+"\U00002705" if pred_ltv_delta > 100 else "\U0001F4C9"+"\U0001F53B"


    # Среднее время сессии
    user_activity['watching_day'] = pd.to_datetime(user_activity['play_start']).dt.to_period('D').dt.to_timestamp()
    user_activity['viewing_time'] = (user_activity['play_end'] - user_activity['play_start']).dt.total_seconds() / 60
    avg_session_time_yesterday = round(user_activity[user_activity['watching_day']==yesterday]['viewing_time'].mean(), 2)
    avg_session_time_yesterday_last_month = round(user_activity[user_activity['watching_day']==yesterday_last_month]['viewing_time'].mean(), 2)
    avg_session_time_delta = round((avg_session_time_yesterday)/avg_session_time_yesterday_last_month*100,2)
    symbol_avg_session_time_delta = "\U0001F4C8"+"\U00002705" if avg_session_time_delta > 100 else "\U0001F4C9"+"\U0001F53B"


    # Досматриваемость
    merged_activity_and_title = title.merge(user_activity, how = 'inner', on='title_id')
    merged_activity_and_title['inspection'] = (merged_activity_and_title['viewing_time'] / merged_activity_and_title['duration'])
    avg_inspection_yesterday = round(merged_activity_and_title[merged_activity_and_title['watching_day']==yesterday]['inspection'].mean(), 2)
    avg_inspection_yesterday_last_month = round(merged_activity_and_title[merged_activity_and_title['watching_day']==yesterday_last_month]['inspection'].mean(), 2)
    avg_inspection_delta = round((avg_inspection_yesterday)/avg_inspection_yesterday_last_month*100,2)
    symbol_avg_inspection_delta = "\U0001F4C8"+"\U00002705" if avg_inspection_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    

    # Количество уникальных просмотров
    unique_viewers_yesterday = user_activity[user_activity['watching_day']==yesterday]['user_id'].nunique()
    unique_viewers_yesterday_last_month = user_activity[user_activity['watching_day']==yesterday_last_month]['user_id'].nunique()
    unique_viewers_delta = round((unique_viewers_yesterday)/unique_viewers_yesterday_last_month*100,2)
    symbol_unique_viewers_delta = "\U00001F4C"+"\U00002705" if unique_viewers_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    

    # Среднее число просмотров на пользователя
    total_views_yesterday = user_activity[user_activity['watching_day'] == yesterday].shape[0]
    avg_views_per_user_yesterday = round(total_views_yesterday / unique_viewers_yesterday, 2)
    total_views_yesterday_last_month = user_activity[user_activity['watching_day'] == yesterday_last_month].shape[0]
    avg_views_per_user_yesterday_last_month = round(total_views_yesterday_last_month / unique_viewers_yesterday_last_month, 2)
    avg_views_per_user_delta = round((avg_views_per_user_yesterday)/avg_views_per_user_yesterday_last_month*100,2)
    symbol_avg_views_per_user_delta = "\U0001F4C8"+"\U00002705" if avg_views_per_user_delta > 100 else "\U0001F4C9"+"\U0001F53B"


    # Доля новых платящих пользователей
    first_payments = merged_users.groupby('user_id')['payment_day'].min().reset_index()
    first_payments.columns = ['user_id', 'first_payment_day']
    merged_users_with_first_payment = merged_users.merge(first_payments, on='user_id', how='left')
    
    new_paying_users_yesterday = merged_users_with_first_payment[
    (merged_users_with_first_payment['payment_day'] == yesterday) & 
    (merged_users_with_first_payment['first_payment_day'] == yesterday)
    ]['user_id'].nunique()
    paying_users_yesterday = merged_users[
        merged_users['payment_day'] == yesterday
    ]['user_id'].nunique()
    new_paying_users_rate_yesterday = round((new_paying_users_yesterday / paying_users_yesterday) * 100, 2)

    new_paying_users_yesterday_last_month = merged_users_with_first_payment[
        (merged_users_with_first_payment['payment_day'] == yesterday_last_month) & 
        (merged_users_with_first_payment['first_payment_day'] == yesterday_last_month)
    ]['user_id'].nunique()
    paying_users_yesterday_last_month = merged_users[
        merged_users['payment_day'] == yesterday_last_month
    ]['user_id'].nunique()

    new_paying_users_rate_yesterday_last_month = round((new_paying_users_yesterday_last_month / paying_users_yesterday_last_month) * 100, 2)
    new_paying_users_rate_delta = round((new_paying_users_rate_yesterday / new_paying_users_rate_yesterday_last_month) * 100, 2)
    symbol_new_paying_users_rate_delta = "\U0001F4C8\U00002705" if new_paying_users_rate_delta > 100 else "\U0001F4C9\U0001F53B"


    # Сообщение
    message = (f'''
    🍿 Central Cinema на {yesterday.strftime('%Y-%m-%d')}
    Вчера (в прошлом месяце)
    
    🔷 Количество триалов(шт.):
    {trial_yesterday} ({trial_yesterday_last_month})
    МoМ %: {symbol_trial_delta} {trial_delta}%
    
    🔷 Количество оплат(шт.):
    {payment_count_yesterday} ({payment_count_yesterday_last_month})
    МoМ %: {symbol_payment_count_delta} {payment_count_delta}%
    
    🔷 Конверсия в первую оплату(%):
    {conversion_yesterday} ({conversion_yesterday_last_month})
    МoМ %: {symbol_conversion_delta} {conversion_delta}%
    
    💰 Валовый Cash-in(руб.):
    {cash_in_yesterday} ({cash_in_yesterday_last_month})
    МoМ %: {symbol_cash_in_delta} {cash_in_delta}%
    
    💰 CAC(руб.):
    {cac_yesterday} ({cac_yesterday_last_month})
    МoМ %: {symbol_cac_delta} {cac_delta}%
    
    💰 Прогнозируемый LTR(руб.):
    {pred_ltr_yesterday} ({pred_ltr_yesterday_last_month})
    МoМ %: {symbol_pred_ltr_delta} {pred_ltr_delta}%
    
    💰 Прогнозируемый LTV(руб.):
    {pred_ltv_yesterday} ({pred_ltv_yesterday_last_month})
    МoМ %: {symbol_pred_ltv_delta} {pred_ltv_delta}%
    
    ⏱ Среднее время сессии(мин.):
    {avg_session_time_yesterday} ({avg_session_time_yesterday_last_month})
    МoМ %: {symbol_avg_session_time_delta} {avg_session_time_delta}%
    
    ⏱ Досматриваемость(%):
    {avg_inspection_yesterday} ({avg_inspection_yesterday_last_month})
    МoМ %: {symbol_avg_inspection_delta} {avg_inspection_delta}%
    
    🔷 Количество уникальных просмотров(шт.):
    {unique_viewers_yesterday} ({unique_viewers_yesterday_last_month})
    МoМ %: {symbol_unique_viewers_delta} {unique_viewers_delta}%

    🔷 Среднее число просмотров на пользователя(шт.):
    {avg_views_per_user_yesterday} ({avg_views_per_user_yesterday_last_month})
    МoМ %: {symbol_avg_views_per_user_delta} {avg_views_per_user_delta}%

    🔷 Доля новых платящих пользователей(%):
    {new_paying_users_rate_yesterday} ({new_paying_users_rate_yesterday_last_month})
    МoМ %: {symbol_new_paying_users_rate_delta} {new_paying_users_rate_delta}%
    
    ''')


    # Расчет данных в течение месяца для построения графиков
    # Количество триалов
    trials_last_month = user_payments[(user_payments['is_trial'] == 1) & 
                                  (user_payments['payment_day'] >= yesterday_last_month) & 
                                  (user_payments['payment_day'] <= yesterday)]
    trials_last_month_grouped = trials_last_month.groupby(trials_last_month['payment_day']).agg({'user_payment_id': 'count'})
    trials_last_month_grouped = trials_last_month_grouped.rename(columns={'user_payment_id': 'trial_count'}).reset_index()

    # Количество оплат
    payments_non_trial_last_month = user_payments[(user_payments['is_trial'] == 0) & 
                                              (user_payments['payment_day'] >= yesterday_last_month) & 
                                              (user_payments['payment_day'] <= yesterday)]
    payments_non_trial_last_month_grouped = payments_non_trial_last_month.groupby(payments_non_trial_last_month['payment_day']).agg({'user_payment_id': 'count'})
    payments_non_trial_last_month_grouped = payments_non_trial_last_month_grouped.rename(columns={'user_payment_id': 'payment_count'}).reset_index()

    # Конверсия в первую оплату
    trials_prev_month = user_payments[(user_payments['is_trial'] == 1) & 
                                  (user_payments['payment_day'] >= yesterday_prev_month) & 
                                  (user_payments['payment_day'] < yesterday_last_month)]

    non_trial_last_month = user_payments[(user_payments['is_trial'] == 0) & 
                                        (user_payments['user_id'].isin(trials_prev_month['user_id'])) & 
                                        (user_payments['payment_day'] >= yesterday_last_month) & 
                                        (user_payments['payment_day'] <= yesterday)]
    trials_grouped = trials_prev_month.groupby(trials_prev_month['payment_day']).agg({'user_payment_id': 'count'}).rename(columns={'user_payment_id': 'trial_payment_count'}).reset_index()
    non_trial_grouped = non_trial_last_month.groupby(non_trial_last_month['payment_day']).agg({'user_payment_id': 'count'}).rename(columns={'user_payment_id': 'non_trial_payment_count'}).reset_index()

    trials_grouped['day'] = trials_grouped['payment_day'].dt.day
    non_trial_grouped['day'] = non_trial_grouped['payment_day'].dt.day

    conversion_df = pd.merge(trials_grouped, non_trial_grouped, on='day', suffixes=('_trial', '_non_trial'))
    conversion_df['conversion'] = round(conversion_df['non_trial_payment_count'] / conversion_df['trial_payment_count'], 2)
    conversion_df = conversion_df[['payment_day_trial', 'conversion']]

    # Валовый cash-in
    payments_non_trial_last_month_grouped['cash_in'] = payments_non_trial_last_month_grouped['payment_count'] * arpu
    cash_in_daily = payments_non_trial_last_month_grouped.groupby(payments_non_trial_last_month_grouped['payment_day']).agg({'cash_in': 'sum'}).reset_index()

    # CAC и LTV
    merged_payments_and_partners = user_payments.merge(partner_comission, how = 'inner', on='partner_id')
    cac_yesterday = round(merged_payments_and_partners[(merged_payments_and_partners['is_trial']==1)&(merged_payments_and_partners['payment_day']==yesterday)]['commission'].mean(), 2)
    cac_yesterday_last_month = round(merged_payments_and_partners[(merged_payments_and_partners['is_trial']==1)&(merged_payments_and_partners['payment_day']==yesterday_last_month)]['commission'].mean(), 2)
    cac_delta = round((cac_yesterday)/cac_yesterday_last_month*100,2)
    
    user_payments['payment_month'] = user_payments['payment_date'].dt.to_period('M')
    merged_users = user_payments.merge(users, how = 'inner', on='user_id')
    
    retention_abs = merged_users.groupby(['cogort', 'payment_month']).agg({'user_id': 'nunique'}).unstack(fill_value=0)
    retention_abs.columns = retention_abs.columns.droplevel()
    retention_abs = retention_abs.reset_index()
    retention_abs.columns.name = None
    cogort_sizes = users.groupby('cogort').agg({'user_id': 'size'})
    retention_rel = retention_abs.set_index('cogort').div(cogort_sizes['user_id'], axis=0)
    cogort_sizes = cogort_sizes.reset_index()
    
    retention_rel = retention_rel.rename(columns=lambda x: f"{x}_ret" if x != "cogort" else x)
    
    ltr_results_yesterday = []
    
    for i in range(11):
        start_col = i + 1
        sum_of_six = retention_rel.iloc[i, start_col:start_col+6].sum()
        ltr_results_yesterday.append(sum_of_six)
        
    avg_ltr_yesterday = sum(ltr_results_yesterday) / len(ltr_results_yesterday)
    pred_ltr_yesterday = round(avg_ltr_yesterday * arpu, 2)
    
    ltr_results_yesterday_last_month = []
    
    for j in range(10):
        start_col = j + 1
        sum_of_six = retention_rel.iloc[j, start_col:start_col+6].sum()
        ltr_results_yesterday_last_month.append(sum_of_six)
        
    avg_ltr_yesterday_last_month = sum(ltr_results_yesterday_last_month) / len(ltr_results_yesterday_last_month)
    pred_ltr_yesterday_last_month = round(avg_ltr_yesterday_last_month * arpu, 2)
    pred_ltr_delta = round((pred_ltr_yesterday)/pred_ltr_yesterday_last_month*100,2)
    symbol_pred_ltr_delta = "\U0001F4C8"+"\U00002705" if pred_ltr_delta > 100 else "\U0001F4C9"+"\U0001F53B"
    
    
    pred_ltv_yesterday =  pred_ltr_yesterday - cac_yesterday
    pred_ltv_yesterday_last_month = pred_ltr_yesterday_last_month - cac_yesterday_last_month
    pred_ltv_delta = round((pred_ltv_yesterday)/pred_ltv_yesterday_last_month*100,2)
    symbol_pred_ltv_delta = "\U0001F4C8"+"\U00002705" if pred_ltv_delta > 100 else "\U0001F4C9"+"\U0001F53B"

    CAC = [cac_yesterday, cac_yesterday_last_month]
    LTV = [pred_ltv_yesterday, pred_ltv_yesterday_last_month]

    # Среднее время сессии
    avg_session_time_last_month = user_activity[(user_activity['watching_day'] >= yesterday_last_month) & 
                                            (user_activity['watching_day'] <= yesterday)]
    avg_session_time_last_month_grouped = avg_session_time_last_month.groupby(avg_session_time_last_month['watching_day']).agg({'viewing_time': 'mean'})
    avg_session_time_last_month_grouped = avg_session_time_last_month_grouped.rename(columns={'viewing_time': 'avg_session_time'}).reset_index()

    # Досматриваемость
    merged_activity_and_title_last_month = merged_activity_and_title[(merged_activity_and_title['watching_day'] >= yesterday_last_month) &
                                                                 (merged_activity_and_title['watching_day'] <= yesterday)]
    daily_inspection = merged_activity_and_title_last_month.groupby(merged_activity_and_title_last_month['watching_day']).agg({'inspection': 'mean'})
    daily_inspection = daily_inspection.rename(columns={'inspection': 'avg_inspection'}).reset_index()

    # Уникальное количество смотревших
    unique_viewers_daily = merged_activity_and_title_last_month.groupby(merged_activity_and_title_last_month['watching_day']).agg({'user_id': 'nunique'})
    unique_viewers_daily = unique_viewers_daily.rename(columns={'user_id': 'unique_viewers'}).reset_index()

    # Среднее число просмотров на пользователя
    views_last_month = user_activity[(user_activity['watching_day'] >= yesterday_last_month) & 
                                    (user_activity['watching_day'] <= yesterday)]
    views_grouped = views_last_month.groupby('watching_day').agg({'play_start': 'count', 'user_id': 'nunique'}).reset_index()
    views_grouped['avg_views_per_user'] = round(views_grouped['play_start'] / views_grouped['user_id'], 2)

    # Доля новых платящих пользователей
    new_paying_users_last_month = user_payments[(user_payments['is_trial'] == 1) & 
                                            (user_payments['payment_day'] >= yesterday_last_month) & 
                                            (user_payments['payment_day'] <= yesterday)]
    new_paying_users_grouped = new_paying_users_last_month.groupby('payment_day').agg({'user_id': 'nunique'}).rename(columns={'user_id': 'new_paying_users'}).reset_index()

    paying_users_last_month = user_payments[(user_payments['payment_day'] >= yesterday_last_month) & 
                                            (user_payments['payment_day'] <= yesterday)]
    paying_users_grouped = paying_users_last_month.groupby('payment_day').agg({'user_id': 'nunique'}).rename(columns={'user_id': 'paying_users'}).reset_index()

    new_paying_users_rate = pd.merge(new_paying_users_grouped, paying_users_grouped, on='payment_day')
    new_paying_users_rate['new_paying_users_rate'] = round((new_paying_users_rate['new_paying_users'] / new_paying_users_rate['paying_users']) * 100, 2)


    # Функции для графиков
    def simple_line_plot(title, x_series, y_series, file_name):
        plt.figure(figsize=(10, 6))
        plt.plot(x_series, y_series, color='teal', label=title)
        plt.title(title)
        plt.xlabel('Дата')
        plt.ylabel('Значение')
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.savefig(file_name)
        plt.show()


    def combined_plot(title, x_series, y1_series, y2_series, file_name):
        fig, ax1 = plt.subplots(figsize=(12, 6))

        ax1.plot(x_series, y1_series, color='teal', label='Линейный график')
        ax1.set_xlabel('Дата')
        ax1.set_ylabel('Линейный график')
        ax1.tick_params(axis='y')
        ax1.legend(loc="upper left")

        ax2 = ax1.twinx()
        ax2.bar(x_series, y2_series, color='darkcyan', alpha=0.3, label='Столбчатый график')
        ax2.set_ylabel('Столбчатый график')
        ax2.tick_params(axis='y')
        ax2.legend(loc="upper right")

        plt.title(title)
        fig.tight_layout()
        plt.savefig(file_name)
        plt.show()
        

    def stacked_bar_chart(title, x_series, y1_series, y2_series, file_name='stacked_bar_chart.png'):
        plt.figure(figsize=(8, 6))
        plt.bar(x_series, y1_series, color='gray', label='CAC')
        plt.bar(x_series, y2_series, bottom=y1_series, color='teal', label='LTV')
        plt.title(title)
        plt.xlabel('Дата')
        plt.ylabel('LTR')
        plt.legend()
        plt.tight_layout()
        plt.savefig(file_name)
        plt.show()


    simple_line_plot("Количество триалов", trials_last_month_grouped['payment_day'], trials_last_month_grouped['trial_count'], 'simple_plot_trial.png')
    simple_line_plot("Cash-in", cash_in_daily['payment_day'], cash_in_daily['cash_in'], 'simple_plot_cash_in.png')
    simple_line_plot("Уникальные посетители", unique_viewers_daily['watching_day'], unique_viewers_daily['unique_viewers'], 'simple_plot_unique_viewers.png')

    combined_plot("Количество оплат и конверсия", payments_non_trial_last_month_grouped['payment_day'], payments_non_trial_last_month_grouped['payment_count'],
                conversion_df['conversion'], 'combined_plot_conversion.png')
    combined_plot("Среднее время сессии и досматриваемость", daily_inspection['watching_day'], avg_session_time_last_month_grouped['avg_session_time'],
                daily_inspection['avg_inspection'], 'combined_plot_inspection.png')

    stacked_bar_chart("LTR = LTV + CAC", ["yesterday", "yesterday_last_month"], [CAC[0], CAC[1]], [LTV[0], LTV[1]])

    simple_line_plot("Среднее число просмотров на пользователя", views_grouped['watching_day'], views_grouped['avg_views_per_user'], 'simple_plot_avg_views_per_users.png')
    simple_line_plot("Доля новых платящих пользователей", new_paying_users_rate['payment_day'], new_paying_users_rate['new_paying_users_rate'], 'simple_plot_new_paying_users.png')

    image_path_trial = 'simple_plot_trial.png'
    image_trial = open(image_path_trial, 'rb')

    image_path_conversion = 'combined_plot_conversion.png'
    image_conversion = open(image_path_conversion, 'rb')

    image_path_cash_in = 'simple_plot_cash_in.png'
    image_cash_in = open(image_path_cash_in, 'rb')

    image_path_ltr = 'stacked_bar_chart.png'
    image_ltr = open(image_path_ltr, 'rb')

    image_path_inspection = 'combined_plot_inspection.png'
    image_inspection = open(image_path_inspection, 'rb')

    image_path_unique = 'simple_plot_unique_viewers.png'
    image_unique = open(image_path_unique, 'rb')

    image_path_avg_views_per_users = 'simple_plot_avg_views_per_users.png'
    image_avg_views_per_users = open(image_path_avg_views_per_users, 'rb')

    image_path_new_paying_users = 'simple_plot_new_paying_users.png'
    image_new_paying_users = open(image_path_new_paying_users, 'rb')

    return message, image_trial, image_conversion, image_cash_in, image_ltr, image_inspection, image_unique, image_avg_views_per_users, image_new_paying_users
