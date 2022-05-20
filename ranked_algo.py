import os
from dotenv import load_dotenv
import snowflake.connector
import json
import requests
import copy
from collections import OrderedDict
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide")

load_dotenv()

conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USERNAME"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_HOST"),
                role=os.getenv("SNOWFLAKE_ROLE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database='TOKEN_FLOW',
                schema='DICU',
                protocol='https',
                port=443
                )

# Create a cursor object.
cur = conn.cursor()
conn.cursor().execute("USE ROLE ETL")

# get ranked_polls from GovAlpha/DUX
url = "https://governance-portal-v2.vercel.app/api/polling/all-polls"
r = requests.get(url)
res = r.json()
polls = res['polls']

ranked_polls = []
for poll in polls:
    if poll['voteType'] == 'Ranked Choice IRV':
        ranked_polls.append(str(poll['pollId']))

# ranked_polls = ['735', '705', '691', '609', '574', '510', '501', '502', '497', '498', '463', '465', '379', '380', '373', '328', '325', '323', '303', '285', '295', '297', '281', '279', '273', '258', '249', '248', '244', '240', '241', '219', '217', '218', '215', '206', '207', '194', '186', '165']
# ranked_polls = ['691']

"""
    RANKED POLL VOTES FLOW
"""

option = st.selectbox(
     'PICK THE POLL',
     ranked_polls)

st.write('SELECTED POLL:', option)

# ranked_polls_lookup = ','.join([f"'{poll}'" for poll in ranked_polls])
# print(ranked_polls_lookup)

# Get options forom yays table
polls_metadata = cur.execute(f"""
    select code, parse_json(options)::string as options
    from mcd.internal.yays
    where type = 'poll'
    and code in ('{option}');
""").fetchall()


for code, options in polls_metadata:

    # get total voting power of voters that took part in the poll
    total_votes_weight = cur.execute(f"""
        select sum(dapproval)
        from mcd.public.votes
        where yay = '{code}' and
        operation = 'FINAL_CHOICE';
    """).fetchone()[0]

    options_set = json.loads(options)
    options_layout = dict()
    for option in options_set:
        options_layout.setdefault(option, 0)

    poll_results = cur.execute(f"""
        select voter, option, dapproval
        from mcd.public.votes
        where yay = '{code}' and
        operation = 'FINAL_CHOICE';
    """).fetchall()

    # create round schema & append options layout to every round
    # options layout: all possible options to pick for poll
    rounds = OrderedDict()
    for round in range(0, len(options_set)):
        # append a copy of options layout to round
        rounds.setdefault(str(round), copy.deepcopy(options_layout))

    for voter, option, dapproval in poll_results:
        user_ranked_choices = option.split(',')
        round = 0
        while round <= len(user_ranked_choices) -1:
            rounds[str(round)][str(user_ranked_choices[round])] += dapproval
            round += 1

    # ALGO STARTS HERE
    voters = list()
    for voter, user_choices, dapproval in poll_results:
        voters.append([voter, dapproval])
    
    df = pd.DataFrame(voters)
    df.columns =['voter', 'power']
    print('VOTERS & POWER')
    print(df)
    print()

    available_options = list()
    for voter, user_choices, dapproval in poll_results:
        for i in user_choices.split(','):
            if i not in available_options:
                available_options.append(i)
    
    eliminated_options = list()

    poll_algo_rounds = list()
    for pointer in range(0, len(options_set)):

        # add round (category) column to df
        df[f'round_{pointer}'] = ''
        category = f'round_{pointer}'
        poll_algo_rounds.append(category)

        final_results = dict()
        final_results.setdefault(str(pointer), {})
        for i in available_options:
            if i not in eliminated_options:
                final_results[str(pointer)].setdefault(str(i), 0)

        print(f"""STARTING ROUND: {pointer}""")

        # counting the support for options
        for voter, user_choices, dapproval in poll_results:
            for i in user_choices.split(','):
                if i not in eliminated_options:
                    final_results.setdefault(str(pointer), {})
                    final_results[str(pointer)].setdefault(str(i), 0)
                    final_results[str(pointer)][str(i)] += dapproval

                    print(options_set[str(i)])
                    df.at[df.index[df['voter'] == voter][0], category] = options_set[str(i)]

                    break

        # override the 'abstain' option
        for voter, user_choices, dapproval in poll_results:
            if len(user_choices.split(',')) == 1 and user_choices.split(',')[0] == '0':
                df.at[df.index[df['voter'] == voter][0], category] = '0'

        r = list()
        for option in final_results[str(pointer)]:
            r.append(final_results[str(pointer)][option])
        ordered_results = sorted(r)

        for option in final_results[str(pointer)]:
            if final_results[str(pointer)][option] == ordered_results[0]:
                if pointer < len(options_set) -1:
                    print(f"""eliminating least supported option: {option}""")
                    least_supported_option = option
                    eliminated_options.append(least_supported_option)
                    c = 0
                    while c <= len(available_options) -1:
                        if str(available_options[c]) == least_supported_option:
                            available_options.pop(c)
                        c += 1

        print(f"ROUND {pointer} SUMMARY")
        for voter, user_choices, dapproval in poll_results:
            if len(user_choices.split(',')) == 1 and user_choices.split(',')[0] == '0':
                final_results[str(pointer)]['0'] = 0
                final_results[str(pointer)]['0'] += dapproval

        print(final_results)
        print(f"""eliminated options: {eliminated_options}""")
        print(f"""available options: {available_options}""")
        print()

        pointer += 1

    df_x = df.replace([''], np.nan)
    df_y = df_x.dropna(how='all', axis=1)
    df1 = df_y.replace([np.nan], 'No vote')
    print(df1)
    print()

    poll_algo_rounds = list()
    for i in df1.columns:
        if i[:6] == 'round_':
            poll_algo_rounds.append(i)

# VIZ
fig = px.parallel_categories(
    df1[['power'] + poll_algo_rounds],
    dimensions=poll_algo_rounds,
    color="power",
    color_continuous_scale=px.colors.sequential.Inferno,
)

# dims = list()
# for dim in poll_algo_rounds:
#     dims.append(go.parcats.Dimension(values=df1[dim], label=dim))

# # Create parcats trace
# color = df1.power

# fig = go.Figure(
#     data = [go.Parcats(
#         dimensions=dims,
#         line={'color': color, 'colorscale': px.colors.sequential.Inferno},
#         hoveron='color', hoverinfo='count',
#         labelfont={'size': 18, 'family': 'Times'},
#         tickfont={'size': 16, 'family': 'Times'},
#         arrangement='freeform')]
#     )

# fig.update_layout(
#     autosize=False,
#     width=1800,
#     height=1000,
#     margin=dict(
#         l=250,
#         r=250,
#         b=250,
#         t=250,
#         pad=100
#     )
# )

st.plotly_chart(fig)