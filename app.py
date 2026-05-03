import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title='GA4 Realtime Dashboard', layout='wide')
st.markdown("""<style>.stApp{background:white;color:#0e1117}</style>""", unsafe_allow_html=True)
st.title('GA4 Realtime Attribution Dashboard')

session = get_active_session()

def run_sql(q):
    return session.sql(q).to_pandas()

# KPI Cards
kpi = run_sql("""
select 
coalesce(sum(purchase_value),0) as total_revenue,
count(*) as total_events,
coalesce(count_if(event_name='purchase'),0) as purchases
from int_events_flat
where event_date >= current_date - 2500
""")

c1,c2,c3 = st.columns(3)
c1.metric('Revenue (5 years)', f"${float(kpi.iloc[0]['TOTAL_REVENUE']):,.0f}")
c2.metric('Events (5 years)', int(kpi.iloc[0]['TOTAL_EVENTS']))
c3.metric(
    'Purchases (5 years)',
    int(0 if pd.isna(kpi.iloc[0]['PURCHASES']) else kpi.iloc[0]['PURCHASES'])
)

# Charts
left,right = st.columns(2)
with left:
    trend = run_sql("""
    select event_date, coalesce(sum(purchase_value),0) as revenue
    from int_events_flat
    where event_date >= current_date - 2500
    group by 1 order by 1
    """)
    fig = px.line(trend, x='EVENT_DATE', y='REVENUE', title='5 Years Revenue Trend', markers=True)
    st.plotly_chart(fig, use_container_width=True)

with right:
    ch = run_sql("""
    select coalesce(traffic_source_source,'(direct)') as channel,
    count(*) as events
    from int_events_flat
    where event_date >= current_date - 2500
    group by 1 order by 2 desc limit 10
    """)
    fig2 = px.pie(ch, names='CHANNEL', values='EVENTS', title='Channel Breakdown')
    st.plotly_chart(fig2, use_container_width=True)

# Attribution Comparison
st.subheader('First vs Last Attribution Totals')
att = run_sql("""
select 'First Click' as model, coalesce(sum(purchase_value),0) as revenue from first_click
union all
select 'Last Click' as model, coalesce(sum(purchase_value),0) as revenue from last_click
""")
fig3 = px.bar(att, x='MODEL', y='REVENUE', title='Attribution Comparison')
st.plotly_chart(fig3, use_container_width=True)

# Live Feed
st.subheader('Live Streamed Events')
feed = run_sql("""
select event_ts, user_pseudo_id, event_name, purchase_value
from int_events_flat
order by event_ts desc limit 20
""")
st.dataframe(feed, use_container_width=True, height=420)

st.caption(f"Last updated: {datetime.utcnow().isoformat()} UTC")