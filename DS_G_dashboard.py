# First, install the library:
# pip install streamlit pandas

# How to run the Dashboard
# Streamlit has its own server. Instead of python dashboard.py, you run:
# python -m streamlit run DS_G_dashboard.py

import streamlit as st
import pandas as pd
import sqlite3
import time

# --- CONFIG ---
DB_NAME = 'laptop_metrics.db'

st.set_page_config(page_title="Laptop Streaming Monitor", layout="wide")
st.title("🚀 Real-Time Hardware Telemetry")

# Function to fetch data from our SQLite Sink
def get_data():
    conn = sqlite3.connect(DB_NAME)
    # We grab the last 60 seconds / records
    query = "SELECT * FROM system_stats ORDER BY timestamp DESC LIMIT 60"
    df = pd.read_sql_query(query, conn)
    conn.close()
    # Reverse so the chart flows left-to-right
    return df.iloc[::-1]

# --- DASHBOARD LAYOUT ---
placeholder = st.empty()

while True:
    df = get_data()
    
    with placeholder.container():
        # 1. Metric Cards (The "At a Glance" view)
        col1, col2, col3 = st.columns(3)
        latest = df.iloc[-1]
        
        col1.metric("Current CPU", f"{latest['cpu_pct']}%")
        col2.metric("Memory Usage", f"{latest['mem_pct']}%")
        col3.metric("Battery", f"{latest['battery_pct']}%")

        # 2. The Trend Line (Visualizing the Stream)
        st.subheader("System Load Over Time (Last 60 Samples)")
        # We use st.line_chart for a quick, clean DE-style visualization
        chart_data = df.set_index('timestamp')[['cpu_pct', 'mem_pct']]
        st.line_chart(chart_data)

        # 3. Raw Data (For the "Audit Trail")
        with st.expander("View Raw Log"):
            st.write(df.tail(10))

    # Refresh every 2 seconds
    time.sleep(2)

