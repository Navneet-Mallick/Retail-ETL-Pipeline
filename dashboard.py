import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import config

st.set_page_config(page_title="Retail Sales Dashboard", page_icon="🛒",
                   layout="wide", initial_sidebar_state="expanded")

COLORS = ["#6366f1", "#8b5cf6", "#ec4899", "#f59e0b", "#10b981"]
TEMPLATE = "plotly_dark"
MONTH_NAMES = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
               7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
MARGIN = dict(l=0, r=0, t=10, b=0)


def layout(fig, height=280):
    fig.update_layout(template=TEMPLATE, height=height, margin=MARGIN)
    return fig


def pie(labels, values, colors=COLORS, hole=0.45):
    fig = go.Figure(go.Pie(labels=labels, values=values,
                           hole=hole, marker_colors=colors, textinfo="label+percent"))
    return layout(fig).update_layout(showlegend=False)


@st.cache_data
def load_data():
    with sqlite3.connect(config.DB_FILE) as conn:
        df = pd.read_sql("SELECT * FROM sales_clean", conn)
    df["Date"] = pd.to_datetime(df["Date"])
    return df

@st.cache_data
def load_quality():
    try:
        return pd.read_csv(config.OUTPUT_QUALITY)
    except FileNotFoundError:
        return pd.DataFrame()


df = load_data()
quality_df = load_quality()

# sidebar
st.sidebar.title("Retail Analytics")
st.sidebar.divider()
sel_category = st.sidebar.selectbox("Category", ["All"] + sorted(df["Product Category"].unique()))
sel_gender   = st.sidebar.selectbox("Gender",   ["All"] + sorted(df["Gender"].unique()))
sel_months   = st.sidebar.slider("Month Range", 1, 12, (1, 12))
st.sidebar.divider()
if st.sidebar.button("Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()
if not quality_df.empty:
    st.sidebar.caption(f"Last run: {quality_df.iloc[-1]['run_timestamp']}")
st.sidebar.caption(f"Pipeline runs: {len(quality_df)}")

# filter
fdf = df.copy()
if sel_category != "All": fdf = fdf[fdf["Product Category"] == sel_category]
if sel_gender   != "All": fdf = fdf[fdf["Gender"]           == sel_gender]
fdf = fdf[(fdf["Month"] >= sel_months[0]) & (fdf["Month"] <= sel_months[1])]

# header + KPIs
st.title("Retail Sales Analytics")
st.caption("Batch ETL Pipeline · SQLite · pandas")
st.divider()

rev   = fdf["Total Amount"].sum()
avg   = fdf["Total Amount"].mean() if not fdf.empty else 0
top   = fdf.groupby("Product Category")["Total Amount"].sum().idxmax() if not fdf.empty else "N/A"

c = st.columns(5)
c[0].metric("Total Revenue",    f"${rev:,.0f}")
c[1].metric("Transactions",     f"{len(fdf):,}")
c[2].metric("Avg Order Value",  f"${avg:,.0f}")
c[3].metric("Unique Customers", f"{fdf['Customer ID'].nunique():,}")
c[4].metric("Top Category",     top)
st.divider()

# revenue trend + heatmap
c1, c2 = st.columns([2, 1])
with c1:
    show_ma = st.checkbox("Show 7-day moving average", value=True)
    st.subheader("Daily Revenue Trend")
    daily = fdf.groupby(fdf["Date"].dt.date)["Total Amount"].sum().reset_index()
    daily.columns = ["Date", "Revenue"]
    daily["MA7"] = daily["Revenue"].rolling(7, min_periods=1).mean()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily["Date"], y=daily["Revenue"], name="Daily Revenue",
                             fill="tozeroy", fillcolor="rgba(99,102,241,0.15)",
                             line=dict(color="#6366f1", width=1.5), hovertemplate="$%{y:,.0f}"))
    if show_ma:
        fig.add_trace(go.Scatter(x=daily["Date"], y=daily["MA7"], name="7-day MA",
                                 line=dict(color="#f59e0b", width=2, dash="dot"),
                                 hovertemplate="$%{y:,.0f}"))
    layout(fig, 300).update_layout(legend=dict(orientation="h", y=1.1),
                                   hovermode="x unified", yaxis_title="Revenue ($)")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Monthly Revenue Heatmap")
    monthly = fdf.groupby(["Year", "Month"])["Total Amount"].sum().reset_index()
    monthly["MonthName"] = monthly["Month"].map(MONTH_NAMES)
    fig = px.density_heatmap(monthly, x="MonthName", y="Year", z="Total Amount",
                             color_continuous_scale="Viridis",
                             category_orders={"MonthName": list(MONTH_NAMES.values())})
    layout(fig, 300).update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

# category / gender / age group
c1, c2, c3 = st.columns(3)
with c1:
    ct = st.radio("Revenue by Category", ["Bar", "Pie"], horizontal=True)
    cat_rev = fdf.groupby("Product Category")["Total Amount"].sum().reset_index()
    if ct == "Bar":
        fig = px.bar(cat_rev, x="Product Category", y="Total Amount",
                     color="Product Category", color_discrete_sequence=COLORS, text_auto=".2s")
        layout(fig).update_layout(showlegend=False, yaxis_title="Revenue ($)")
        fig.update_traces(textposition="outside")
    else:
        fig = pie(cat_rev["Product Category"], cat_rev["Total Amount"])
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Revenue by Gender")
    gen_rev = fdf.groupby("Gender")["Total Amount"].sum().reset_index()
    st.plotly_chart(pie(gen_rev["Gender"], gen_rev["Total Amount"],
                        colors=["#6366f1","#ec4899"], hole=0.55),
                    use_container_width=True)

with c3:
    at = st.radio("Revenue by Age Group", ["Bar", "Pie"], horizontal=True)
    age_rev = fdf.groupby("Age Group")["Total Amount"].sum().reset_index()
    age_order = ["18-25","26-35","36-45","46-60","60+"]
    age_rev["Age Group"] = pd.Categorical(age_rev["Age Group"], categories=age_order, ordered=True)
    age_rev = age_rev.sort_values("Age Group")
    if at == "Bar":
        fig = px.bar(age_rev, x="Age Group", y="Total Amount",
                     color="Total Amount", color_continuous_scale="Purples", text_auto=".2s")
        layout(fig).update_layout(coloraxis_showscale=False, yaxis_title="Revenue ($)")
        fig.update_traces(textposition="outside")
    else:
        fig = pie(age_rev["Age Group"], age_rev["Total Amount"])
    st.plotly_chart(fig, use_container_width=True)

# units by month + category x gender
c1, c2 = st.columns(2)
with c1:
    ut = st.radio("Units Sold by Category per Month", ["Line", "Bar"], horizontal=True)
    cat_month = fdf.groupby(["Month","Product Category"])["Quantity"].sum().reset_index()
    cat_month["MonthName"] = cat_month["Month"].map(MONTH_NAMES)
    order = {"MonthName": list(MONTH_NAMES.values())}
    if ut == "Line":
        fig = px.line(cat_month, x="MonthName", y="Quantity", color="Product Category",
                      markers=True, color_discrete_sequence=COLORS, category_orders=order)
    else:
        fig = px.bar(cat_month, x="MonthName", y="Quantity", color="Product Category",
                     barmode="group", color_discrete_sequence=COLORS, category_orders=order)
    layout(fig, 300).update_layout(yaxis_title="Units", legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Category Revenue by Gender")
    cat_gen = fdf.groupby(["Product Category","Gender"])["Total Amount"].sum().reset_index()
    fig = px.bar(cat_gen, x="Product Category", y="Total Amount", color="Gender",
                 barmode="group", text_auto=".2s",
                 color_discrete_map={"Male":"#6366f1","Female":"#ec4899"})
    layout(fig, 300).update_layout(yaxis_title="Revenue ($)", legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

# distributions
c1, c2 = st.columns(2)
with c1:
    st.subheader("Order Value Distribution")
    fig = px.histogram(fdf, x="Total Amount", nbins=30, color="Product Category",
                       color_discrete_sequence=COLORS, opacity=0.8, barmode="overlay")
    layout(fig).update_layout(xaxis_title="Order Value ($)", yaxis_title="Count",
                               legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Customer Age Distribution")
    fig = px.histogram(fdf, x="Age", nbins=25, color="Gender", opacity=0.8, barmode="overlay",
                       color_discrete_map={"Male":"#6366f1","Female":"#ec4899"})
    layout(fig).update_layout(xaxis_title="Age", yaxis_title="Count",
                               legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

# data quality
st.divider()
st.subheader("Pipeline Data Quality")

if not quality_df.empty:
    latest = quality_df.iloc[-1]
    c = st.columns(5)
    for col, label, key in zip(c,
        ["Input Rows","Duplicates Dropped","Nulls Dropped","Type Errors","Clean Output"],
        ["total_rows_input","duplicate_rows_dropped","rows_dropped_null_required",
         "rows_dropped_type_conversion","rows_output_clean"]):
        col.metric(label, f"{int(latest[key]):,}")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Issues Breakdown")
        issues = {k: int(latest[v]) for k, v in {
            "Duplicates":    "duplicate_rows_dropped",
            "Nulls":         "rows_dropped_null_required",
            "Type Errors":   "rows_dropped_type_conversion",
            "Business Rules":"rows_dropped_business_rules",
        }.items() if int(latest[v]) > 0}
        if issues:
            fig = pie(list(issues.keys()), list(issues.values()),
                      colors=["#f59e0b","#ef4444","#8b5cf6","#3b82f6"], hole=0.5)
            fig.update_traces(textinfo="label+value")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("No data quality issues detected.")

    with c2:
        st.subheader("Run History")
        qc = quality_df.copy()
        qc["run"] = range(1, len(qc) + 1)
        fig = go.Figure([
            go.Bar(x=qc["run"], y=qc["total_rows_input"],   name="Input", marker_color="#6366f1"),
            go.Bar(x=qc["run"], y=qc["rows_output_clean"],  name="Clean", marker_color="#10b981"),
        ])
        layout(fig).update_layout(barmode="group", xaxis_title="Run #", yaxis_title="Rows",
                                   legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, use_container_width=True)

st.divider()
st.caption("Retail Sales ETL Pipeline · SQLite · pandas · Streamlit · Plotly")
