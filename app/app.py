import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit.components.v1 as components
from prophet import Prophet
import subprocess
import glob
import shutil
import os
import time
import jwt 

# --- 1. SYSTEM CONFIG ---
st.set_page_config(
    page_title="Air Quality Lakehouse",
    page_icon="🌫️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS STYLING ---
# --- CSS STYLING ---
st.markdown("""
<style>
    /* Main Background */
    .stApp {
        background-color: #FFFFFF;
        font-family: 'Segoe UI', sans-serif;
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #F8F9FA;
        border-right: 1px solid #E5E7EB;
    }
    
    /* --- CHỈNH SỬA QUAN TRỌNG TẠI ĐÂY: METRIC CARD --- */
    div[data-testid="stMetric"] {
        background-color: #FFFFFF;
        border: 1px solid #E5E7EB;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        
        /* Cố định chiều cao để các ô đều nhau */
        height: 140px; 
        min-height: 140px;
        
        /* Căn giữa nội dung trong ô */
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    
    div[data-testid="stMetric"] label {
        font-size: 15px;
        color: #6B7280;
        margin-bottom: 5px;
    }
    
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 700;
        color: #111827;
    }
    
    /* Radio Button & Header giữ nguyên */
    div.stRadio > div { background-color: transparent; }
    div.stRadio > div > label {
        padding: 10px 15px; border-radius: 6px; cursor: pointer;
        color: #4B5563; font-weight: 500;
    }
    div.stRadio > div > label:hover { background-color: #E5E7EB; color: #111827; }
    
    h1 { font-size: 26px; font-weight: 700; color: #111827; margin-bottom: 0.5rem; }
    h3 { font-size: 18px; font-weight: 600; color: #374151; margin-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# --- STATE MANAGEMENT ---
if 'bronze_on' not in st.session_state: st.session_state['bronze_on'] = False
if 'silver_on' not in st.session_state: st.session_state['silver_on'] = False
if 'gold_on' not in st.session_state:   st.session_state['gold_on'] = False

def run_docker_cmd(command_list):
    try:
        cmd_final = command_list.copy()
        if "exec" in cmd_final:
            exec_index = cmd_final.index("exec")
            if "-T" not in cmd_final:
                cmd_final.insert(exec_index + 1, "-T")
        process = subprocess.run(cmd_final, capture_output=True, text=True, encoding="utf-8")
        return process
    except Exception as e:
        return None

def handle_toggle_change(layer, dag_id):
    widget_key = f"toggle_{layer}"
    state_key = f"{layer}_on"
    new_state = st.session_state[widget_key]
    st.session_state[state_key] = new_state
    
    action = "unpause" if new_state else "pause"
    cmd = ["docker", "compose", "exec", "airflow-webserver", "airflow", "dags", action, dag_id]
    run_docker_cmd(cmd)

# --- DATA LOADER ---
@st.cache_data
def load_data():
    try:
        df = pd.read_csv("aqi_data.csv")
        df['measure_date'] = pd.to_datetime(df['measure_date'])
        df['year'] = df['measure_date'].dt.year
        df['month'] = df['measure_date'].dt.month
        return df
    except FileNotFoundError:
        return None

df_main = load_data()

# --- SIDEBAR ---
with st.sidebar:
    st.header("Air Quality Lakehouse") 
    st.write("")
    
    page = st.radio("MAIN MENU", 
        ["Dashboard", "Analytics", "Forecasting", "System Admin"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    if df_main is not None:
        st.caption("SYSTEM STATUS")
        st.success(f"Synced: {df_main['measure_date'].max().date()}")
    else:
        st.caption("SYSTEM STATUS")
        st.warning("No Data Available")

# ==============================================================================
# 1. DASHBOARD (TỔNG QUAN + CHẤT LƯỢNG DỮ LIỆU)
# ==============================================================================
if page == "Dashboard":
    st.title("Executive Dashboard")
    
    if df_main is None:
        st.warning("Data is missing. Please go to 'System Admin' to sync data.")
        st.stop()

    # --- TOP KPIS ---
    # Tính toán các chỉ số
    valid_days = len(df_main[df_main['data_quality_status'] == 'Valid'])
    total_days = len(df_main)
    completeness = (valid_days / total_days) * 100 if total_days > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Avg VN_AQI", f"{df_main['vn_aqi'].mean():.0f}")
    with col2:
        st.metric("Max PM2.5", f"{df_main['max_pm2_5'].max():.0f}", "µg/m³")
    with col3:
        # Hiển thị độ đầy đủ dữ liệu dưới dạng %
        st.metric("Data Integrity", f"{completeness:.1f}%", f"{total_days} days")
    with col4:
        # Đếm số ngày lỗi
        invalid = total_days - valid_days
        st.metric("Data Issues", f"{invalid}", "Days", delta_color="inverse")

    # --- MAIN CHARTS ---
    st.markdown("### Air Quality Trends")
    
    # Biểu đồ Xu hướng chính (Area Chart)
    fig_trend = px.area(df_main, x="measure_date", y="vn_aqi", 
                  title=None, template="plotly_white")
    fig_trend.update_traces(line_color='#2563EB', fillcolor='rgba(37, 99, 235, 0.1)')
    fig_trend.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300, xaxis_title=None, yaxis_title="VN_AQI")
    st.plotly_chart(fig_trend, use_container_width=True)

    # --- DATA QUALITY SECTION ---
    st.markdown("### Data Quality & Issues")
    
    c1, c2 = st.columns([2, 1])
    
    with c1:
        # Biểu đồ phân tán thể hiện mật độ thu thập
        fig_scatter = px.scatter(df_main, x="measure_date", y="record_count", color="data_quality_status",
                                color_discrete_map={"Valid": "#10B981", "Insufficient Data (<75%)": "#EF4444", "Missing PM Data": "#F59E0B"},
                                title="Daily Data Completeness (Standard: 24h)")
        fig_scatter.add_hline(y=18, line_dash="dot", line_color="red", annotation_text="Threshold (75%)")
        fig_scatter.update_layout(template="plotly_white", legend_title="Quality Status", height=300)
        st.plotly_chart(fig_scatter, use_container_width=True)
        
    with c2:
        # Bảng nhật ký lỗi (Chỉ hiện thị khi có lỗi)
        df_bad = df_main[df_main['data_quality_status'] != 'Valid'][['measure_date', 'record_count', 'data_quality_status']]
        if not df_bad.empty:
            st.warning(f"Found **{len(df_bad)}** invalid records")
            st.dataframe(df_bad, hide_index=True, use_container_width=True, height=220)
        else:
            st.success("✅ Perfect Data Quality! (100% Valid)")
            st.caption("No missing days or sensor failures detected.")

# ==============================================================================
# 2. ANALYTICS (METABASE)
# ==============================================================================
# ==============================================================================
# 2. ANALYTICS (METABASE EMBEDDED - MULTI DASHBOARD)
# ==============================================================================
elif page == "Analytics":
    st.title("Advanced Analytics Dashboard")
    st.caption("Hệ thống báo cáo phân tích chuyên sâu từ Data Lakehouse (Trino Engine)")

    # --- HÀM TẠO URL BẢO MẬT (Viết 1 lần dùng cho cả 3) ---
    def get_metabase_url(dashboard_id):
        METABASE_SITE_URL = "http://localhost:3000"
        METABASE_SECRET_KEY = "03e27f5f1882a259fc1b2ad38c948482566441ce1467a361ada436f1d1c7c89e"
        
        payload = {
            "resource": {"dashboard": dashboard_id},
            "params": {},
            "exp": round(time.time()) + (60 * 60) # Token sống 60 phút
        }
        token = jwt.encode(payload, METABASE_SECRET_KEY, algorithm="HS256")
        # bordered=false&titled=false để ẩn khung viền của Metabase cho đẹp
        return f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=false&titled=false"

    # --- GIAO DIỆN TABS ---
    # Tạo 3 Tabs cho 3 Dashboard
    tab1, tab2, tab3 = st.tabs([
        "Executive Overview", 
        "Temporal Patterns", 
        "Chemical Analysis"
    ])

    # --- TAB 1: DASHBOARD TỔNG QUAN (ID = 2) ---
    with tab1:
        st.header("Executive Air Quality Overview")
        try:
            url = get_metabase_url(2)
            components.iframe(url, height=1200, scrolling=True)
        except Exception as e:
            st.error(f"Lỗi hiển thị Dashboard 1: {e}")

    # --- TAB 2: DASHBOARD THỜI GIAN (ID = 3) ---
    with tab2:
        st.header("Temporal Patterns & Trends Analysis")
        try:
            url = get_metabase_url(3)
            components.iframe(url, height=1200, scrolling=True)
        except Exception as e:
            st.error(f"Lỗi hiển thị Dashboard 2: {e}")

    # --- TAB 3: DASHBOARD HÓA HỌC (ID = 4) ---
    with tab3:
        st.header("Chemical Composition & Source Analysis")
        try:
            url = get_metabase_url(4)
            components.iframe(url, height=1200, scrolling=True)
        except Exception as e:
            st.error(f"Lỗi hiển thị Dashboard 3: {e}")
        
# 3. FORECASTING
# ==============================================================================
elif page == "Forecasting":
    st.title("Forecast of air quality indexes")
    
    if df_main is not None:
        with st.container():
            c1, c2, c3 = st.columns([2, 2, 1])
            # Chọn chỉ số (Full 8 chất)
            target = c1.selectbox("Target Metric", 
                ["vn_aqi", "avg_pm2_5", "avg_pm10", "avg_co", "avg_no2", "avg_so2", "avg_o3"], 
                format_func=str.upper)
            days = c2.slider("Horizon (Days)", 7, 90, 30)
            c3.write("")
            c3.write("")
            run = c3.button("Run", type="primary", use_container_width=True)
        
        st.divider()
        
        if run:
            with st.spinner('Training Prophet Model...'):
                df_p = df_main[['measure_date', target]].rename(columns={'measure_date': 'ds', target: 'y'}).dropna()
                m = Prophet(daily_seasonality=True)
                m.fit(df_p)
                future = m.make_future_dataframe(periods=days)
                forecast = m.predict(future)
                
                fig = go.Figure()
                # Vẽ đường thực tế (Màu xám nhạt để làm nền)
                fig.add_trace(go.Scatter(x=df_p['ds'], y=df_p['y'], name='Actual', 
                                        line=dict(color='#E5E7EB', width=1.5)))
                # Vẽ đường dự báo (Màu chủ đạo)
                fig.add_trace(go.Scatter(x=forecast['ds'].tail(days), y=forecast['yhat'].tail(days), name='Forecast', 
                                        line=dict(color='#2563EB', width=2)))
                
                fig.update_layout(template="plotly_white", hovermode="x unified", height=500, 
                                  title=f"{target.upper()} Forecast ({days} days)")
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Please sync data first.")

# ==============================================================================
# 4. SYSTEM ADMIN (PIPELINE CONTROL)
# ==============================================================================
elif page == "System Admin":
    st.title("System Administration")
    
    # --- PHẦN 1: ORCHESTRATION ---
    st.subheader("1. Pipeline Orchestration")
    st.caption("Control Airflow DAGs state.")
    
    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.markdown("**Bronze (Ingest)**")
        st.toggle("Active", key="toggle_bronze", 
                  value=st.session_state['bronze_on'], 
                  on_change=handle_toggle_change, args=("bronze", "ingest_batch_owm"))
    
    with c2:
        st.markdown("**Silver (Transform)**")
        st.toggle("Active", key="toggle_silver", 
                  value=st.session_state['silver_on'], 
                  on_change=handle_toggle_change, args=("silver", "transform_silver_owm"))
        
    with c3:
        st.markdown("**Gold (Aggregate)**")
        st.toggle("Active", key="toggle_gold", 
                  value=st.session_state['gold_on'], 
                  on_change=handle_toggle_change, args=("gold", "transform_gold_fact"))

    st.write("")
    st.write("")

    # --- PHẦN 2: DATA SYNC ---
    st.subheader("2. Data Synchronization")
    
    c_info, c_btn = st.columns([3, 1])
    c_info.info("Sync latest data from Gold Layer (Iceberg) to Application Layer.")
    
    if c_btn.button("Sync Now", type="primary", use_container_width=True):
        status = st.status("Processing...", expanded=True)
        try:
            status.write("Sending job to Spark Cluster...")
            # Lệnh chạy Spark
            cmd = [
                "docker", "compose", "exec", "spark-master", 
                "/opt/spark/bin/spark-submit", 
                "--master", "local[*]", 
                "--conf", "spark.driver.extraClassPath=/opt/spark/extra-jars/hadoop-aws-3.3.4.jar:/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar:/opt/spark/extra-jars/postgresql-42.7.3.jar", 
                "/opt/spark/scripts/export_gold_to_csv.py"
            ]
            # Thêm encoding utf-8 và fix lỗi -T trong hàm run_docker_cmd
            res = run_docker_cmd(cmd)
            
            if res and res.returncode == 0:
                # Logic copy file
                EXPORT_DIR = "../spark/scripts/gold_data_export" 
                TARGET_FILE = "aqi_data.csv"
                files = glob.glob(f"{EXPORT_DIR}/*.csv")
                
                if files:
                    latest = max(files, key=os.path.getctime)
                    shutil.copy(latest, TARGET_FILE)
                    status.update(label="Synced Successfully!", state="complete", expanded=False)
                    time.sleep(1)
                    st.cache_data.clear()
                    st.rerun()
                else:
                    status.update(label="Error: File not found", state="error")
            else:
                status.update(label="Job Failed", state="error")
                if res: st.error(res.stderr)
        except Exception as e:
            status.update(label="System Error", state="error")
            st.error(str(e))