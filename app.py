import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunRealtimeReportRequest, Dimension, Metric, MinuteRange
from google.oauth2 import service_account
import time
from datetime import datetime
from streamlit_cookies_manager import EncryptedCookieManager
import json
import pytz

# --- CẤU HÌNH ---
PROPERTY_ID = "501726461"
REFRESH_INTERVAL_SECONDS = 60

# Tải các quy tắc mapping
try:
    with open('marketer_mapping.json', 'r', encoding='utf-8') as f:
        marketer_map = json.load(f)
except FileNotFoundError:
    st.error("Lỗi: Không tìm thấy file marketer_mapping.json.")
    st.stop()

TIMEZONE_MAPPINGS = {
    "Viet Nam (UTC+7)": "Asia/Ho_Chi_Minh", "New York (UTC-4)": "America/New_York",
    "Chicago (UTC-5)": "America/Chicago", "Denver (UTC-6)": "America/Denver",
    "Los Angeles (UTC-7)": "America/Los_Angeles", "Anchorage (UTC-8)": "America/Anchorage",
    "Honolulu (UTC-10)": "Pacific/Honolulu"
}

# --- XÁC THỰC VÀ COOKIES ---
cookies = EncryptedCookieManager(password=st.secrets["cookie"]["encrypt_key"])

def check_credentials(username, password):
    correct_username = st.secrets["credentials"]["username"]
    correct_password = st.secrets["credentials"]["password"]
    return username == correct_username and password == correct_password

# --- KẾT NỐI GOOGLE ANALYTICS ---
try:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["google_credentials"],
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    client = BetaAnalyticsDataClient(credentials=credentials)
except Exception as e:
    st.error(f"Lỗi khi khởi tạo Google Client: {e}")
    st.stop()

# --- GIAO DIỆN ---
st.markdown("""
    <style>
    .stApp { background-color: black; color: white; } .stMetric { color: white; }
    .stDataFrame { color: white; } .stPlotlyChart { background-color: transparent; }
    </style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=60)
def fetch_realtime_data():
    try:
        # 1. LỆNH GỌI A: Lấy chỉ số KPI chính xác (5 phút và 30 phút)
        kpi_request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            metrics=[Metric(name="activeUsers")],
            minute_ranges=[
                MinuteRange(start_minutes_ago=29, end_minutes_ago=0),
                MinuteRange(start_minutes_ago=4, end_minutes_ago=0)
            ]
        )
        
        # 2. LỆNH GỌI B: Lấy dữ liệu cho bảng trang
        pages_request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[Dimension(name="unifiedScreenName")],
            metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")],
            minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)]
        )
        
        # 3. LỆNH GỌI C: Lấy dữ liệu cho biểu đồ
        per_min_request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[Dimension(name="minutesAgo")],
            metrics=[Metric(name="activeUsers")],
            minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)]
        )

        # Chạy các request
        kpi_response = client.run_realtime_report(kpi_request)
        pages_response = client.run_realtime_report(pages_request)
        per_min_response = client.run_realtime_report(per_min_request)

        # Xử lý response A: Lấy chỉ số KPI
        active_users_30min = int(kpi_response.rows[0].metric_values[0].value) if len(kpi_response.rows) > 0 else 0
        active_users_5min = int(kpi_response.rows[1].metric_values[0].value) if len(kpi_response.rows) > 1 else 0

        # Xử lý response B: Lấy dữ liệu bảng và tính tổng Views
        pages_data = []
        total_views = 0
        for row in pages_response.rows:
            page_title = row.dimension_values[0].value
            page_users = int(row.metric_values[0].value)
            page_views = int(row.metric_values[1].value)
            total_views += page_views
            marketer = ""
            for symbol, name in marketer_map.items():
                if symbol in page_title:
                    marketer = name
                    break
            pages_data.append({
                "Page Title and Screen Class": page_title, "Marketer": marketer,
                "Active Users": page_users, "Views": page_views
            })
        pages_df = pd.DataFrame(pages_data).sort_values(by="Active Users", ascending=False).head(10)

        # Xử lý response C: Lấy dữ liệu biểu đồ
        per_min_data = {str(i): 0 for i in range(30)}
        for row in per_min_response.rows:
            min_ago = row.dimension_values[0].value
            users = int(row.metric_values[0].value)
            per_min_data[min_ago] = users
        per_min_df = pd.DataFrame([
            {"Time": f"-{int(min_ago)} min", "Active Users": per_min_data[min_ago]} 
            for min_ago in sorted(per_min_data.keys(), key=int)
        ])
        
        now_in_utc = datetime.now(pytz.utc)
        return active_users_5min, active_users_30min, total_views, pages_df, per_min_df, now_in_utc
    except Exception as e:
        return None, None, None, None, None, str(e)


# --- LUỒNG CHÍNH CỦA ỨNG DỤNG ---
if not cookies.ready():
    st.spinner()
    st.stop()

if 'logged_in' not in st.session_state:
    if cookies.get('logged_in_status') == 'true':
        st.session_state['logged_in'] = True
    else:
        st.session_state['logged_in'] = False

if not st.session_state.get('logged_in'):
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Log In"):
        if check_credentials(username, password):
            st.session_state['logged_in'] = True
            cookies['logged_in_status'] = 'true'
            cookies.save()
            st.rerun()
        else:
            st.error("Incorrect username or password")
else:
    st.title("Realtime Pages Dashboard")
    if st.sidebar.button("Log Out"):
        st.session_state['logged_in'] = False
        cookies['logged_in_status'] = 'false'
        cookies.save()
        st.rerun()

    if 'selected_timezone_label' not in st.session_state:
        st.session_state.selected_timezone_label = "Viet Nam (UTC+7)"

    st.session_state.selected_timezone_label = st.sidebar.selectbox(
        "Select Timezone", options=list(TIMEZONE_MAPPINGS.keys()), key="timezone_selector"
    )

    placeholder = st.empty()
    while True:
        active_users_5min, active_users_30min, views, pages_df, per_min_df, utc_fetch_time = fetch_realtime_data()
        
        if active_users_5min is None:
            # Nếu có lỗi, chỉ hiển thị lỗi trong placeholder chính
            with placeholder.container():
                st.error(f"Error fetching data: {utc_fetch_time}")
        else:
            # Nếu không có lỗi, vẽ toàn bộ dashboard
            with placeholder.container():
                selected_tz_str = TIMEZONE_MAPPINGS[st.session_state.selected_timezone_label]
                selected_tz = pytz.timezone(selected_tz_str)
                localized_fetch_time = utc_fetch_time.astimezone(selected_tz)
                last_update_time_str = localized_fetch_time.strftime("%Y-%m-%d %H:%M:%S")

                st.markdown(f"*Data fetched at: {last_update_time_str}*")
                
                ### THAY ĐỔI 1: Tạo một "vùng chứa" trống cho bộ đếm ngược ###
                timer_placeholder = st.empty()

                # Hiển thị các chỉ số, biểu đồ, bảng như cũ
                col1, col2, col3 = st.columns(3)
                col1.metric("ACTIVE USERS IN LAST 5 MINUTES", active_users_5min)
                col2.metric("ACTIVE USERS IN LAST 30 MINUTES", active_users_30min)
                col3.metric("VIEWS IN LAST 30 MINUTES", views)

                if not per_min_df.empty and per_min_df["Active Users"].sum() > 0:
                    fig, ax = plt.subplots(figsize=(12, 4))
                    fig.patch.set_facecolor('black')
                    ax.set_facecolor('black')
                    ax.bar(per_min_df["Time"], per_min_df["Active Users"], color='#4A90E2')
                    ax.tick_params(axis='x', colors='white')
                    ax.tick_params(axis='y', colors='white')
                    ax.spines['bottom'].set_color('white')
                    ax.spines['left'].set_color('white')
                    ax.spines['top'].set_color('black')
                    ax.spines['right'].set_color('black')
                    ax.yaxis.label.set_color('white')
                    ax.xaxis.label.set_color('white')
                    plt.xticks(rotation=90)
                    st.pyplot(fig)

                st.subheader("Page and screen in last 30 minutes")
                if not pages_df.empty:
                    st.table(pages_df.reset_index(drop=True))
                else:
                    st.write("No data available in the last 30 minutes.")
        
        ### THAY ĐỔI 2: Chạy vòng lặp đếm ngược và cập nhật "vùng chứa" đã tạo ###
        # Vòng lặp này bây giờ nằm ngoài placeholder.container()
        for seconds in range(REFRESH_INTERVAL_SECONDS, 0, -1):
            # Cập nhật nội dung của timer_placeholder với màu xanh
            timer_placeholder.markdown(
                f'<p style="color:green;"><b>Next UI refresh in: {seconds} seconds...</b></p>',
                unsafe_allow_html=True
            )
            time.sleep(1)
            
        st.rerun()
