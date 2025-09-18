import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunRealtimeReportRequest, Dimension, Metric, MinuteRange
from google.oauth2 import service_account
import time
from datetime import datetime
from streamlit_cookies_manager import EncryptedCookieManager
import json ### MỚI: Thêm thư viện json

# --- CẤU HÌNH ---
PROPERTY_ID = "501726461"
REFRESH_INTERVAL_SECONDS = 60

### MỚI: Tải các quy tắc mapping từ file JSON khi ứng dụng khởi động ###
# encoding='utf-8' rất quan trọng để đọc được các icon
try:
    with open('marketer_mapping.json', 'r', encoding='utf-8') as f:
        marketer_map = json.load(f)
except FileNotFoundError:
    st.error("Lỗi: Không tìm thấy file marketer_mapping.json. Vui lòng tải file này lên GitHub.")
    st.stop()


# --- XÁC THỰC VÀ COOKIES ---
cookies = EncryptedCookieManager(
    password=st.secrets["cookie"]["encrypt_key"],
)

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
    .stApp { background-color: black; color: white; }
    .stMetric { color: white; }
    .stDataFrame { color: white; }
    .stPlotlyChart { background-color: transparent; }
    </style>
""", unsafe_allow_html=True)

# --- HÀM LẤY DỮ LIỆU ---
def fetch_realtime_data():
    try:
        # 1. Lệnh gọi API riêng cho 5 phút
        five_min_request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            metrics=[Metric(name="activeUsers")],
            minute_ranges=[MinuteRange(start_minutes_ago=4, end_minutes_ago=0)]
        )
        five_min_response = client.run_realtime_report(five_min_request)
        active_users_5min_api = int(five_min_response.totals[0].metric_values[0].value) if five_min_response.totals else 0

        # ... (các request khác giữ nguyên) ...
        totals_request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")],
            minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)]
        )
        totals_response = client.run_realtime_report(totals_request)
        active_users_30min = int(totals_response.totals[0].metric_values[0].value) if totals_response.totals else 0
        views = int(totals_response.totals[0].metric_values[1].value) if totals_response.totals else 0

        pages_request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[Dimension(name="unifiedScreenName")],
            metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")],
            minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)]
        )
        pages_response = client.run_realtime_report(pages_request)
        pages_data = []
        pages_views_sum = 0
        for row in pages_response.rows:
            page_title = row.dimension_values[0].value
            page_users = int(row.metric_values[0].value)
            page_views = int(row.metric_values[1].value)

            ### THAY ĐỔI: Sử dụng vòng lặp để tìm marketer từ file JSON ###
            marketer = "" # Giá trị mặc định
            # Lặp qua từng quy tắc trong file JSON đã tải
            for symbol, name in marketer_map.items():
                if symbol in page_title:
                    marketer = name
                    break # Dừng lại ngay khi tìm thấy marketer đầu tiên

            pages_data.append({
                "Page Title and Screen Class": page_title,
                "Marketer": marketer,
                "Active Users": page_users,
                "Views": page_views
            })
            pages_views_sum += page_views
            
        pages_df = pd.DataFrame(pages_data).sort_values(by="Active Users", ascending=False).head(10)

        # ... (phần còn lại của hàm giữ nguyên) ...
        if active_users_30min == 0 and not pages_df.empty and pages_df['Active Users'].sum() > 0:
            active_users_30min = pages_df['Active Users'].sum()
        if views == 0 and pages_views_sum > 0:
            views = pages_views_sum

        per_min_request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[Dimension(name="minutesAgo")],
            metrics=[Metric(name="activeUsers")],
            minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)]
        )
        per_min_response = client.run_realtime_report(per_min_request)
        per_min_data = {str(i): 0 for i in range(30)}
        for row in per_min_response.rows:
            min_ago = row.dimension_values[0].value
            users = int(row.metric_values[0].value)
            per_min_data[min_ago] = users
        per_min_df = pd.DataFrame([
            {"Time": f"-{int(min_ago)} min", "Active Users": per_min_data[min_ago]} 
            for min_ago in sorted(per_min_data.keys(), key=int)
        ])
        
        active_users_5min_final = active_users_5min_api
        if active_users_5min_api == 0 and not per_min_df.empty:
            sum_from_chart = int(per_min_df.head(5)['Active Users'].sum())
            if sum_from_chart > 0:
                active_users_5min_final = sum_from_chart

        return active_users_5min_final, active_users_30min, views, pages_df, per_min_df
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return 0, 0, 0, pd.DataFrame(), pd.DataFrame()

# --- LUỒNG CHÍNH CỦA ỨNG DỤNG ---
# (Toàn bộ phần này giữ nguyên, không thay đổi)
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

    placeholder = st.empty()
    while True:
        active_users_5min, active_users_30min, views, pages_df, per_min_df = fetch_realtime_data()
        last_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with placeholder.container():
            st.markdown(f"*Last updated: {last_update_time}*")
            
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

        timer_placeholder = st.empty()
        for seconds in range(REFRESH_INTERVAL_SECONDS, 0, -1):
            timer_placeholder.markdown(f"**Next refresh in: {seconds} seconds...**")
            time.sleep(1)
            
        placeholder.empty()
        timer_placeholder.empty()
