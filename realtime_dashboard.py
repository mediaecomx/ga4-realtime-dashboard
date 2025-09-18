import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunRealtimeReportRequest, Dimension, Metric, MinuteRange
from google.oauth2 import service_account
import time
from datetime import datetime ### MỚI ### - Thêm thư viện datetime để lấy thời gian hiện tại

# Đường dẫn đến file JSON key
KEY_PATH = "autocomplete-address-419409-7980bc3b2193.json"

# Property ID từ GA4
PROPERTY_ID = "501726461"

### MỚI ### - Định nghĩa khoảng thời gian làm mới (tính bằng giây)
REFRESH_INTERVAL_SECONDS = 60

# Khởi tạo client
credentials = service_account.Credentials.from_service_account_file(
    KEY_PATH, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
)
client = BetaAnalyticsDataClient(credentials=credentials)

# Giao diện tối
st.markdown("""
    <style>
    .stApp { background-color: black; color: white; }
    .stMetric { color: white; }
    .stDataFrame { color: white; }
    .stPlotlyChart { background-color: transparent; }
    </style>
""", unsafe_allow_html=True)

def fetch_realtime_data():
    try:
        # 1. Request cho số liệu tổng
        totals_request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")],
            minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)]
        )
        totals_response = client.run_realtime_report(totals_request)
        active_users = int(totals_response.totals[0].metric_values[0].value) if totals_response.totals else 0
        views = int(totals_response.totals[0].metric_values[1].value) if totals_response.totals else 0

        # 2. Request cho bảng các trang
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
            pages_data.append({"Page Title and Screen Class": page_title, "Active Users": page_users, "Views": page_views})
            pages_views_sum += page_views
        pages_df = pd.DataFrame(pages_data).sort_values(by="Active Users", ascending=False).head(10)

        # Fallback
        if active_users == 0 and not pages_df.empty and pages_df['Active Users'].sum() > 0:
            active_users = pages_df['Active Users'].sum()
        if views == 0 and pages_views_sum > 0:
            views = pages_views_sum

        # 3. Request cho biểu đồ mỗi phút
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

        return active_users, views, pages_df, per_min_df
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return 0, 0, pd.DataFrame(), pd.DataFrame()

# Streamlit dashboard
st.title("Realtime Pages Dashboard")

placeholder = st.empty()

while True:
    active_users, views, pages_df, per_min_df = fetch_realtime_data()
    
    ### MỚI ### - Lấy thời gian cập nhật hiện tại và định dạng nó
    last_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with placeholder.container():
        
        ### MỚI ### - Hiển thị thời gian cập nhật lần cuối
        st.markdown(f"*Last updated: {last_update_time}*")
        
        col1, col2 = st.columns(2)
        col1.metric("ACTIVE USERS IN LAST 30 MINUTES", active_users)
        col2.metric("VIEWS IN LAST 30 MINUTES", views)

        # Biểu đồ bar
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

        # Bảng các trang
        st.subheader("Page and screen in last 30 minutes")
        if not pages_df.empty:
            st.table(pages_df.reset_index(drop=True))
        else:
            st.write("No data available in the last 30 minutes.")
            
    ### MỚI ### - Thêm một placeholder riêng cho đồng hồ đếm ngược
    timer_placeholder = st.empty()
    
    ### MỚI ### - Vòng lặp đếm ngược thay cho time.sleep(60)
    for seconds in range(REFRESH_INTERVAL_SECONDS, 0, -1):
        timer_placeholder.markdown(f"**Next refresh in: {seconds} seconds...**")
        time.sleep(1)
        
    placeholder.empty()
    timer_placeholder.empty()