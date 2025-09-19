import streamlit as st
import pandas as pd
import plotly.express as px  # THAY THẾ MATPLOTLIB
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest,
    RunReportRequest,
    Dimension,
    Metric,
    MinuteRange,
    DateRange,
    FilterExpression,
    FilterExpressionList,
    Filter
)
from google.oauth2 import service_account
import time
from datetime import datetime, timedelta
from streamlit_cookies_manager import EncryptedCookieManager
import json
import pytz
import numpy as np

# --- CẤU HÌNH CHUNG ---
PROPERTY_ID = "501726461"
REFRESH_INTERVAL_SECONDS = 60

# --- TẢI CÁC QUY TẮC MAPPING TỪ FILE JSON ---
try:
    with open('marketer_mapping.json', 'r', encoding='utf-8') as f:
        full_mapping = json.load(f)
        page_title_map = full_mapping.get('page_title_mapping', {})
        landing_page_map = full_mapping.get('landing_page_mapping', {})
except FileNotFoundError:
    st.error("Lỗi: Không tìm thấy file marketer_mapping.json.")
    st.stop()
except (json.JSONDecodeError, KeyError):
    st.error("Lỗi: File marketer_mapping.json có cấu trúc không hợp lệ. Vui lòng kiểm tra lại.")
    st.stop()

# Định nghĩa các múi giờ
TIMEZONE_MAPPINGS = {
    "Viet Nam (UTC+7)": "Asia/Ho_Chi_Minh", "New York (UTC-4)": "America/New_York",
    "Chicago (UTC-5)": "America/Chicago", "Denver (UTC-6)": "America/Denver",
    "Los Angeles (UTC-7)": "America/Los_Angeles", "Anchorage (UTC-8)": "America/Anchorage",
    "Honolulu (UTC-10)": "Pacific/Honolulu"
}

# --- KẾT NỐI VÀ XÁC THỰC ---
cookies = EncryptedCookieManager(password=st.secrets["cookie"]["encrypt_key"])

def get_user_details(username: str):
    users = st.secrets.get("users", {})
    for user_key, user_info in users.items():
        if user_info.get("username") == username:
            return user_info
    return None

def check_credentials(username, password):
    user_details = get_user_details(username)
    if user_details and user_details.get("password") == password:
        return user_details
    return None

try:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["google_credentials"],
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    client = BetaAnalyticsDataClient(credentials=credentials)
except Exception as e:
    st.error(f"Lỗi khi khởi tạo Google Client: {e}")
    st.stop()

# --- GIAO DIỆN CHUNG ---
st.markdown("""
    <style>
    .stApp { background-color: black; color: white; }
    .stMetric { color: white; }
    .stDataFrame { color: white; }
    .stPlotlyChart { background-color: transparent; }
    .block-container { max-width: 960px; }
    </style>
""", unsafe_allow_html=True)

# --- CÁC HÀM LẤY DỮ LIỆU ---

@st.cache_data(ttl=60)
def fetch_realtime_data():
    try:
        kpi_request = RunRealtimeReportRequest(property=f"properties/{PROPERTY_ID}", metrics=[Metric(name="activeUsers")], minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0), MinuteRange(start_minutes_ago=4, end_minutes_ago=0)])
        pages_request = RunRealtimeReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="unifiedScreenName")], metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")], minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)])
        per_min_request = RunRealtimeReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="minutesAgo")], metrics=[Metric(name="activeUsers")], minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)])
        kpi_response, pages_response, per_min_response = client.run_realtime_report(kpi_request), client.run_realtime_report(pages_request), client.run_realtime_report(per_min_request)
        active_users_30min, active_users_5min = (int(kpi_response.rows[0].metric_values[0].value) if len(kpi_response.rows) > 0 else 0), (int(kpi_response.rows[1].metric_values[0].value) if len(kpi_response.rows) > 1 else 0)
        pages_data, total_views = [], 0
        for row in pages_response.rows:
            page_title, page_users, page_views = row.dimension_values[0].value, int(row.metric_values[0].value), int(row.metric_values[1].value)
            total_views += page_views
            marketer = ""
            for symbol, name in page_title_map.items():
                if symbol in page_title: marketer = name; break
            pages_data.append({"Page Title and Screen Class": page_title, "Marketer": marketer, "Active Users": page_users, "Views": page_views})
        pages_df = pd.DataFrame(pages_data).sort_values(by="Active Users", ascending=False).head(10)
        per_min_data = {str(i): 0 for i in range(30)}
        for row in per_min_response.rows:
            min_ago, users = row.dimension_values[0].value, int(row.metric_values[0].value)
            per_min_data[min_ago] = users
        per_min_df = pd.DataFrame([{"Time": f"-{int(min_ago)} min", "Active Users": per_min_data[min_ago]} for min_ago in sorted(per_min_data.keys(), key=int)])
        now_in_utc = datetime.now(pytz.utc)
        return active_users_5min, active_users_30min, total_views, pages_df, per_min_df, now_in_utc
    except Exception as e:
        return None, None, None, None, None, str(e)


def get_marketer_from_landing_page(landing_page_url: str) -> str:
    landing_page_url_lower = landing_page_url.lower()
    sorted_mapping_items = sorted(landing_page_map.items(), key=lambda item: len(item[0]), reverse=True)
    for key_string, marketer_name in sorted_mapping_items:
        if key_string.lower() in landing_page_url_lower:
            return marketer_name
    return ""

def get_date_range(selection: str) -> tuple[datetime.date, datetime.date]:
    today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
    if selection == "Today": 
        start_date = today - timedelta(days=1)
        end_date = today
    elif selection == "Yesterday": 
        start_date = end_date = today - timedelta(days=1)
    elif selection == "This Week": 
        start_date = today - timedelta(days=today.weekday())
        end_date = today
    elif selection == "Last Week": 
        end_date = today - timedelta(days=today.weekday() + 1)
        start_date = end_date - timedelta(days=6)
    elif selection == "Last 7 days": 
        start_date = today - timedelta(days=6)
        end_date = today
    elif selection == "Last 30 days": 
        start_date = today - timedelta(days=29)
        end_date = today
    else: 
        start_date = end_date = today
    return start_date, end_date


@st.cache_data
def fetch_landing_page_data(start_date: str, end_date: str):
    try:
        product_page_filter = FilterExpression(filter=Filter(field_name="landingPage", string_filter=Filter.StringFilter(value="/products/", match_type=Filter.StringFilter.MatchType.CONTAINS)))
        limit_rows = 10000
        sessions_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="landingPage")], metrics=[Metric(name="sessions")], date_ranges=[DateRange(start_date=start_date, end_date=end_date)], dimension_filter=product_page_filter, limit=limit_rows)
        purchases_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="landingPage")], metrics=[Metric(name="keyEvents")], date_ranges=[DateRange(start_date=start_date, end_date=end_date)], dimension_filter=FilterExpression(and_group=FilterExpressionList(expressions=[product_page_filter, FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value="purchase")))])), limit=limit_rows)
        sessions_response, purchases_response = client.run_report(sessions_request), client.run_report(purchases_request)
        sessions_data = [{"Landing page": row.dimension_values[0].value, "Sessions": int(row.metric_values[0].value)} for row in sessions_response.rows]
        sessions_df = pd.DataFrame(sessions_data)
        purchases_data = [{"Landing page": row.dimension_values[0].value, "Key Events (purchase)": int(row.metric_values[0].value)} for row in purchases_response.rows]
        purchases_df = pd.DataFrame(purchases_data)
        
        if not sessions_df.empty:
            if not purchases_df.empty: merged_df = pd.merge(sessions_df, purchases_df, on="Landing page", how="left")
            else: merged_df = sessions_df.copy(); merged_df["Key Events (purchase)"] = 0
            
            merged_df['Marketer'] = merged_df['Landing page'].apply(get_marketer_from_landing_page)
            merged_df["Key Events (purchase)"] = merged_df["Key Events (purchase)"].fillna(0).astype(int)
            merged_df['Session Key Event Rate (purchase)'] = np.divide(merged_df['Key Events (purchase)'], merged_df['Sessions'], out=np.zeros_like(merged_df['Sessions'], dtype=float), where=(merged_df['Sessions']!=0)) * 100
            merged_df['Session Key Event Rate (purchase)'] = merged_df['Session Key Event Rate (purchase)'].apply(lambda x: f"{x:.2f}%")
            
            column_order = ["Marketer", "Landing page", "Sessions", "Key Events (purchase)", "Session Key Event Rate (purchase)"]
            all_data_df = merged_df.sort_values(by="Sessions", ascending=False)[column_order]
            return all_data_df
        else: return pd.DataFrame()
    except Exception as e: st.error(f"Error fetching Landing Page data: {e}"); return pd.DataFrame()


# --- LUỒNG CHÍNH CỦA ỨNG DỤNG ---
if not cookies.ready(): st.spinner(); st.stop()

if 'user_info' not in st.session_state:
    username_from_cookie = cookies.get('username')
    if username_from_cookie:
        st.session_state['user_info'] = get_user_details(username_from_cookie)
    else:
        st.session_state['user_info'] = None

if not st.session_state['user_info']:
    st.title("Login")
    username, password = st.text_input("Username"), st.text_input("Password", type="password")
    if st.button("Log In"):
        user_details = check_credentials(username, password)
        if user_details:
            st.session_state['user_info'] = user_details
            cookies['username'] = user_details['username']
            cookies.save()
            st.rerun()
        else:
            st.error("Incorrect username or password")
else:
    user_info = st.session_state['user_info']
    
    st.sidebar.success(f"Welcome, **{user_info['username']}**!")
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Choose a report:", ("Realtime Dashboard", "Landing Page Report"))
    
    if st.sidebar.button("Log Out"):
        st.session_state['user_info'] = None
        cookies['username'] = None
        cookies.save()
        st.rerun()

    if 'selected_timezone_label' not in st.session_state:
        st.session_state.selected_timezone_label = "Viet Nam (UTC+7)"
    st.session_state.selected_timezone_label = st.sidebar.selectbox("Select Timezone", options=list(TIMEZONE_MAPPINGS.keys()), key="timezone_selector")
    
    debug_mode = False
    if user_info['role'] == 'admin':
        debug_mode = st.sidebar.checkbox("Enable Debug Mode")
        if debug_mode:
            st.sidebar.warning("Debug mode is ON. Raw data is shown.")

    if page == "Realtime Dashboard":
        st.title("Realtime Pages Dashboard")
        placeholder = st.empty()
        with placeholder.container():
            active_users_5min, active_users_30min, views, pages_df, per_min_df, utc_fetch_time = fetch_realtime_data()
            if active_users_5min is None: st.error(f"Error fetching data: {utc_fetch_time}")
            else:
                selected_tz_str = TIMEZONE_MAPPINGS[st.session_state.selected_timezone_label]
                selected_tz = pytz.timezone(selected_tz_str)
                localized_fetch_time = utc_fetch_time.astimezone(selected_tz)
                last_update_time_str = localized_fetch_time.strftime("%Y-%m-%d %H:%M:%S")
                st.markdown(f"*Data fetched at: {last_update_time_str}*")
                timer_placeholder = st.empty()
                col1, col2, col3 = st.columns(3)
                col1.metric("ACTIVE USERS IN LAST 5 MINUTES", active_users_5min); col2.metric("ACTIVE USERS IN LAST 30 MINUTES", active_users_30min); col3.metric("VIEWS IN LAST 30 MINUTES", views)
                
                if not per_min_df.empty and per_min_df["Active Users"].sum() > 0:
                    # *** THAY ĐỔI QUAN TRỌNG: Vẽ biểu đồ bằng Plotly ***
                    fig = px.bar(
                        per_min_df,
                        x="Time",
                        y="Active Users",
                        template="plotly_dark",
                        color_discrete_sequence=['#4A90E2']
                    )
                    fig.update_layout(
                        xaxis_title=None,
                        yaxis_title="Active Users",
                        plot_bgcolor='rgba(0, 0, 0, 0)',
                        paper_bgcolor='rgba(0, 0, 0, 0)',
                        yaxis=dict(gridcolor='rgba(255, 255, 255, 0.1)'),
                        xaxis=dict(tickangle=-90)
                    )
                    st.plotly_chart(fig, use_container_width=True)

                st.subheader("Page and screen in last 30 minutes")
                if not pages_df.empty: st.table(pages_df.reset_index(drop=True))
                else: st.write("No data available in the last 30 minutes.")
        for seconds in range(REFRESH_INTERVAL_SECONDS, 0, -1):
            timer_placeholder.markdown(f'<p style="color:green;"><b>Next realtime data refresh in: {seconds} seconds...</b></p>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()

    elif page == "Landing Page Report":
        st.title("Landing Page Report (Purchase Key Event)")
        date_options = ["Today", "Yesterday", "This Week", "Last Week", "Last 7 days", "Last 30 days", "Custom Range..."]
        selected_option = st.selectbox("Select Date Range", options=date_options, index=0)

        start_date, end_date = None, None
        if selected_option == "Custom Range...":
            today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
            default_start = today - timedelta(days=6)
            selected_range = st.date_input(label="Select your custom date range", value=(default_start, today), min_value=today - timedelta(days=365), max_value=today, format="YYYY/MM/DD")
            if len(selected_range) == 2: start_date, end_date = selected_range
        else:
            start_date, end_date = get_date_range(selected_option)

        if start_date and end_date:
            display_date = f"{start_date.strftime('%b %d, %Y')}" if start_date == end_date else f"{start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}"
            st.markdown(f"**Displaying data for:** `{display_date}`")
            start_date_str, end_date_str = start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            with st.spinner(f"Fetching data..."):
                all_data_df = fetch_landing_page_data(start_date_str, end_date_str)
                
                if not all_data_df.empty:
                    if debug_mode:
                        st.subheader("DEBUG MODE: Raw Data from Google Analytics")
                        st.write("Check the 'Marketer' column. If it shows '' for your pages, the mapping key in your JSON file is incorrect.")
                        st.dataframe(all_data_df)
                    else:
                        data_to_display = pd.DataFrame()
                        if user_info['role'] == 'admin':
                            data_to_display = all_data_df.head(20)
                        else:
                            marketer_id = user_info['marketer_id']
                            employee_df = all_data_df[all_data_df['Marketer'] == marketer_id]
                            data_to_display = employee_df.sort_values(by="Sessions", ascending=False).head(20)

                        if not data_to_display.empty:
                            total_sessions = data_to_display['Sessions'].sum()
                            total_key_events = data_to_display['Key Events (purchase)'].sum()
                            total_rate = (total_key_events / total_sessions * 100) if total_sessions > 0 else 0
                            
                            total_row = pd.DataFrame([{"Marketer": "", "Landing page": "Total", "Sessions": total_sessions, "Key Events (purchase)": total_key_events, "Session Key Event Rate (purchase)": f"{total_rate:.2f}%"}])
                            final_df = pd.concat([total_row, data_to_display], ignore_index=True)
                            st.dataframe(final_df.reset_index(drop=True))
                        else:
                            st.write("No data found for your user in the selected date range.")
                else:
                    st.write("No product landing pages found with sessions in the selected date range.")
