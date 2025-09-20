import streamlit as st
import pandas as pd
import plotly.express as px
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest, RunReportRequest, Dimension, Metric, MinuteRange,
    DateRange, FilterExpression, FilterExpressionList, Filter
)
from google.oauth2 import service_account
import time
from datetime import datetime, timedelta, timezone
from streamlit_cookies_manager import EncryptedCookieManager
import json
import pytz
import numpy as np
import re
import requests # Thêm thư viện requests để gọi REST API

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
    st.error("Lỗi: Không tìm thấy file marketer_mapping.json."); st.stop()
except (json.JSONDecodeError, KeyError):
    st.error("Lỗi: File marketer_mapping.json có cấu trúc không hợp lệ."); st.stop()

# Định nghĩa các múi giờ và danh sách biểu tượng
TIMEZONE_MAPPINGS = {"Viet Nam (UTC+7)": "Asia/Ho_Chi_Minh", "New York (UTC-4)": "America/New_York", "Chicago (UTC-5)": "America/Chicago", "Denver (UTC-6)": "America/Denver", "Los Angeles (UTC-7)": "America/Los_Angeles", "Anchorage (UTC-8)": "America/Anchorage", "Honolulu (UTC-10)": "Pacific/Honolulu"}
SYMBOLS = list(page_title_map.keys())

# --- KẾT NỐI VÀ XÁC THỰC ---
cookies = EncryptedCookieManager(password=st.secrets["cookie"]["encrypt_key"])

def get_user_details(username: str):
    users = st.secrets.get("users", {})
    for _, user_info in users.items():
        if user_info.get("username") == username: return user_info
    return None

def check_credentials(username, password):
    user_details = get_user_details(username)
    if user_details and user_details.get("password") == password: return user_details
    return None

try:
    ga_credentials = service_account.Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=["https://www.googleapis.com/auth/analytics.readonly"])
    ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)
    shopify_creds = st.secrets["shopify_credentials"]
except Exception as e:
    st.error(f"Lỗi khi khởi tạo Client: {e}"); st.stop()

# --- GIAO DIỆN CHUNG ---
st.markdown("""<style>.stApp{background-color:black;color:white;}.stMetric{color:white;}.stDataFrame{color:white;}.stPlotlyChart{background-color:transparent;}.block-container{max-width:960px;}</style>""", unsafe_allow_html=True)

# --- CÁC HÀM TIỆN ÍCH ---

def extract_core_and_symbol(title: str, symbols: list):
    found_symbol = ""
    for s in symbols:
        if s in title:
            found_symbol = s
            break
            
    cleaned_text = title.lower()
    cleaned_text = cleaned_text.split('–')[0].split(' - ')[0]
    for s in symbols:
        cleaned_text = cleaned_text.replace(s, '')
    
    cleaned_text = re.sub(r'[^\w\s]', '', cleaned_text, flags=re.UNICODE)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    return cleaned_text, found_symbol
    
def highlight_purchases(val):
    if isinstance(val, (int, float)) and val > 0:
        return 'background-color: #023020; color: #23d123; font-weight: bold;'
    return ''

# --- CÁC HÀM LẤY DỮ LIỆU ---

@st.cache_data(ttl=60)
def fetch_shopify_realtime_purchases_rest():
    """
    Hàm được khôi phục từ script gốc của bạn, đã được xác nhận là lấy đúng dữ liệu.
    """
    try:
        thirty_minutes_ago = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        url = f"https://{shopify_creds['store_url']}/admipi/{shopify_creds['api_version']}/orders.json"
        headers = {"X-Shopify-Access-Token": shopify_creds['access_token']}
        params = {
            "created_at_min": thirty_minutes_ago,
            "status": "any",
            "fields": "line_items"
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        orders = response.json().get('orders', [])
        
        purchase_data = []
        for order in orders:
            for item in order.get('line_items', []):
                purchase_data.append({
                    'Product Title': item['title'],
                    'Purchases': item['quantity']
                })

        if not purchase_data: return pd.DataFrame(columns=["Product Title", "Purchases"]), 0
        
        purchases_df = pd.DataFrame(purchase_data)
        total_purchases = purchases_df['Purchases'].sum()
        return purchases_df, total_purchases
    except Exception as e:
        return pd.DataFrame(columns=["Product Title", "Purchases"]), 0

@st.cache_data(ttl=60)
def fetch_realtime_data():
    try:
        kpi_request = RunRealtimeReportRequest(property=f"properties/{PROPERTY_ID}", metrics=[Metric(name="activeUsers")], minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0), MinuteRange(start_minutes_ago=4, end_minutes_ago=0)])
        pages_request = RunRealtimeReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="unifiedScreenName")], metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")], minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)])
        per_min_request = RunRealtimeReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="minutesAgo")], metrics=[Metric(name="activeUsers")], minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)])
        
        kpi_response, pages_response, per_min_response = ga_client.run_realtime_report(kpi_request), ga_client.run_realtime_report(pages_request), ga_client.run_realtime_report(per_min_request)

        active_users_30min, active_users_5min = (int(kpi_response.rows[0].metric_values[0].value) if kpi_response.rows else 0), (int(kpi_response.rows[1].metric_values[0].value) if len(kpi_response.rows) > 1 else 0)
        pages_data, total_views = [], 0
        for row in pages_response.rows:
            pages_data.append({"Page Title and Screen Class": row.dimension_values[0].value, "Active Users": int(row.metric_values[0].value)})
            total_views += int(row.metric_values[1].value) if len(row.metric_values) > 1 else 0
        ga_pages_df = pd.DataFrame(pages_data)

        per_min_data = {str(i): 0 for i in range(30)}
        for row in per_min_response.rows: per_min_data[row.dimension_values[0].value] = int(row.metric_values[0].value)
        per_min_df = pd.DataFrame([{"Time": f"-{int(k)} min", "Active Users": v} for k, v in sorted(per_min_data.items(), key=lambda item: int(item[0]))])
        
        # *** THAY ĐỔI: Gọi hàm REST API chính xác ***
        shopify_purchases_df, purchase_count_30min = fetch_shopify_realtime_purchases_rest()

        if not ga_pages_df.empty:
            ga_pages_df[['core_title', 'symbol']] = ga_pages_df['Page Title and Screen Class'].apply(lambda x: pd.Series(extract_core_and_symbol(x, SYMBOLS)))
            
            if not shopify_purchases_df.empty:
                shopify_purchases_df[['core_title', 'symbol']] = shopify_purchases_df['Product Title'].apply(lambda x: pd.Series(extract_core_and_symbol(x, SYMBOLS)))
                shopify_grouped = shopify_purchases_df.groupby(['core_title', 'symbol'])['Purchases'].sum().reset_index()
                merged_df = pd.merge(ga_pages_df, shopify_grouped, on=['core_title', 'symbol'], how='left')
            else:
                merged_df = ga_pages_df.copy(); merged_df['Purchases'] = 0

            merged_df["Purchases"] = merged_df["Purchases"].fillna(0).astype(int)
            merged_df["CR"] = np.divide(merged_df["Purchases"], merged_df["Active Users"], out=np.zeros_like(merged_df["Active Users"], dtype=float), where=(merged_df["Active Users"]!=0)) * 100
            merged_df['Marketer'] = merged_df['Page Title and Screen Class'].apply(get_marketer_from_landing_page)
            
            final_pages_df = merged_df.sort_values(by="Active Users", ascending=False).head(10)
            final_pages_df = final_pages_df[["Page Title and Screen Class", "Marketer", "Active Users", "Purchases", "CR"]]
        else:
            final_pages_df = pd.DataFrame()
        
        now_in_utc = datetime.now(pytz.utc)
        return active_users_5min, active_users_30min, total_views, purchase_count_30min, final_pages_df, per_min_df, now_in_utc, ga_pages_df, shopify_purchases_df
    except Exception as e:
        return None, None, None, None, None, None, str(e), pd.DataFrame(), pd.DataFrame()


def get_marketer_from_landing_page(title: str) -> str:
    # ... (Hàm này giữ nguyên)
    for key, name in landing_page_map.items():
        if key.lower() in title.lower(): return name
    for symbol, name in page_title_map.items():
        if symbol in title: return name
    return ""

def get_date_range(selection: str) -> tuple[datetime.date, datetime.date]:
    # ... (Hàm này giữ nguyên)
    today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
    if selection == "Today": start_date = today - timedelta(days=1); end_date = today
    elif selection == "Yesterday": start_date = end_date = today - timedelta(days=1)
    elif selection == "This Week": start_date = today - timedelta(days=today.weekday()); end_date = today
    elif selection == "Last Week": end_date = today - timedelta(days=today.weekday() + 1); start_date = end_date - timedelta(days=6)
    elif selection == "Last 7 days": start_date = today - timedelta(days=6); end_date = today
    elif selection == "Last 30 days": start_date = today - timedelta(days=29); end_date = today
    else: start_date = end_date = today
    return start_date, end_date


@st.cache_data
def fetch_landing_page_data(start_date: str, end_date: str):
    # ... (Hàm này giữ nguyên)
    try:
        product_page_filter = FilterExpression(filter=Filter(field_name="landingPage", string_filter=Filter.StringFilter(value="/products/", match_type=Filter.StringFilter.MatchType.CONTAINS)))
        limit_rows = 10000
        sessions_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="landingPage")], metrics=[Metric(name="sessions")], date_ranges=[DateRange(start_date=start_date, end_date=end_date)], dimension_filter=product_page_filter, limit=limit_rows)
        purchases_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="landingPage")], metrics=[Metric(name="keyEvents")], date_ranges=[DateRange(start_date=start_date, end_date=end_date)], dimension_filter=FilterExpression(and_group=FilterExpressionList(expressions=[product_page_filter, FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value="purchase")))])), limit=limit_rows)
        sessions_response, purchases_response = ga_client.run_report(sessions_request), ga_client.run_report(purchases_request)
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
    st.session_state['user_info'] = get_user_details(username_from_cookie) if username_from_cookie else None

if not st.session_state['user_info']:
    st.title("Login")
    username, password = st.text_input("Username"), st.text_input("Password", type="password")
    if st.button("Log In"):
        user_details = check_credentials(username, password)
        if user_details:
            st.session_state['user_info'] = user_details; cookies['username'] = user_details['username']; cookies.save(); st.rerun()
        else:
            st.error("Incorrect username or password")
else:
    user_info = st.session_state['user_info']
    st.sidebar.success(f"Welcome, **{user_info['username']}**!")
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Choose a report:", ("Realtime Dashboard", "Landing Page Report"))
    if st.sidebar.button("Log Out"):
        st.session_state['user_info'] = None; cookies['username'] = None; cookies.save(); st.rerun()
    debug_mode = False
    if user_info['role'] == 'admin':
        debug_mode = st.sidebar.checkbox("Enable Debug Mode")
        if debug_mode: st.sidebar.warning("Debug mode is ON. Raw data is shown.")
    if page == "Realtime Dashboard":
        st.title("Realtime Pages Dashboard")
        if 'selected_timezone_label' not in st.session_state: st.session_state.selected_timezone_label = "Viet Nam (UTC+7)"
        st.session_state.selected_timezone_label = st.sidebar.selectbox("Select Timezone", options=list(TIMEZONE_MAPPINGS.keys()), key="timezone_selector")
        
        timer_placeholder = st.empty()
        placeholder = st.empty()
        with placeholder.container():
            active_users_5min, active_users_30min, total_views, purchase_count_30min, pages_df, per_min_df, utc_fetch_time, ga_raw_df, shopify_raw_df = fetch_realtime_data()
            if active_users_5min is None: 
                st.error(f"Error fetching data: {utc_fetch_time}")
            else:
                selected_tz_str = TIMEZONE_MAPPINGS[st.session_state.selected_timezone_label]
                selected_tz = pytz.timezone(selected_tz_str)
                localized_fetch_time = utc_fetch_time.astimezone(selected_tz)
                last_update_time_str = localized_fetch_time.strftime("%Y-%m-%d %H:%M:%S")
                st.markdown(f"*Data fetched at: {last_update_time_str}*")
                
                top_col1, top_col2, top_col3 = st.columns(3)
                top_col1.metric("ACTIVE USERS IN LAST 5 MIN", active_users_5min)
                top_col2.metric("ACTIVE USERS IN LAST 30 MIN", active_users_30min)
                top_col3.metric("VIEWS IN LAST 30 MIN", total_views)
                st.divider()
                bottom_col1, bottom_col2 = st.columns(2)
                with bottom_col1:
                    purchase_html = f"""<div style="background-color: #025402; border: 2px solid #057805; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: #b0b0b0; margin-bottom: 5px; font-family: 'Source Sans Pro', sans-serif;">PURCHASES (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: #23d123; margin: 0; font-family: 'Source Sans Pro', sans-serif;">{purchase_count_30min}</p></div>"""
                    st.markdown(purchase_html, unsafe_allow_html=True)
                with bottom_col2:
                    conversion_rate_30min = (purchase_count_30min / active_users_30min * 100) if active_users_30min > 0 else 0.0
                    conversion_rate_str = f"{conversion_rate_30min:.2f}%"
                    cr_html = f"""<div style="background-color: #013254; border: 2px solid #0564a8; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: #b0b0b0; margin-bottom: 5px; font-family: 'Source Sans Pro', sans-serif;">CONVERSION RATE (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: #23a7d1; margin: 0; font-family: 'Source Sans Pro', sans-serif;">{conversion_rate_str}</p></div>"""
                    st.markdown(cr_html, unsafe_allow_html=True)
                st.divider()
                if not per_min_df.empty and per_min_df["Active Users"].sum() > 0:
                    fig = px.bar(per_min_df, x="Time", y="Active Users", template="plotly_dark", color_discrete_sequence=['#4A90E2'])
                    fig.update_layout(xaxis_title=None, yaxis_title="Active Users", plot_bgcolor='rgba(0, 0, 0, 0)', paper_bgcolor='rgba(0, 0, 0, 0)', yaxis=dict(gridcolor='rgba(255, 255, 255, 0.1)'), xaxis=dict(tickangle=-90))
                    st.plotly_chart(fig, use_container_width=True)
                
                if debug_mode:
                    st.subheader("Debug Mode: Raw Data Inspection")
                    st.write("Dưới đây là dữ liệu gốc từ hai nguồn trước khi được gộp.")
                    st.write("1. Dữ liệu từ Google Analytics (Traffic):")
                    st.code(ga_raw_df.to_dict('records'))
                    st.write("2. Dữ liệu từ Shopify (Purchases):")
                    st.code(shopify_raw_df.to_dict('records'))

                st.subheader("Page and screen in last 30 minutes")
                if not pages_df.empty:
                    st.dataframe(
                        pages_df.style.format({'CR': "{:.2f}%"}).applymap(highlight_purchases, subset=['Purchases', 'CR']),
                        use_container_width=True
                    )
                else: 
                    st.write("No data available in the last 30 minutes.")
        for seconds in range(REFRESH_INTERVAL_SECONDS, 0, -1):
            timer_placeholder.markdown(f'<p style="color:green;"><b>Next realtime data refresh in: {seconds} seconds...</b></p>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
    elif page == "Landing Page Report":
        # ... (Phần này không thay đổi)
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
                        st.subheader("DEBUG MODE: Raw Data from Google Analytics"); st.write("Check the 'Marketer' column. If it shows '' for your pages, the mapping key in your JSON file is incorrect."); st.dataframe(all_data_df)
                    else:
                        data_to_display = pd.DataFrame()
                        if user_info['role'] == 'admin':
                            data_to_display = all_data_df.head(20)
                        else:
                            marketer_id = user_info['marketer_id']
                            employee_df = all_data_df[all_data_df['Marketer'] == marketer_id]
                            data_to_display = employee_df.sort_values(by="Sessions", ascending=False).head(20)
                        if not data_to_display.empty:
                            total_sessions = data_to_display['Sessions'].sum(); total_key_events = data_to_display['Key Events (purchase)'].sum()
                            total_rate = (total_key_events / total_sessions * 100) if total_sessions > 0 else 0
                            total_row = pd.DataFrame([{"Marketer": "", "Landing page": "Total", "Sessions": total_sessions, "Key Events (purchase)": total_key_events, "Session Key Event Rate (purchase)": f"{total_rate:.2f}%"}])
                            final_df = pd.concat([total_row, data_to_display], ignore_index=True)
                            st.dataframe(final_df.reset_index(drop=True))
                        else:
                            st.write("No data found for your user in the selected date range.")
                else:
                    st.write("No product landing pages found with sessions in the selected date range.")
