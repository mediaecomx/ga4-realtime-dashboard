import streamlit as st
import pandas as pd
import plotly.express as px
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest, RunReportRequest, Dimension, Metric, MinuteRange,
    DateRange
)
from google.oauth2 import service_account
import time
from datetime import datetime, timedelta, timezone
from streamlit_cookies_manager import EncryptedCookieManager
import json
import pytz
import numpy as np
import re
import requests
from supabase import create_client, Client

# --- CẤU HÌNH CHUNG ---
PROPERTY_ID = "501726461"

# --- BẮT ĐẦU CẤU HÌNH CHO THẺ NHIỆT ---
# Bạn có thể thay đổi các giá trị "TARGET" này để điều chỉnh độ nhạy của màu sắc.
# Đây là ngưỡng mà tại đó thẻ sẽ đạt màu sắc nóng nhất.
TARGET_USERS_5MIN = 100
TARGET_USERS_30MIN = 500
TARGET_VIEWS_30MIN = 1000

# Định nghĩa dải màu từ LẠNH sang NÓNG (định dạng (R, G, B))
COLOR_COLD = (40, 40, 60)   # Xanh xám đậm (màu lạnh)
COLOR_HOT = (255, 190, 0)  # Vàng cam rực (màu nóng)
# --- KẾT THÚC CẤU HÌNH CHO THẺ NHIỆT ---

# --- TẢI CÁC QUY TẮC MAPPING TỪ FILE JSON ---
try:
    with open('marketer_mapping.json', 'r', encoding='utf-8') as f:
        full_mapping = json.load(f)
        page_title_map = full_mapping.get('page_title_mapping', {})
except FileNotFoundError:
    st.error("Lỗi: Không tìm thấy file marketer_mapping.json."); st.stop()
except (json.JSONDecodeError, KeyError):
    st.error("Lỗi: File marketer_mapping.json có cấu trúc không hợp lệ."); st.stop()

# Định nghĩa các múi giờ và danh sách biểu tượng
TIMEZONE_MAPPINGS = {"Viet Nam (UTC+7)": "Asia/Ho_Chi_Minh", "New York (UTC-4)": "America/New_York", "Chicago (UTC-5)": "America/Chicago", "Denver (UTC-6)": "America/Denver", "Los Angeles (UTC-7)": "America/Los_Angeles", "Anchorage (UTC-8)": "America/Anchorage", "Honolulu (UTC-10)": "Pacific/Honolulu"}
SYMBOLS = sorted(list(page_title_map.keys()), key=len, reverse=True)

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
    google_creds_dict = dict(st.secrets["google_credentials"])
    google_creds_dict["private_key"] = google_creds_dict["private_key"].replace("\\n", "\n")
    ga_credentials = service_account.Credentials.from_service_account_info(
        google_creds_dict,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)
    shopify_creds = st.secrets["shopify_credentials"]
    cloudinary_cloud_name = st.secrets["cloudinary"]["cloud_name"]
    cloudinary_upload_preset = st.secrets["cloudinary"]["upload_preset"]
    default_avatar_url = st.secrets["default_images"]["avatar_url"]
    supabase_url = st.secrets["supabase"]["url"]
    supabase_key = st.secrets["supabase"]["service_role_key"]
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    st.error(f"Lỗi khi khởi tạo Client hoặc đọc secrets: {e}"); st.stop()

# --- GIAO DIỆN CHUNG ---
st.markdown("""<style>.stApp{background-color:black;color:white;}.stMetric{color:white;}.stDataFrame{color:white;}.stPlotlyChart{background-color:transparent;}.block-container{max-width:960px;}</style>""", unsafe_allow_html=True)

# --- CÁC HÀM TIỆN ÍCH ---
def get_heatmap_color_and_text(value, target, cold_color, hot_color):
    """
    Tính toán màu nền và màu chữ dựa trên giá trị và mục tiêu.
    """
    if target == 0:
        bg_rgb = cold_color
    else:
        ratio = min(1.0, value / target)
        r = int(cold_color[0] + ratio * (hot_color[0] - cold_color[0]))
        g = int(cold_color[1] + ratio * (hot_color[1] - cold_color[1]))
        b = int(cold_color[2] + ratio * (hot_color[2] - cold_color[2]))
        bg_rgb = (r, g, b)
    brightness = (bg_rgb[0] * 299 + bg_rgb[1] * 587 + bg_rgb[2] * 114) / 1000
    text_color = "#FFFFFF" if brightness < 140 else "#000000"
    return f"rgb({bg_rgb[0]},{bg_rgb[1]},{bg_rgb[2]})", text_color

def extract_core_and_symbol(title: str, symbols: list):
    found_symbol = ""
    title_str = str(title)
    for s in symbols:
        if s in title_str:
            found_symbol = s
            break
    cleaned_text = title_str.lower().split('–')[0].split(' - ')[0]
    for s in symbols: cleaned_text = cleaned_text.replace(s, '')
    cleaned_text = re.sub(r'[^\w\s]', '', cleaned_text, flags=re.UNICODE).strip()
    return cleaned_text, found_symbol

def highlight_metrics(val):
    if isinstance(val, (int, float)) and val > 0:
        return 'background-color: #023020; color: #23d123; font-weight: bold;'
    return ''

def get_marketer_from_page_title(title: str) -> str:
    for symbol in SYMBOLS:
        if symbol in title:
            return page_title_map.get(symbol, "")
    return ""

# --- CÁC HÀM LẤY DỮ LIỆU ---
@st.cache_data(ttl=60)
def fetch_shopify_realtime_purchases_rest():
    try:
        thirty_minutes_ago = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
        url = f"https://{shopify_creds['store_url']}/admin/api/{shopify_creds['api_version']}/orders.json"
        headers = {"X-Shopify-Access-Token": shopify_creds['access_token']}
        params = {"created_at_min": thirty_minutes_ago, "status": "any", "fields": "line_items,total_shipping_price_set,subtotal_price"}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        orders = response.json().get('orders', [])
        purchase_data = []
        for order in orders:
            subtotal = float(order.get('subtotal_price', 0.0))
            shipping_fee = float(order.get('total_shipping_price_set', {}).get('shop_money', {}).get('amount', 0.0))
            for item in order.get('line_items', []):
                item_price = float(item['price'])
                item_quantity = item['quantity']
                item_total_value = item_price * item_quantity
                shipping_allocation = (shipping_fee * (item_total_value / subtotal)) if subtotal > 0 else 0
                purchase_data.append({'Product Title': item['title'], 'Purchases': item_quantity, 'Revenue': item_total_value + shipping_allocation})
        if not purchase_data: return pd.DataFrame(columns=["Product Title", "Purchases", "Revenue"]), 0
        purchases_df = pd.DataFrame(purchase_data)
        return purchases_df, purchases_df['Purchases'].sum()
    except Exception: return pd.DataFrame(columns=["Product Title", "Purchases", "Revenue"]), 0

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
        shopify_purchases_df, purchase_count_30min = fetch_shopify_realtime_purchases_rest()
        ga_pages_df_processed = ga_pages_df.copy()
        shopify_purchases_df_processed = shopify_purchases_df.copy()
        if not ga_pages_df_processed.empty:
            ga_pages_df_processed[['core_title', 'symbol']] = ga_pages_df_processed['Page Title and Screen Class'].apply(lambda x: pd.Series(extract_core_and_symbol(x, SYMBOLS)))
            if not shopify_purchases_df_processed.empty:
                shopify_purchases_df_processed[['core_title', 'symbol']] = shopify_purchases_df_processed['Product Title'].apply(lambda x: pd.Series(extract_core_and_symbol(x, SYMBOLS)))
                shopify_grouped = shopify_purchases_df_processed.groupby(['core_title', 'symbol'])[['Purchases', 'Revenue']].sum().reset_index()
                merged_df = pd.merge(ga_pages_df_processed, shopify_grouped, on=['core_title', 'symbol'], how='left')
            else:
                merged_df = ga_pages_df_processed.copy(); merged_df['Purchases'] = 0; merged_df['Revenue'] = 0.0
            merged_df["Purchases"] = merged_df["Purchases"].fillna(0).astype(int)
            merged_df["Revenue"] = merged_df["Revenue"].fillna(0).astype(float)
            merged_df["CR"] = np.divide(merged_df["Purchases"], merged_df["Active Users"], out=np.zeros_like(merged_df["Active Users"], dtype=float), where=(merged_df["Active Users"]!=0)) * 100
            merged_df['Marketer'] = merged_df['Page Title and Screen Class'].apply(get_marketer_from_page_title)
            final_pages_df = merged_df.sort_values(by="Active Users", ascending=False)[["Page Title and Screen Class", "Marketer", "Active Users", "Purchases", "Revenue", "CR"]]
        else:
            final_pages_df = pd.DataFrame()
        now_in_utc = datetime.now(pytz.utc)
        return active_users_5min, active_users_30min, total_views, purchase_count_30min, final_pages_df, per_min_df, now_in_utc
    except Exception as e:
        return None, None, None, None, None, None, str(e)

@st.cache_data
def fetch_historical_page_report(start_date: str, end_date: str, segment: str):
    try:
        dimensions = [Dimension(name="pageTitle")]
        if segment == 'By Day': dimensions.append(Dimension(name="date"))
        elif segment == 'By Week': dimensions.append(Dimension(name="week"))
        sessions_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=dimensions, metrics=[Metric(name="sessions"), Metric(name="totalUsers")], date_ranges=[DateRange(start_date=start_date, end_date=end_date)], limit=50000)
        sessions_response = ga_client.run_report(sessions_request)
        ga_sessions_data = []
        for row in sessions_response.rows:
            item_data = { "Page Title": row.dimension_values[0].value, "Sessions": int(row.metric_values[0].value), "Users": int(row.metric_values[1].value) }
            if segment == 'By Day': item_data['Date'] = datetime.strptime(row.dimension_values[1].value, '%Y%m%d').strftime('%Y-%m-%d')
            elif segment == 'By Week': item_data['Week'] = row.dimension_values[1].value
            ga_sessions_data.append(item_data)
        ga_sessions_df = pd.DataFrame(ga_sessions_data)
        if ga_sessions_df.empty: return pd.DataFrame()
        return ga_sessions_df
    except Exception as e:
        st.error(f"Error fetching Historical GA data: {e}"); return pd.DataFrame()

# --- LUỒNG CHÍNH CỦA ỨNG DỤNG ---
if not cookies.ready(): st.spinner(); st.stop()
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = get_user_details(cookies.get('username'))

if 'realtime_history' not in st.session_state:
    st.session_state.realtime_history = []

if not st.session_state['user_info']:
    st.title("Login")
    username, password = st.text_input("Username"), st.text_input("Password", type="password")
    if st.button("Log In"):
        user_details = check_credentials(username, password)
        if user_details:
            st.session_state['user_info'] = user_details
            try:
                profile_data = supabase.table("profiles").select("avatar_url").eq("username", user_details['username']).single().execute()
                if profile_data.data: st.session_state['user_info']['avatar_url'] = profile_data.data.get('avatar_url')
            except: pass
            cookies['username'] = user_details['username']; cookies.save(); st.rerun()
        else: st.error("Incorrect username or password")
else:
    effective_user_info = dict(st.session_state['user_info'])
    avatar_url = effective_user_info.get("avatar_url") or default_avatar_url
    st.sidebar.markdown(f"""<div style="display: flex; flex-direction: column; align-items: center; text-align: center; margin-bottom: 20px;"><img src="{avatar_url}" style="width: 100px; height: 100px; border-radius: 50%; object-fit: cover; border: 2px solid #3c4043;"><p style="margin-top: 10px; margin-bottom: 0; font-size: 1em; color: #d0d0d0;">Welcome,</p><p style="margin: 0; font-size: 1.25em; font-weight: bold; color: #1ED760;">{effective_user_info['username']}</p></div>""", unsafe_allow_html=True)
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Choose a report:", ("Realtime Dashboard", "Landing Page Report", "Profile"))
    if st.sidebar.button("Log Out"):
        for key in list(st.session_state.keys()): del st.session_state[key]
        cookies.clear(); cookies.save(); st.rerun()
    impersonating = False
    if st.session_state['user_info']['role'] == 'admin':
        st.sidebar.divider()
        all_users = st.secrets.get("users", {})
        employee_details = {v['username']: v for k, v in all_users.items() if v['role'] == 'employee'}
        selected_user_name = st.sidebar.selectbox("Impersonate User", options=["None (View as Admin)"] + list(employee_details.keys()))
        if selected_user_name != "None (View as Admin)":
            impersonating = True
            effective_user_info = employee_details[selected_user_name]
            st.sidebar.info(f"Viewing as **{selected_user_name}**")
    debug_mode = st.sidebar.checkbox("Enable Debug Mode") if st.session_state['user_info']['role'] == 'admin' and not impersonating else False
    if debug_mode: st.sidebar.warning("Debug mode is ON.")

    if page == "Profile":
        st.title("👤 Your Profile"); st.header("Update Your Avatar")
        col1, col2 = st.columns([1, 2])
        with col1:
            st.write("Current Avatar:")
            st.image(st.session_state['user_info'].get('avatar_url') or default_avatar_url, width=150)
        with col2:
            st.write("Upload a new image (JPG, PNG):")
            uploaded_file = st.file_uploader("Choose a file", type=["jpg", "jpeg", "png"])
            if uploaded_file:
                with st.spinner("Uploading to Cloudinary..."):
                    try:
                        response = requests.post(f"https://api.cloudinary.com/v1_1/{cloudinary_cloud_name}/image/upload", files={"file": uploaded_file.getvalue()}, data={"upload_preset": cloudinary_upload_preset}, timeout=30)
                        response.raise_for_status()
                        new_link = response.json().get("secure_url")
                        if new_link:
                            supabase.table("profiles").upsert({"username": st.session_state['user_info']['username'], "avatar_url": new_link}).execute()
                            st.session_state['user_info']['avatar_url'] = new_link
                            st.success("Avatar updated successfully!"); time.sleep(1); st.rerun()
                        else: st.error(f"Upload succeeded but no URL returned. Response: {response.json()}")
                    except Exception as e: st.error(f"Failed to upload image. Error: {e}")

    elif page == "Realtime Dashboard":
        st.title("Realtime Pages Dashboard")
        selected_tz_name = st.sidebar.selectbox("Select Timezone", options=list(TIMEZONE_MAPPINGS.keys()), key="timezone_selector")
        selected_tz = pytz.timezone(TIMEZONE_MAPPINGS[selected_tz_name])
        try: refresh_interval = int(cookies.get('refresh_interval', 60))
        except (ValueError, TypeError): refresh_interval = 60
        if st.session_state['user_info']['role'] == 'admin' and not impersonating:
            new_interval = st.sidebar.number_input("Set Refresh Interval (seconds)", min_value=30, value=refresh_interval, step=10)
            if new_interval != refresh_interval: cookies['refresh_interval'] = str(new_interval); cookies.save(); st.rerun()
            refresh_interval = new_interval
        
        timer_placeholder, placeholder = st.empty(), st.empty()
        
        with placeholder.container():
            fetch_result = fetch_realtime_data()
            if fetch_result[0] is None:
                st.error(f"Error fetching data: {fetch_result[6]}")
            else:
                (active_users_5min, active_users_30min, total_views, purchase_count_30min, pages_df_full, per_min_df, utc_fetch_time) = fetch_result
                localized_fetch_time = utc_fetch_time.astimezone(selected_tz)
                st.markdown(f"*Data fetched at: {localized_fetch_time.strftime('%Y-%m-%d %H:%M:%S')}*")

                top_col1, top_col2, top_col3 = st.columns(3)
                with top_col1:
                    bg_color, text_color = get_heatmap_color_and_text(active_users_5min, TARGET_USERS_5MIN, COLOR_COLD, COLOR_HOT)
                    html_5min = f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">ACTIVE USERS (5 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{active_users_5min}</p></div>"""
                    st.markdown(html_5min, unsafe_allow_html=True)
                with top_col2:
                    bg_color, text_color = get_heatmap_color_and_text(active_users_30min, TARGET_USERS_30MIN, COLOR_COLD, COLOR_HOT)
                    html_30min = f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">ACTIVE USERS (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{active_users_30min}</p></div>"""
                    st.markdown(html_30min, unsafe_allow_html=True)
                with top_col3:
                    bg_color, text_color = get_heatmap_color_and_text(total_views, TARGET_VIEWS_30MIN, COLOR_COLD, COLOR_HOT)
                    html_views = f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">VIEWS (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{total_views}</p></div>"""
                    st.markdown(html_views, unsafe_allow_html=True)
                
                st.divider()
                bottom_col1, bottom_col2 = st.columns(2)
                with bottom_col1: st.markdown(f"""<div style="background-color: #025402; border: 2px solid #057805; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: #b0b0b0; margin-bottom: 5px;">PURCHASES (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: #23d123; margin: 0;">{purchase_count_30min}</p></div>""", unsafe_allow_html=True)
                with bottom_col2: st.markdown(f"""<div style="background-color: #013254; border: 2px solid #0564a8; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: #b0b0b0; margin-bottom: 5px;">CONVERSION RATE (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: #23a7d1; margin: 0;">{(purchase_count_30min / active_users_30min * 100) if active_users_30min > 0 else 0:.2f}%</p></div>""", unsafe_allow_html=True)
                
                if not pages_df_full.empty:
                    marketer_summary = pages_df_full.groupby('Marketer')['Active Users'].sum()
                    current_snapshot = marketer_summary.to_dict()
                else:
                    current_snapshot = {}
                st.session_state.realtime_history.append({'timestamp': localized_fetch_time, **current_snapshot})
                MAX_HISTORY_POINTS = 60
                if len(st.session_state.realtime_history) > MAX_HISTORY_POINTS:
                    st.session_state.realtime_history = st.session_state.realtime_history[-MAX_HISTORY_POINTS:]
                history_df = pd.DataFrame(st.session_state.realtime_history).set_index('timestamp')
                history_df_melted = history_df.reset_index().melt(id_vars='timestamp', var_name='Marketer', value_name='Active Users').dropna(subset=['Active Users'])
                st.divider()
                st.subheader("Active Users Trend by Marketer (Live)")
                if not history_df_melted.empty:
                    fig_trend = px.line(history_df_melted, x='timestamp', y='Active Users', color='Marketer', template='plotly_dark', markers=True, labels={'timestamp': 'Time'})
                    fig_trend.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', yaxis=dict(gridcolor='rgba(255,255,255,0.1)'), legend_title_text='')
                    st.plotly_chart(fig_trend, use_container_width=True)
                else:
                    st.write("Collecting data for trend chart... Please wait for the next refresh.")
                if not per_min_df.empty and per_min_df["Active Users"].sum() > 0:
                    st.subheader("Total Active Users per Minute (All Marketers)")
                    fig = px.bar(per_min_df, x="Time", y="Active Users", template="plotly_dark", color_discrete_sequence=['#4A90E2'])
                    fig.update_layout(xaxis_title=None, yaxis_title="Active Users", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', yaxis=dict(gridcolor='rgba(255,255,255,0.1)'), xaxis=dict(tickangle=-90))
                    st.plotly_chart(fig, use_container_width=True)
                st.divider()
                st.subheader("Page and screen in last 30 minutes")
                pages_to_display = pages_df_full
                if not (effective_user_info['role'] == 'admin' or effective_user_info.get('can_view_all_realtime_data', False)):
                    marketer_id = effective_user_info['marketer_id']
                    pages_to_display = pages_df_full[pages_df_full['Marketer'] == marketer_id]
                if not pages_to_display.empty:
                    st.dataframe(pages_to_display.style.format({'CR': "{:.2f}%", 'Revenue': "${:,.2f}"}).applymap(highlight_metrics, subset=['Purchases', 'Revenue', 'CR']), use_container_width=True)
                else:
                    st.write("No data available for your user.")
        for seconds in range(refresh_interval, 0, -1):
            timer_placeholder.markdown(f'<p style="color:green;"><b>Next realtime data refresh in: {seconds} seconds...</b></p>', unsafe_allow_html=True); time.sleep(1)
        st.rerun()


    elif page == "Landing Page Report":
        #... Full code for Landing Page Report
        st.title("Page Performance Report (by Title)")
        col1, col2 = st.columns(2)
        with col1:
            date_options = ["Today", "Yesterday", "This Week", "Last Week", "Last 7 days", "Last 30 days", "Custom Range..."]
            selected_option = st.selectbox("Select Date Range", options=date_options, index=5)
        with col2:
            segment_option = st.selectbox("Segment by:", ("Summary", "By Day", "By Week"))
        min_purchases = 1 if segment_option != 'Summary' else 0
        if segment_option != 'Summary':
            min_purchases = st.number_input("Minimum Purchases to Display", min_value=0, value=1, step=1)
        start_date, end_date = None, None
        if selected_option == "Custom Range...":
            today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
            selected_range = st.date_input("Select your custom date range", value=(today - timedelta(days=6), today), min_value=today - timedelta(days=365), max_value=today, format="YYYY/MM/DD")
            if len(selected_range) == 2: start_date, end_date = selected_range
        else: start_date, end_date = get_date_range(selected_option)
        if start_date and end_date:
            st.markdown(f"**Displaying data for:** `{start_date.strftime('%b %d, %Y')}{' - ' + end_date.strftime('%b %d, %Y') if start_date != end_date else ''}`")
            with st.spinner("Fetching data from GA & Shopify..."):
                all_data_df = fetch_historical_page_report(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), segment_option)
                if not all_data_df.empty:
                    if segment_option != 'Summary':
                        all_data_df = all_data_df[all_data_df['Purchases'] >= min_purchases]
                    data_to_display = all_data_df if effective_user_info['role'] == 'admin' else all_data_df[all_data_df['Marketer'] == effective_user_info['marketer_id']]
                    if not data_to_display.empty:
                        if segment_option == "Summary":
                            total_sessions, total_users, total_purchases, total_revenue = data_to_display['Sessions'].sum(), data_to_display['Users'].sum(), data_to_display['Purchases'].sum(), data_to_display['Revenue'].sum()
                            total_session_cr = (total_purchases / total_sessions * 100) if total_sessions > 0 else 0
                            total_user_cr = (total_purchases / total_users * 100) if total_users > 0 else 0
                            total_row = pd.DataFrame([{"Page Title": "Total", "Marketer": "", "Sessions": total_sessions, "Users": total_users, "Purchases": total_purchases, "Revenue": total_revenue, "Session CR": total_session_cr, "User CR": total_user_cr}])
                            data_to_display = pd.concat([total_row, data_to_display], ignore_index=True)
                        st.dataframe(data_to_display.style.format({'Revenue': "${:,.2f}", 'Session CR': "{:.2f}%", 'User CR': "{:.2f}%"}).applymap(highlight_metrics, subset=['Purchases', 'Revenue', 'Session CR', 'User CR']), use_container_width=True)
                    else: st.write("No data found for your user/filters in the selected date range.")
                else: st.write("No page data found with sessions in the selected date range.")
