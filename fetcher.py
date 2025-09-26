import os
import json
import re
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (Dimension, Metric, MinuteRange,
                                                  RunRealtimeReportRequest)
from google.oauth2 import service_account
from supabase import Client, create_client


# ==============================================================================
# CÁC HÀM TIỆN ÍCH (SAO CHÉP TỪ SCRIPT CHÍNH ĐỂ ĐẢM BẢO ĐỒNG BỘ)
# ==============================================================================

def get_marketer_from_page_title(title: str, page_title_map: dict, symbols: list) -> str:
    """Xác định Marketer ID từ tiêu đề trang."""
    for symbol in symbols:
        if symbol in title:
            return page_title_map.get(symbol, "")
    return ""

def extract_core_and_symbol(title: str, symbols: list):
    """Tách biểu tượng và tên cốt lõi từ tiêu đề."""
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

# ==============================================================================
# CÁC HÀM LẤY DỮ LIỆU TỪ API
# ==============================================================================

def fetch_shopify_data(shopify_creds: dict) -> list:
    """Lấy dữ liệu đơn hàng trong 30 phút qua từ Shopify."""
    print("Fetching data from Shopify...")
    thirty_minutes_ago = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    url = f"https://{shopify_creds['store_url']}/admin/api/{shopify_creds['api_version']}/orders.json"
    headers = {"X-Shopify-Access-Token": shopify_creds['access_token']}
    params = {"created_at_min": thirty_minutes_ago, "status": "any", "fields": "id,line_items,total_shipping_price_set,subtotal_price,created_at"}
    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    print(f"Found {len(response.json().get('orders', []))} orders from Shopify.")
    return response.json().get('orders', [])

def fetch_ga_data(ga_client, property_id: str) -> list:
    """Lấy dữ liệu realtime từ Google Analytics bằng MỘT lệnh gọi duy nhất."""
    print("Fetching data from Google Analytics...")
    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="unifiedScreenName"), Dimension(name="minutesAgo")],
        metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")],
        minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)]
    )
    response = ga_client.run_realtime_report(request)
    print(f"Found {len(response.rows)} rows from Google Analytics.")
    return [
        {
            "Page Title and Screen Class": row.dimension_values[0].value,
            "minutesAgo": int(row.dimension_values[1].value),
            "Active Users": int(row.metric_values[0].value),
            "Views": int(row.metric_values[1].value)
        } for row in response.rows
    ]

# ==============================================================================
# HÀM CHÍNH
# ==============================================================================

def main():
    print("Starting data fetch process...")
    try:
        # --- 1. LẤY CREDENTIALS TỪ GITHUB SECRETS ---
        ga_creds_json = os.environ['GOOGLE_CREDENTIALS_JSON']
        shopify_creds_json = os.environ['SHOPIFY_CREDENTIALS_JSON']
        supabase_url = os.environ['SUPABASE_URL']
        supabase_key = os.environ['SUPABASE_SERVICE_ROLE_KEY']
        property_id = os.environ['GA_PROPERTY_ID']

        ga_creds_dict = json.loads(ga_creds_json)
        shopify_creds = json.loads(shopify_creds_json)
        
        # --- 2. KẾT NỐI TỚI CÁC DỊCH VỤ ---
        print("Connecting to services...")
        ga_credentials = service_account.Credentials.from_service_account_info(ga_creds_dict)
        ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)
        supabase: Client = create_client(supabase_url, supabase_key)

        with open('marketer_mapping.json', 'r', encoding='utf-8') as f:
            mapping = json.load(f)
            page_title_map = mapping.get('page_title_mapping', {})
            SYMBOLS = sorted(list(page_title_map.keys()), key=len, reverse=True)
            
        # --- 3. LẤY DỮ LIỆU THÔ ---
        ga_data = fetch_ga_data(ga_client, property_id)
        shopify_orders = fetch_shopify_data(shopify_creds)

        # --- 4. TẠO ĐỐI TƯỢNG JSON CUỐI CÙNG ĐỂ LƯU TRỮ ---
        # Dashboard sẽ tự xử lý logic gộp và tính toán từ dữ liệu thô này.
        # Điều này giúp fetcher đơn giản và ít bị lỗi hơn.
        final_data_blob = {
            "ga_data": ga_data,
            "shopify_orders": shopify_orders,
            "last_updated_utc": datetime.now(timezone.utc).isoformat()
        }

        # --- 5. LƯU DỮ LIỆU LÊN "BẢNG TIN" SUPABASE ---
        print("Updating data to Supabase...")
        supabase.table("realtime_data").update({"data": final_data_blob}).eq("id", 1).execute()
        print(f"Successfully fetched data and updated Supabase at {final_data_blob['last_updated_utc']}")

    except Exception as e:
        print(f"An error occurred: {e}")
        # Ghi nhận lỗi để có thể xem lại trong logs của GitHub Actions
        raise e

if __name__ == "__main__":
    main()