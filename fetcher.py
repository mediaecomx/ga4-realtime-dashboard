import os
import json
import re
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (Dimension, Metric, MinuteRange,
                                                  RunRealtimeReportRequest)
from google.oauth2 import service_account
from supabase import Client, create_client

# ==============================================================================
# CÁC HÀM TIỆN ÍCH
# ==============================================================================

def get_marketer_from_page_title(title: str, page_title_map: dict, symbols: list) -> str:
    for symbol in symbols:
        if symbol in title:
            return page_title_map.get(symbol, "")
    return ""

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

# ==============================================================================
# CÁC HÀM LẤY DỮ LIỆU
# ==============================================================================

def fetch_shopify_data(shopify_creds: dict):
    thirty_minutes_ago = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    url = f"https://{shopify_creds['store_url']}/admin/api/{shopify_creds['api_version']}/orders.json"
    headers = {"X-Shopify-Access-Token": shopify_creds['access_token']}
    params = {"created_at_min": thirty_minutes_ago, "status": "any", "fields": "id,line_items,total_shipping_price_set,subtotal_price,created_at"}
    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    return response.json().get('orders', [])

def fetch_ga_data(ga_client, property_id: str):
    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="unifiedScreenName"), Dimension(name="minutesAgo")],
        metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")],
        minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)]
    )
    response = ga_client.run_realtime_report(request)
    return [{"Page Title and Screen Class": row.dimension_values[0].value, "minutesAgo": int(row.dimension_values[1].value), "Active Users": int(row.metric_values[0].value), "Views": int(row.metric_values[1].value)} for row in response.rows]

def run_fetch_cycle(ga_client, shopify_creds, supabase, page_title_map, SYMBOLS, property_id):
    """Thực hiện một chu kỳ lấy và cập nhật dữ liệu."""
    print(f"Executing fetch cycle at {datetime.now(timezone.utc)}")
    ga_data = fetch_ga_data(ga_client, property_id)
    shopify_orders = fetch_shopify_data(shopify_creds)
    
    final_data_blob = {
        "ga_data": ga_data,
        "shopify_orders": shopify_orders,
        "last_updated_utc": datetime.now(timezone.utc).isoformat()
    }

    supabase.table("realtime_data").update({"data": final_data_blob}).eq("id", 1).execute()
    print(f"Successfully updated Supabase at {final_data_blob['last_updated_utc']}")

# ==============================================================================
# HÀM CHÍNH
# ==============================================================================

def main():
    print("Starting data fetch process...")
    try:
        ga_creds_json = os.environ['GOOGLE_CREDENTIALS_JSON']
        shopify_creds_json = os.environ['SHOPIFY_CREDENTIALS_JSON']
        supabase_url = os.environ['SUPABASE_URL']
        supabase_key = os.environ['SUPABASE_SERVICE_ROLE_KEY']
        property_id = os.environ['GA_PROPERTY_ID']

        ga_creds_dict = json.loads(ga_creds_json)
        shopify_creds = json.loads(shopify_creds_json)
        
        ga_credentials = service_account.Credentials.from_service_account_info(ga_creds_dict)
        ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)
        supabase: Client = create_client(supabase_url, supabase_key)

        with open('marketer_mapping.json', 'r', encoding='utf-8') as f:
            mapping = json.load(f)
            page_title_map = mapping.get('page_title_mapping', {})
            SYMBOLS = sorted(list(page_title_map.keys()), key=len, reverse=True)
        
        # GitHub Actions chạy mỗi phút, chúng ta sẽ lặp 2 lần với 45 giây ngủ
        # để tạo ra chu kỳ ~90 giây (2 * 45s = 90s).
        # Thời gian chạy thực tế của script khoảng 10-15s.
        for i in range(2):
            run_fetch_cycle(ga_client, shopify_creds, supabase, page_title_map, SYMBOLS, property_id)
            if i == 0: # Chỉ ngủ sau lần chạy đầu tiên
                print("Sleeping for 45 seconds before the next cycle...")
                time.sleep(45)

    except Exception as e:
        print(f"An error occurred: {e}")
        raise e

if __name__ == "__main__":
    main()
