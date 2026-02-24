import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect

def get_table_columns(engine, table_name):
    """Get existing columns in a table."""
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return set()
    columns = inspector.get_columns(table_name)
    return {col['name'] for col in columns}

def add_missing_columns(engine, table_name, df_columns):
    """Add missing columns to the table."""
    existing_cols = get_table_columns(engine, table_name)
    missing_cols = set(df_columns) - existing_cols
    
    if not missing_cols:
        print(f"✅ No missing columns in '{table_name}'")
        return
    
    print(f"📊 Adding {len(missing_cols)} missing columns to '{table_name}':")
    
    with engine.begin() as conn:
        for col in missing_cols:
            try:
                # Use TEXT type for flexibility (handles all data types)
                sql = f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT'
                conn.execute(text(sql))
                print(f"  ✓ Added column: {col}")
            except Exception as e:
                print(f"  ✗ Failed to add {col}: {e}")
    
    print(f"✅ Schema sync completed for '{table_name}'\n")

def sync_all_schemas(neon_connection_string):
    """Sync schemas for all Meta Ads tables."""
    engine = create_engine(neon_connection_string)
    
    # Define all possible column names from Meta Ads API
    # This is a comprehensive list based on common Meta Ads fields
    common_columns = [
        # Basic campaign info
        'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 
        'ad_id', 'ad_name', 'objective', 'account_id',
        
        # Core metrics
        'impressions', 'reach', 'frequency', 'clicks', 'ctr', 'cpc', 
        'cpm', 'spend', 'date_start', 'date_stop',
        
        # Result tracking
        'result',
        
        # Engagement actions
        'link_click', 'page_engagement', 'post_engagement', 
        'post_interaction_gross', 'post', 'post_reaction',
        'like', 'comment', 'video_view',
        
        # Post actions
        'onsite_conversion_post_save', 'onsite_conversion_post_net_save',
        'onsite_conversion_post_unlike', 'onsite_conversion_post_net_like',
        'onsite_conversion_post_unsave',
        
        # Messaging
        'onsite_conversion_messaging_block',
        'onsite_conversion_total_messaging_connection',
        'onsite_conversion_messaging_first_reply',
        'onsite_conversion_messaging_conversation_started_7d',
        'onsite_conversion_messaging_conversation_replied_7d',
        'onsite_conversion_messaging_user_depth_2_message_send',
        
        # Landing pages
        'landing_page_view', 'omni_landing_page_view',
        
        # App actions
        'app_site_visit', 'app_store_visit',
        
        # Checkout & Cart
        'omni_initiated_checkout', 'onsite_web_initiate_checkout',
        'offsite_conversion_fb_pixel_initiate_checkout', 'initiate_checkout',
        'omni_add_to_cart', 'add_to_cart', 'onsite_web_app_add_to_cart',
        'onsite_web_add_to_cart', 'offsite_conversion_fb_pixel_add_to_cart',
        
        # Checkout & Cart Values
        'onsite_web_app_add_to_cart_value', 'add_to_cart_value',
        'offsite_conversion_fb_pixel_add_to_cart_value', 'omni_add_to_cart_value',
        'onsite_web_add_to_cart_value', 'onsite_web_initiate_checkout_value',
        'omni_initiated_checkout_value', 'initiate_checkout_value',
        'offsite_conversion_fb_pixel_initiate_checkout_value',
        
        # Purchases
        'purchase_roas', 'web_in_store_purchase', 'omni_purchase', 'purchase',
        'onsite_web_view_content', 'onsite_web_app_purchase', 'view_content',
        'web_app_in_store_purchase', 'onsite_web_app_view_content',
        'offsite_content_view_add_meta_leads', 'onsite_web_purchase',
        'omni_view_content', 'offsite_conversion_fb_pixel_view_content',
        'offsite_conversion_fb_pixel_purchase',
        
        # Purchase Values
        'onsite_web_app_purchase_value', 'offsite_conversion_fb_pixel_view_content_value',
        'web_app_in_store_purchase_value', 'omni_view_content_value',
        'onsite_web_purchase_value', 'purchase_value', 'view_content_value',
        'onsite_web_view_content_value', 'onsite_web_app_view_content_value',
        'offsite_conversion_fb_pixel_purchase_value', 'omni_purchase_value',
        'web_in_store_purchase_value',
        
        # Custom conversions
        'offsite_conversion_fb_pixel_custom',
        
        # Cost per actions
        'cost_per_video_view', 'cost_per_link_click', 'cost_per_like',
        'cost_per_post_interaction_gross', 'cost_per_post_engagement',
        'cost_per_page_engagement', 'cost_per_landing_page_view',
        'cost_per_omni_landing_page_view', 'cost_per_app_site_visit',
        'cost_per_initiate_checkout', 'cost_per_onsite_web_initiate_checkout',
        'cost_per_omni_initiated_checkout', 'cost_per_omni_add_to_cart',
        'cost_per_add_to_cart', 'cost_per_offsite_conversion_fb_pixel_custom',
        'cost_per_web_in_store_purchase', 'cost_per_omni_purchase',
        'cost_per_omni_view_content', 'cost_per_onsite_web_purchase',
        'cost_per_view_content', 'cost_per_offsite_content_view_add_meta_leads',
        'cost_per_purchase',
        
        # Messaging costs
        'cost_per_onsite_conversion_total_messaging_connection',
        'cost_per_onsite_conversion_messaging_first_reply',
        'cost_per_onsite_conversion_messaging_conversation_started_7d',
        'cost_per_onsite_conversion_messaging_conversation_replied_7d',
        'cost_per_onsite_conversion_messaging_user_depth_2_message_send',
        
        # Demographic breakdowns (for age/gender table)
        'age', 'gender',
        
        # Regional breakdown (for region table)
        'region',
    ]
    
    # Tables to sync
    tables = {
        'meta_ads_summary': common_columns,
        'meta_ads_age_gender': common_columns,
        'meta_ads_region': common_columns,
    }
    
    print("=" * 60)
    print("META ADS SCHEMA SYNC")
    print("=" * 60)
    print()
    
    for table_name, columns in tables.items():
        if get_table_columns(engine, table_name):
            add_missing_columns(engine, table_name, columns)
        else:
            print(f"⚠️  Table '{table_name}' does not exist yet. It will be created on first run.\n")
    
    print("=" * 60)
    print("SCHEMA SYNC COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    NEON_CONNECTION_STRING = os.environ.get("NEON_CONNECTION_STRING")
    
    if not NEON_CONNECTION_STRING:
        print("❌ Error: NEON_CONNECTION_STRING environment variable not set")
        exit(1)
    
    sync_all_schemas(NEON_CONNECTION_STRING)
