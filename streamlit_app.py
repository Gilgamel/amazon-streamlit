import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io
import os
import sys
from dotenv import load_dotenv

# Add Amazon project to path
amazon_path = "C:/Users/vuser/Documents/Projects/Amazon/src"
if amazon_path not in sys.path:
    sys.path.insert(0, amazon_path)

# Load environment variables from Amazon project
env_path = "C:/Users/vuser/Documents/Projects/Amazon/.env"
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)

# ==================== Region Config ====================
REGION_CONFIG = {
    "US": {
        "marketplace": "Amazon.com",
    },
    "CA": {
        "marketplace": "Amazon.ca",
    }
}


# ==================== Auth & Google Sheets ====================
def get_google_creds():
    """Get Google API credentials using Service Account"""
    from google.oauth2 import service_account
    import json

    # Check for local JSON file first
    service_account_path = os.path.join(
        os.path.dirname(__file__),
        "service_account.json"
    )

    if not os.path.exists(service_account_path):
        service_account_path = os.path.join(
            "C:/Users/vuser/Documents/Projects/Amazon",
            "service_account.json"
        )

    if os.path.exists(service_account_path):
        try:
            creds = service_account.Credentials.from_service_account_file(
                service_account_path,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive.readonly'
                ]
            )
            print(f"[DEBUG] Loaded credentials from file: {service_account_path}")
            return creds
        except Exception as e:
            print(f"[ERROR] Failed to load service account file: {str(e)}")
            return None

    # No local file - try Streamlit Cloud secrets
    try:
        if hasattr(st, 'secrets') and st.secrets and 'gcp_service_account' in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                json.loads(st.secrets['gcp_service_account']),
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive.readonly'
                ]
            )
            print("[DEBUG] Loaded credentials from Streamlit secrets")
            return creds
    except Exception as e:
        print(f"[ERROR] Failed to load credentials from secrets: {str(e)}")
        return None

    print("[ERROR] No service account file found")
    return None


def load_gsheet_data(sheet_name, region="US"):
    """Load Google Sheet and return SKU to cost dict. Returns None on failure, {} if empty."""
    import gspread

    try:
        creds = get_google_creds()
        if not creds:
            return None

        client = gspread.authorize(creds)

        # US and CA use the same sheet names
        spreadsheet = client.open(sheet_name)
        sheet = spreadsheet.sheet1
        rows = sheet.get_all_values()

        if not rows:
            return {}

        cost_mapping = {}
        for row in rows[1:]:
            sku = row[0].strip()
            cost_str = row[10].strip() if len(row) > 10 else ''
            if not sku:
                continue
            try:
                cost = float(cost_str) if cost_str else 0.0
            except ValueError:
                cost = 0.0
            cost_mapping[sku] = cost

        return cost_mapping
    except Exception as e:
        import traceback
        print(f"[ERROR] Failed to load {sheet_name}: {str(e)}")
        print(traceback.format_exc())
        return None


def add_master_sku_from_gsheet(df):
    """Add master_sku from Google Sheet"""
    import gspread

    try:
        creds = get_google_creds()
        if not creds:
            df['master_sku'] = df['sku']
            return df
        client = gspread.authorize(creds)
        spreadsheet = client.open("SKU Manual Mapping")
        sheet = spreadsheet.sheet1

        headers = sheet.row_values(1)
        header_clean = [h.strip().lower() for h in headers]

        required_columns = ['channel_sku', 'sku_backup']
        missing = [col for col in required_columns if col not in header_clean]

        if missing:
            st.warning(f"Missing columns in SKU mapping: {missing}")
            df['master_sku'] = df['sku']
            return df

        records = sheet.get_all_records()
        sku_mapping = {}

        for row in records:
            channel_sku = str(row.get('channel_sku', '')).strip()
            sku_backup = str(row.get('sku_backup', '')).strip()
            if channel_sku:
                sku_mapping[channel_sku] = sku_backup

        df['master_sku'] = df['sku'].map(sku_mapping)
        df['master_sku'] = df['master_sku'].fillna(df['sku'])
        return df

    except Exception as e:
        st.warning(f"SKU mapping failed: {str(e)}")
        df['master_sku'] = df['sku']
        return df


# ==================== Data Processing ====================
def fill_missing_qty(merged_df, raw_source_df):
    """Fill missing QTY values"""
    try:
        mask = merged_df['QTY'].isna()
        if not mask.any():
            return merged_df

        source_data = raw_source_df[
            (raw_source_df['amount-type'] == 'ItemWithheldTax') &
            (raw_source_df['transaction-type'] == 'Order') &
            (raw_source_df['sku'].notna())
        ]

        qty_lookup = source_data.groupby(
            ['order-id', 'shipment-id', 'sku']
        )['quantity-purchased'].sum().reset_index()
        qty_lookup.rename(columns={'quantity-purchased': '补充QTY'}, inplace=True)

        filled_df = pd.merge(
            merged_df,
            qty_lookup,
            on=['order-id', 'shipment-id', 'sku'],
            how='left'
        )

        filled_df['QTY'] = filled_df['QTY'].fillna(filled_df['补充QTY']).fillna(0)
        filled_df.drop(columns=['补充QTY'], inplace=True)

        return filled_df
    except Exception as e:
        st.warning(f"QTY fill failed: {str(e)}")
        return merged_df


def merge_order_qty(order_df, qty_df, raw_source_df=None):
    """Merge Order and QTY data with master_sku"""
    try:
        merge_keys = ['order-id', 'shipment-id', 'sku']

        for df, name in [(order_df, 'Order'), (qty_df, 'QTY')]:
            missing = [col for col in merge_keys if col not in df.columns]
            if missing:
                raise ValueError(f"{name} table missing columns: {', '.join(missing)}")

        merged_df = pd.merge(
            order_df,
            qty_df[merge_keys + ['quantity-purchased']],
            on=merge_keys,
            how='left'
        )

        if 'quantity-purchased' in merged_df.columns:
            merged_df.rename(columns={'quantity-purchased': 'QTY'}, inplace=True)

        if raw_source_df is not None:
            merged_df = fill_missing_qty(merged_df, raw_source_df)

        merged_df = add_master_sku_from_gsheet(merged_df)

        columns = [col for col in merged_df.columns if col != 'master_sku'] + ['master_sku']
        return merged_df[columns]

    except Exception as e:
        st.error(f"Merge failed: {str(e)}")
        return None


def generate_summary(raw_df, start_date, end_date, region="US"):
    """Generate summary pivot tables by month"""
    try:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        required_cols = ['transaction-type', 'amount-type', 'amount', 'posted-date']
        missing_cols = [col for col in required_cols if col not in raw_df.columns]
        if missing_cols:
            st.warning(f"Missing columns: {', '.join(missing_cols)}")
            return None

        raw_df = raw_df.copy()
        date_format = '%Y-%m-%d' if region == "US" else '%d.%m.%Y'
        raw_df['posted-date'] = pd.to_datetime(raw_df['posted-date'], format=date_format, errors='coerce')
        raw_df = raw_df.dropna(subset=['posted-date'])

        mask = (raw_df['posted-date'] >= start_date) & (raw_df['posted-date'] <= end_date)
        df = raw_df[mask].copy()

        df['month'] = df['posted-date'].dt.to_period('M')
        months = df['month'].unique()

        pivot_tables = []
        for month in months:
            month_df = df[df['month'] == month]
            pivot = month_df.pivot_table(
                index=['amount-type'],
                columns=['transaction-type'],
                values='amount',
                aggfunc='sum',
                fill_value=0,
                margins=True,
                margins_name='Grand Total'
            )
            pivot_tables.append((month, pivot.round(2).reset_index()))

        return pivot_tables

    except Exception as e:
        st.error(f"Summary generation failed: {str(e)}")
        return None


def split_data_by_month(df, start_date, end_date):
    """Split data by month"""
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    monthly_data = {}
    current_date = start_date
    while current_date <= end_date:
        month_start = datetime(current_date.year, current_date.month, 1)
        month_end = (month_start + pd.DateOffset(months=1)) - pd.DateOffset(days=1)

        effective_start = max(current_date, month_start)
        effective_end = min(end_date, month_end)

        mask = (df['posted-date'] >= effective_start) & (df['posted-date'] <= effective_end)
        month_df = df[mask].copy()

        month_key = effective_start.strftime("%Y%m")
        monthly_data[month_key] = month_df

        current_date = effective_end + pd.DateOffset(days=1)

    return monthly_data


def process_qty_data(input_data, start_date, end_date, region="US"):
    """Process QTY data"""
    try:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        if isinstance(input_data, str):
            df = pd.read_csv(input_data, delimiter='\t', encoding='utf-8')
            df = df.iloc[1:].reset_index(drop=True)
        else:
            df = input_data.copy()

        date_format = '%Y-%m-%d' if region == "US" else '%d.%m.%Y'
        df['posted-date'] = pd.to_datetime(df['posted-date'], format=date_format, errors='coerce')
        df = df.dropna(subset=['posted-date'])

        mask = (df['posted-date'] >= start_date) & (df['posted-date'] <= end_date)
        df = df[mask]

        marketplace = REGION_CONFIG[region]["marketplace"]

        type_map = {
            "settlement-id": 'Int64',
            "total-amount": 'float',
            "amount": 'float',
            "order-item-code": 'Int64',
            "quantity-purchased": 'Int64'
        }
        df = df.astype({k: v for k, v in type_map.items() if k in df.columns})

        df = df[
            (df['transaction-type'] == 'Order') &
            (df['marketplace-name'] == marketplace)
        ].drop(columns=[
            "settlement-id", "settlement-start-date", "settlement-end-date",
            "deposit-date", "total-amount", "currency", "transaction-type",
            "merchant-order-id", "adjustment-id", "marketplace-name",
            "fulfillment-id", "posted-date", "posted-date-time",
            "order-item-code", "merchant-order-item-id",
            "merchant-adjustment-item-id", "promotion-id"
        ])

        df['des-type'] = df['amount-description'] + ":" + df['amount-type']
        df = df[df['des-type'] == "Principal:ItemPrice"]
        return df.groupby(
            ["order-id", "shipment-id", "sku"],
            as_index=False
        )["quantity-purchased"].sum().sort_values("shipment-id"), start_date, end_date

    except Exception as e:
        st.error(f"QTY processing failed: {str(e)}")
        return None, None, None


def process_order_data(raw_df, region="US"):
    """Process Order data"""
    try:
        df = raw_df.copy()
        marketplace = REGION_CONFIG[region]["marketplace"]

        df = df[
            (df['transaction-type'] == 'Order') &
            (df['amount-type'].isin(['ItemPrice', 'ItemWithheldTax', 'Promotion'])) &
            (df['marketplace-name'] == marketplace)
        ]

        cols_to_drop = [
            'settlement-id', 'settlement-start-date', 'settlement-end-date',
            'deposit-date', 'total-amount', 'currency', 'transaction-type',
            'merchant-order-id', 'adjustment-id', 'marketplace-name',
            'fulfillment-id', 'posted-date', 'posted-date-time',
            'order-item-code', 'merchant-order-item-id',
            'merchant-adjustment-item-id', 'quantity-purchased', 'promotion-id'
        ]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

        df['des-type'] = df['amount-description'] + ":" + df['amount-type']
        pivot_df = df.pivot_table(
            index=['order-id', 'shipment-id', 'sku'],
            columns='des-type',
            values='amount',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        required_columns = [
            "Principal:ItemPrice", "Principal:Promotion",
            "Tax:ItemPrice", "MarketplaceFacilitatorTax-Principal:ItemWithheldTax",
            "MarketplaceFacilitatorVAT-Principal:ItemWithheldTax",
            "LowValueGoodsTax-Principal:ItemWithheldTax",
            "Shipping:ItemPrice", "Shipping:Promotion",
            "GiftWrap:ItemPrice", "GiftWrap:Promotion",
            "GiftWrapTax:ItemPrice", "MarketplaceFacilitatorTax-Other:ItemWithheldTax"
        ]

        existing_columns = pivot_df.columns.tolist()
        for col in required_columns:
            if col not in existing_columns:
                pivot_df[col] = 0

        pivot_df['Product Amount'] = pivot_df['Principal:ItemPrice'] + pivot_df['Principal:Promotion']
        pivot_df = pivot_df.drop(['Principal:ItemPrice', 'Principal:Promotion'], axis=1, errors='ignore')

        product_tax_cols = [
            'Tax:ItemPrice',
            'MarketplaceFacilitatorTax-Principal:ItemWithheldTax',
            'MarketplaceFacilitatorVAT-Principal:ItemWithheldTax',
            'LowValueGoodsTax-Principal:ItemWithheldTax'
        ]
        pivot_df['Product Tax'] = pivot_df[product_tax_cols].sum(axis=1)
        pivot_df = pivot_df.drop(product_tax_cols, axis=1, errors='ignore')

        pivot_df['Shipping'] = pivot_df['Shipping:ItemPrice'] + pivot_df['Shipping:Promotion']
        pivot_df = pivot_df.drop(['Shipping:ItemPrice', 'Shipping:Promotion'], axis=1, errors='ignore')

        pivot_df['Giftwrap'] = pivot_df['GiftWrap:ItemPrice'] + pivot_df['GiftWrap:Promotion']
        pivot_df = pivot_df.drop(['GiftWrap:ItemPrice', 'GiftWrap:Promotion'], axis=1, errors='ignore')

        giftwrap_tax_cols = [
            'GiftWrapTax:ItemPrice',
            'MarketplaceFacilitatorTax-Other:ItemWithheldTax'
        ]
        pivot_df['Giftwrap Tax'] = pivot_df[giftwrap_tax_cols].sum(axis=1)
        pivot_df = pivot_df.drop(giftwrap_tax_cols, axis=1, errors='ignore')

        pivot_df['Total_amount'] = pivot_df[['Product Tax', 'Product Amount', 'Giftwrap', 'Giftwrap Tax']].sum(axis=1)

        if 'Shipping Tax' not in pivot_df.columns:
            pivot_df['Shipping Tax'] = 0
        pivot_df['Total_shipping'] = pivot_df['Shipping'] + pivot_df['Shipping Tax']

        pivot_df['tax_rate'] = np.where(
            pivot_df['Product Amount'] != 0,
            (pivot_df['Product Tax'] / pivot_df['Product Amount']).round(2),
            0
        )
        pivot_df['tax_rate'] = pivot_df['tax_rate'].apply(lambda x: f"{x:.0%}")

        final_columns = [
            'order-id', 'shipment-id', 'sku',
            'Product Amount', 'Product Tax', 'tax_rate',
            'Shipping', 'Shipping Tax', 'Total_shipping',
            'Giftwrap', 'Giftwrap Tax', 'Total_amount'
        ]

        return pivot_df[final_columns].sort_values("shipment-id")

    except Exception as e:
        st.error(f"Order processing failed: {str(e)}")
        return None


# ==================== CA Refund Processing ====================
def calculate_tax_code(tax_location):
    """Calculate tax code based on location - matches original amazon_ca_qty_order.py"""
    if not tax_location:
        return ''

    jurisdiction_upper = tax_location.upper()

    if jurisdiction_upper in ['MANITOBA', 'SASKATCHEWAN', 'ALBERTA', 'QUEBEC',
                             'BRITISH COLUMBIA', 'NUNAVUT', 'NORTHWEST TERRITORIES',
                             'YUKON TERRITORY']:
        return 'GST'
    elif jurisdiction_upper == 'NEW BRUNSWICK':
        return 'HST NB 2016'
    elif jurisdiction_upper == 'ONTARIO':
        return 'HST ON'
    elif jurisdiction_upper == 'NOVA SCOTIA':
        return 'HST NS 2025'
    elif jurisdiction_upper == 'PRINCE EDWARD ISLAND':
        return 'HST PEI'
    elif jurisdiction_upper == 'NEWFOUNDLAND AND LABRADOR':
        return 'HST NL 2016'
    else:
        return ''


def process_refund_data(refund_raw_df, tax_report_mapping=None):
    """Process refund data for CA region"""
    try:
        refund_df = refund_raw_df.copy()

        refund_df = refund_df[
            (refund_df['transaction-type'].str.lower().str.contains('refund', na=False)) &
            (refund_df['marketplace-name'] == 'Amazon.ca')
        ]

        if len(refund_df) == 0:
            return None

        cols_to_drop = [
            'settlement-id', 'settlement-start-date', 'settlement-end-date',
            'deposit-date', 'total-amount', 'currency', 'transaction-type',
            'merchant-order-id', 'adjustment-id', 'marketplace-name',
            'fulfillment-id', 'posted-date', 'posted-date-time',
            'order-item-code', 'merchant-order-item-id',
            'merchant-adjustment-item-id', 'quantity-purchased', 'promotion-id'
        ]
        refund_df = refund_df.drop(columns=[c for c in cols_to_drop if c in refund_df.columns])

        refund_df['des-type'] = refund_df['amount-description'] + ":" + refund_df['amount-type']

        index_cols = ['order-id', 'sku']
        if 'shipment-id' in refund_df.columns and refund_df['shipment-id'].notna().any():
            index_cols.insert(1, 'shipment-id')

        pivot_df = refund_df.pivot_table(
            index=index_cols,
            columns='des-type',
            values='amount',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        required_columns = [
            "Principal:ItemPrice", "Principal:Promotion",
            "Tax:ItemPrice", "MarketplaceFacilitatorTax-Principal:ItemWithheldTax",
            "MarketplaceFacilitatorVAT-Principal:ItemWithheldTax",
            "LowValueGoodsTax-Principal:ItemWithheldTax",
            "Shipping:ItemPrice", "Shipping:Promotion",
            "GiftWrap:ItemPrice", "GiftWrap:Promotion",
            "GiftWrapTax:ItemPrice", "MarketplaceFacilitatorTax-Other:ItemWithheldTax"
        ]

        existing_columns = pivot_df.columns.tolist()
        for col in required_columns:
            if col not in existing_columns:
                pivot_df[col] = 0

        pivot_df['Product Amount'] = pivot_df['Principal:ItemPrice'] + pivot_df['Principal:Promotion']
        pivot_df = pivot_df.drop(['Principal:ItemPrice', 'Principal:Promotion'], axis=1, errors='ignore')

        product_tax_cols = [
            'Tax:ItemPrice',
            'MarketplaceFacilitatorTax-Principal:ItemWithheldTax',
            'MarketplaceFacilitatorVAT-Principal:ItemWithheldTax',
            'LowValueGoodsTax-Principal:ItemWithheldTax'
        ]
        pivot_df['Product Tax'] = pivot_df[product_tax_cols].sum(axis=1)
        pivot_df = pivot_df.drop(product_tax_cols, axis=1, errors='ignore')

        pivot_df['Shipping'] = pivot_df['Shipping:ItemPrice'] + pivot_df['Shipping:Promotion']
        pivot_df = pivot_df.drop(['Shipping:ItemPrice', 'Shipping:Promotion'], axis=1, errors='ignore')

        pivot_df['Giftwrap'] = pivot_df['GiftWrap:ItemPrice'] + pivot_df['GiftWrap:Promotion']
        pivot_df = pivot_df.drop(['GiftWrap:ItemPrice', 'GiftWrap:Promotion'], axis=1, errors='ignore')

        giftwrap_tax_cols = [
            'GiftWrapTax:ItemPrice',
            'MarketplaceFacilitatorTax-Other:ItemWithheldTax'
        ]
        pivot_df['Giftwrap Tax'] = pivot_df[giftwrap_tax_cols].sum(axis=1)
        pivot_df = pivot_df.drop(giftwrap_tax_cols, axis=1, errors='ignore')

        exclude_cols = ['shipment-id', 'order-id', 'sku', 'tax_rate']
        sum_cols = [col for col in pivot_df.columns if col not in exclude_cols]
        pivot_df['Total_amount'] = pivot_df[sum_cols].sum(axis=1)

        if tax_report_mapping:
            pivot_df['tax_location'] = pivot_df['order-id'].map(tax_report_mapping).fillna('')
        else:
            pivot_df['tax_location'] = ''

        pivot_df['tax_rate'] = np.where(
            pivot_df['Product Amount'] != 0,
            (pivot_df['Product Tax'] / pivot_df['Product Amount']).round(2),
            0
        )
        pivot_df['tax_rate'] = pivot_df['tax_rate'].apply(lambda x: f"{x:.0%}")

        pivot_df['tax_code'] = np.where(
            pivot_df['tax_rate'] == '0%',
            'OUT OF SCOPE',
            pivot_df['tax_location'].apply(calculate_tax_code)
        )

        if 'Shipping Tax' not in pivot_df.columns:
            pivot_df['Shipping Tax'] = 0

        preferred_start = ['order-id', 'shipment-id', 'sku',
                   'Product Amount', 'Product Tax', 'Shipping', 'Shipping Tax',
                   'Giftwrap', 'Giftwrap Tax']
        preferred_start = [col for col in preferred_start if col in pivot_df.columns]

        all_cols = pivot_df.columns.tolist()
        middle_cols = [col for col in all_cols if col not in preferred_start + ['Total_amount', 'tax_rate', 'tax_location', 'tax_code']]
        final_columns = preferred_start + middle_cols + ['Total_amount', 'tax_rate', 'tax_location', 'tax_code']

        return pivot_df[final_columns]

    except Exception as e:
        st.error(f"Refund processing failed: {str(e)}")
        return None


def generate_refund_summary_monthly(monthly_refund_data):
    """Generate refund summary pivot tables by month"""
    try:
        if not monthly_refund_data:
            return None

        valid_months = {}
        for month_key, refund_df in monthly_refund_data.items():
            if refund_df is not None and len(refund_df) > 0:
                valid_months[month_key] = refund_df

        if not valid_months:
            return None

        pivot_tables = []
        for month_key, refund_df in valid_months.items():
            pivot = refund_df.pivot_table(
                index=['tax_code'],
                values='Total_amount',
                aggfunc='sum',
                fill_value=0,
                margins=True,
                margins_name='Grand Total'
            ).round(2).reset_index()

            pivot.columns = ['Tax Code', 'Total Amount']
            pivot_tables.append((month_key, pivot))

        return pivot_tables

    except Exception as e:
        st.error(f"Refund summary generation failed: {str(e)}")
        return None


# ==================== Main Processing ====================
def process_data(file, start_date, end_date, landed_cost_data, pdb_us_data, region="US", tax_report_file=None):
    """Main data processing pipeline - matches original amazon_ca_qty_order.py structure"""
    try:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        raw_source_df = pd.read_csv(file, delimiter='\t').iloc[1:]
        date_format = '%Y-%m-%d' if region == "US" else '%d.%m.%Y'
        raw_source_df['posted-date'] = pd.to_datetime(raw_source_df['posted-date'], format=date_format, errors='coerce')

        raw_df = raw_source_df.copy()
        raw_df = raw_df.dropna(subset=['posted-date'])

        # Process Tax Report for CA - store state_tax_data
        state_tax_data = None
        tax_report_mapping = {}
        if region == "CA" and tax_report_file is not None:
            try:
                tax_report_df = pd.read_csv(tax_report_file)
                tax_report_df.columns = [col.strip().replace(' ', '_') for col in tax_report_df.columns]

                if 'Jurisdiction_Level' in tax_report_df.columns and 'Jurisdiction_Name' in tax_report_df.columns:
                    state_tax_data = tax_report_df[
                        (tax_report_df['Jurisdiction_Level'] == 'State') &
                        (tax_report_df['Tax_Address_Role'] == 'ShipTo')]

                    for _, row in state_tax_data.iterrows():
                        order_id = str(row.get('Order_ID', '')).strip()
                        jurisdiction = str(row.get('Jurisdiction_Name', '')).strip()
                        if order_id and jurisdiction:
                            tax_report_mapping[order_id] = jurisdiction
            except Exception as e:
                st.warning(f"Tax Report processing failed: {str(e)}")

        output = io.BytesIO()

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # 1. Generate Summary
            pivot_tables = generate_summary(raw_df, start_date, end_date, region)
            if pivot_tables:
                start_row = 0
                for month, pivot in pivot_tables:
                    pivot.to_excel(writer, sheet_name='Summary', index=False, startrow=start_row, float_format="%.2f")
                    start_row += len(pivot) + 3

            # 2. CA: Collect monthly data first (don't write yet)
            monthly_results = {}
            monthly_refund_data = {}
            is_multi_month = start_date.month != end_date.month or start_date.year != end_date.year

            if is_multi_month:
                monthly_data = split_data_by_month(raw_df, start_date, end_date)
                for month_key, month_df in monthly_data.items():
                    month_start = month_df['posted-date'].min().to_pydatetime()
                    month_end = month_df['posted-date'].max().to_pydatetime()

                    qty_df, _, _ = process_qty_data(month_df, month_start, month_end, region)
                    order_df = process_order_data(month_df, region)

                    # CA: Process refund data
                    refund_df = None
                    if region == "CA":
                        refund_df = process_refund_data(month_df, tax_report_mapping)
                        monthly_refund_data[month_key] = refund_df

                    # Merge and calculate order_import
                    merged_month = None
                    order_import_df = None
                    if qty_df is not None and order_df is not None:
                        merged_month = merge_order_qty(order_df, qty_df, raw_source_df)
                        if merged_month is not None:
                            if region == "CA" and tax_report_mapping:
                                merged_month['tax_location'] = merged_month['order-id'].map(tax_report_mapping).fillna('')

                            if not merged_month.empty and 'master_sku' in merged_month.columns:
                                grouped = merged_month.groupby('master_sku', as_index=False).agg({
                                    'QTY': 'sum',
                                    'Total_amount': 'sum'
                                }).rename(columns={'QTY': 'total QTY', 'Total_amount': 'total amount'})

                                if not grouped.empty:
                                    grouped['product_rate'] = np.where(
                                        grouped['total QTY'] > 0,
                                        (grouped['total amount'] / grouped['total QTY']).round(2),
                                        0.0
                                    )

                                grouped['product_cost'] = grouped['master_sku'].apply(
                                    lambda sku: 0.0 if str(sku).strip().lower() == "shipping"
                                    else landed_cost_data.get(str(sku).strip(), pdb_us_data.get(str(sku).strip(), None))
                                )
                                grouped['total_cost'] = grouped['product_cost'] * grouped['total QTY']

                                try:
                                    sum_total_shipping = merged_month['Total_shipping'].sum()
                                    if sum_total_shipping != 0:
                                        new_row = pd.DataFrame([{
                                            'master_sku': 'Shipping',
                                            'total QTY': 1,
                                            'total amount': sum_total_shipping,
                                            'product_rate': sum_total_shipping,
                                            'product_cost': 0,
                                            'total_cost': 0
                                        }])
                                        grouped = pd.concat([grouped, new_row], ignore_index=True)
                                except:
                                    pass

                                order_import_df = grouped

                    monthly_results[month_key] = {
                        'qty_df': qty_df,
                        'order_df': order_df,
                        'refund_df': refund_df,
                        'merged_month': merged_month,
                        'order_import_df': order_import_df
                    }
            else:
                # Single month processing
                qty_df, _, _ = process_qty_data(raw_df, start_date, end_date, region)
                order_df = process_order_data(raw_df, region)

                # CA: Process refund data
                refund_df = None
                if region == "CA":
                    refund_df = process_refund_data(raw_df, tax_report_mapping)

                # Merge and calculate order_import
                merged_all = None
                order_import_df = None
                if qty_df is not None and order_df is not None:
                    merged_all = merge_order_qty(order_df, qty_df, raw_source_df)
                    if merged_all is not None:
                        if region == "CA" and tax_report_mapping:
                            merged_all['tax_location'] = merged_all['order-id'].map(tax_report_mapping).fillna('')

                        if not merged_all.empty and 'master_sku' in merged_all.columns:
                            grouped = merged_all.groupby('master_sku', as_index=False).agg({
                                'QTY': 'sum',
                                'Total_amount': 'sum'
                            }).rename(columns={'QTY': 'total QTY', 'Total_amount': 'total amount'})

                            if not grouped.empty:
                                grouped['product_rate'] = np.where(
                                    grouped['total QTY'] > 0,
                                    (grouped['total amount'] / grouped['total QTY']).round(2),
                                    0.0
                                )

                            grouped['product_cost'] = grouped['master_sku'].apply(
                                lambda sku: 0.0 if str(sku).strip().lower() == "shipping"
                                else landed_cost_data.get(str(sku).strip(), pdb_us_data.get(str(sku).strip(), None))
                            )
                            grouped['total_cost'] = grouped['product_cost'] * grouped['total QTY']

                            try:
                                sum_total_shipping = merged_all['Total_shipping'].sum()
                                if sum_total_shipping != 0:
                                    new_row = pd.DataFrame([{
                                        'master_sku': 'Shipping',
                                        'total QTY': 1,
                                        'total amount': sum_total_shipping,
                                        'product_rate': sum_total_shipping,
                                        'product_cost': 0,
                                        'total_cost': 0
                                    }])
                                    grouped = pd.concat([grouped, new_row], ignore_index=True)
                            except:
                                pass

                            order_import_df = grouped

                monthly_results['single'] = {
                    'qty_df': qty_df,
                    'order_df': order_df,
                    'refund_df': refund_df,
                    'merged_all': merged_all,
                    'order_import_df': order_import_df
                }

            # 3. CA: Generate Refund Summary
            if region == "CA" and monthly_refund_data:
                refund_summary_tables = generate_refund_summary_monthly(monthly_refund_data)
                if refund_summary_tables:
                    start_row = 0
                    for month_key, pivot in refund_summary_tables:
                        month_title = pd.DataFrame({f"Month: {month_key}": [""]})
                        month_title.to_excel(writer, sheet_name='Refund Summary', index=False, startrow=start_row, header=True)
                        start_row += 2
                        pivot.to_excel(writer, sheet_name='Refund Summary', index=False, startrow=start_row, float_format="%.2f")
                        start_row += len(pivot) + 3

            # 4. CA: Write tax report filter
            if region == "CA" and state_tax_data is not None and not state_tax_data.empty:
                state_tax_data.to_excel(writer, sheet_name='tax report filter', index=False)

            # 5. Write monthly/single sheets in original order
            if is_multi_month:
                for month_key, result in monthly_results.items():
                    # qty
                    if result['qty_df'] is not None:
                        result['qty_df'].to_excel(writer, sheet_name=f"{month_key}_qty", index=False)
                    # order
                    if result['order_df'] is not None:
                        result['order_df'].to_excel(writer, sheet_name=f"{month_key}_order", index=False)
                    # refund
                    if result['refund_df'] is not None and not result['refund_df'].empty:
                        result['refund_df'].to_excel(writer, sheet_name=f"{month_key}_refund", index=False)
                    else:
                        # Create empty refund table with correct columns
                        empty_refund_df = pd.DataFrame(columns=[
                            'order-id', 'shipment-id', 'sku',
                            'Product Amount', 'Product Tax', 'tax_rate',
                            'Shipping', 'Shipping Tax', 'Total_shipping',
                            'Giftwrap', 'Giftwrap Tax', 'Total_amount', 'tax_location', 'tax_code'
                        ])
                        empty_refund_df.to_excel(writer, sheet_name=f"{month_key}_refund", index=False)
                    # order_details
                    if result['merged_month'] is not None:
                        result['merged_month'].to_excel(writer, sheet_name=f"{month_key}_order_details", index=False)
                    # order_import
                    if result['order_import_df'] is not None and not result['order_import_df'].empty:
                        result['order_import_df'].to_excel(writer, sheet_name=f"{month_key}_order_import", index=False)
            else:
                result = monthly_results.get('single', {})
                # qty
                if result.get('qty_df') is not None:
                    result['qty_df'].to_excel(writer, sheet_name='qty', index=False)
                # order
                if result.get('order_df') is not None:
                    result['order_df'].to_excel(writer, sheet_name='order', index=False)
                # refund
                if result.get('refund_df') is not None and not result['refund_df'].empty:
                    result['refund_df'].to_excel(writer, sheet_name='refund', index=False)
                else:
                    empty_refund_df = pd.DataFrame(columns=[
                        'order-id', 'shipment-id', 'sku',
                        'Product Amount', 'Product Tax', 'tax_rate',
                        'Shipping', 'Shipping Tax', 'Total_shipping',
                        'Giftwrap', 'Giftwrap Tax', 'Total_amount', 'tax_location', 'tax_code'
                    ])
                    empty_refund_df.to_excel(writer, sheet_name='refund', index=False)
                # order_details
                if result.get('merged_all') is not None:
                    result['merged_all'].to_excel(writer, sheet_name='order_details', index=False)
                # order_import
                if result.get('order_import_df') is not None and not result['order_import_df'].empty:
                    result['order_import_df'].to_excel(writer, sheet_name='order_import', index=False)

        output.seek(0)
        return output

    except Exception as e:
        st.error(f"Processing failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


# ==================== Streamlit UI ====================
st.set_page_config(page_title="Amazon Processor", page_icon="📊", layout="wide")

st.title("Amazon Processor")
st.markdown("### Amazon Data Processing")

# Initialize session state
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'output_file' not in st.session_state:
    st.session_state.output_file = None
if 'date_range' not in st.session_state:
    st.session_state.date_range = None
if 'total_amount' not in st.session_state:
    st.session_state.total_amount = None

# Region selector
region = st.selectbox("Select Region", options=["US", "CA"], index=0)

# File upload - always visible
uploaded_file = st.file_uploader("Upload Amazon Report (TXT)", type=['txt'])

# Tax report uploader (CA only) - always visible but only required for CA
tax_report_file = None
if region == "CA":
    tax_report_file = st.file_uploader("Upload Tax Report (CSV) - Required for CA", type=['csv'])

if uploaded_file is None:
    st.info("Please upload an Amazon report file to begin.")
    st.stop()

# Read and preview
df_preview = pd.read_csv(uploaded_file, delimiter='\t', nrows=5)
st.write("File preview:")
st.dataframe(df_preview.head())

# Calculate total amount
uploaded_file.seek(0)
df_full = pd.read_csv(uploaded_file, delimiter='\t')
total_amount = df_full['amount'].sum() if 'amount' in df_full.columns else None
st.session_state.total_amount = total_amount

if total_amount:
    st.info(f"Total Amount: ${total_amount:,.2f}")

# Get date range from file
# US uses %Y-%m-%d, CA uses %d.%m.%Y
date_format = '%Y-%m-%d' if region == "US" else '%d.%m.%Y'
uploaded_file.seek(0)
df_dates = pd.read_csv(uploaded_file, delimiter='\t', usecols=['posted-date'], dtype={'posted-date': 'string'})
dates = pd.to_datetime(df_dates['posted-date'], format=date_format, errors='coerce').dropna()

if dates.empty:
    st.error("No valid date data found in file.")
    st.stop()

min_date = dates.min().date()
max_date = dates.max().date()
st.session_state.date_range = (min_date, max_date)

st.markdown("#### Select Date Range")
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date)
with col2:
    end_date = st.date_input("End Date", value=max_date, min_value=min_date, max_value=max_date)

# CA validation
if region == "CA" and tax_report_file is None:
    st.warning("CA region requires a Tax Report file.")

# Process button
if st.button("Process Data", type="primary"):
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Step 1: Load Google Sheets
    status_text.text("Loading Google Sheets data...")
    progress_bar.progress(20)
    landed_cost_data = load_gsheet_data("landed_cost", region)
    progress_bar.progress(40)
    pdb_us_data = load_gsheet_data("pdb_us", region)
    progress_bar.progress(50)

    if landed_cost_data is None or pdb_us_data is None:
        progress_bar.empty()
        status_text.empty()
        st.error("Failed to load cost data from Google Sheets. Please check your connection and try again.")
    else:
        # Step 2: Process data
        status_text.text("Processing data...")
        progress_bar.progress(70)
        uploaded_file.seek(0)
        output = process_data(uploaded_file, start_date, end_date, landed_cost_data, pdb_us_data, region, tax_report_file)
        progress_bar.progress(100)

        if output:
            st.session_state.processing_complete = True
            st.session_state.output_file = output
            progress_bar.empty()
            status_text.empty()
            st.success("Processing complete!")
        else:
            progress_bar.empty()
            status_text.empty()

# Download button
if st.session_state.processing_complete and st.session_state.output_file:
    st.download_button(
        label="Download Excel Report",
        data=st.session_state.output_file,
        file_name=f"Amazon_{region}_Report_{start_date}_{end_date}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Please upload an Amazon report file to begin.")
