import pandas as pd
import numpy as np
import streamlit as st
from io import BytesIO
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import MaxNLocator

def preprocess_data(df):
    logs = []
    required_cols = [
        'Article', 'Article Description', 'RP Type', 'Site', 'OM', 'MOQ',
        'SaSa Net Stock', 'Pending Received', 'Safety Stock',
        'Last Month Sold Qty', 'MTD Sold Qty'
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        error_msg = f"錯誤: Excel文件缺少以下必需欄位: {', '.join(missing_cols)}"
        logs.append(error_msg)
        st.error(error_msg)
        return None, logs

    df['Notes'] = ''

    df['Article'] = df['Article'].astype(str)
    logs.append("Info: 'Article' 欄位已強制轉換為字串類型。")

    quantity_cols = [
        'MOQ', 'SaSa Net Stock', 'Pending Received', 'Safety Stock',
        'Last Month Sold Qty', 'MTD Sold Qty'
    ]
    for col in quantity_cols:
        original_nan_mask = pd.to_numeric(df[col], errors='coerce').isnull()
        if original_nan_mask.any():
            df.loc[original_nan_mask, 'Notes'] += f'{col}非數字值已填充為0; '
            df.loc[original_nan_mask, col] = 0
            logs.append(f"Warning: '{col}' 欄位中的非數字值已填充為0。")

        df[col] = df[col].astype(int)

        negative_mask = df[col] < 0
        if negative_mask.any():
            df.loc[negative_mask, 'Notes'] += f'{col}負值已修正為0; '
            df.loc[negative_mask, col] = 0
            logs.append(f"Warning: '{col}' 欄位中的負值已修正為0。")

        if col in ['Last Month Sold Qty', 'MTD Sold Qty']:
            limit = 100000
            over_limit_mask = df[col] > limit
            if over_limit_mask.any():
                df.loc[over_limit_mask, 'Notes'] += f'{col}超過{limit}已限制為{limit}; '
                df.loc[over_limit_mask, col] = limit
                logs.append(f"Warning: '{col}' 中超過 {limit} 的值已限制為 {limit}。")

    string_cols = ['Article Description', 'RP Type', 'Site', 'OM']
    for col in string_cols:
        nan_mask = df[col].isnull() | (df[col] == '')
        if nan_mask.any():
            df.loc[nan_mask, 'Notes'] += f'{col}空值已填充; '
            df.loc[nan_mask, col] = ''
            logs.append(f"Info: '{col}' 欄位中的空值已填充。")

    valid_rp_types = ['ND', 'RF']
    invalid_rp_mask = ~df['RP Type'].isin(valid_rp_types)
    if invalid_rp_mask.any():
        error_msg = f"錯誤: 'RP Type' 欄位包含無效值。只允許 {valid_rp_types}。"
        st.error(error_msg)
        logs.append(error_msg)
        df = df[df['RP Type'].isin(valid_rp_types)]
        logs.append("Warning: 已過濾掉 'RP Type' 無效的行。")

    return df, logs

def _calculate_candidates(df, transfer_mode):
    """
    內部輔助函數，根據業務規則識別轉出和接收候選。
    此函數不執行匹配。
    """
    mode = transfer_mode[0]  # 'A' or 'B'
    
    senders = []
    receivers = []
    
    grouped = df.groupby('Article')
    
    for article, group in grouped:
        max_sales_in_group = group['Effective Sold Qty'].max()
        
        sorted_group = group
        if mode == 'B':
            sorted_group = group.sort_values(by=['Last Month Sold Qty', 'MTD Sold Qty'], ascending=True)

        for _, row in sorted_group.iterrows():
            stock = row['SaSa Net Stock']
            pending = row['Pending Received']
            safety_stock = row['Safety Stock']
            effective_sales = row['Effective Sold Qty']
            moq = row['MOQ']
            
            # --- 轉出候選邏輯 ---
            if row['RP Type'] == 'ND' and stock > 0:
                senders.append({
                    'type': 'ND轉出', 'priority': 1, 'data': row, 
                    'available_qty': stock, 'current_stock': stock
                })
            
            if mode == 'A':
                if row['RP Type'] == 'RF' and (stock + pending) > safety_stock and effective_sales < max_sales_in_group:
                    base_transferable = (stock + pending) - safety_stock
                    upper_limit = (stock + pending) * 0.2
                    actual_transfer = min(base_transferable, max(upper_limit, 2))
                    actual_transfer = min(actual_transfer, stock)
                    
                    if actual_transfer > 0:
                        senders.append({
                            'type': 'RF過剩轉出', 'priority': 2, 'data': row,
                            'available_qty': int(np.floor(actual_transfer)),
                            'current_stock': stock
                        })
            
            elif mode == 'B':
                if row['RP Type'] == 'RF' and (stock + pending) > (moq + 1) and effective_sales < max_sales_in_group:
                    base_transferable = (stock + pending) - (moq + 1)
                    upper_limit = (stock + pending) * 0.5
                    actual_transfer = min(base_transferable, max(upper_limit, 2))
                    actual_transfer = min(actual_transfer, stock)

                    if actual_transfer > 0:
                        senders.append({
                            'type': 'RF加強轉出', 'priority': 2, 'data': row,
                            'available_qty': int(np.floor(actual_transfer)),
                            'current_stock': stock
                        })

            # --- 接收候選邏輯 ---
            if row['RP Type'] == 'RF' and stock == 0 and effective_sales > 0:
                receivers.append({
                    'type': '緊急缺貨補貨', 'priority': 1, 'data': row,
                    'needed_qty': safety_stock
                })
            elif row['RP Type'] == 'RF' and (stock + pending) < safety_stock and effective_sales == max_sales_in_group:
                needed = safety_stock - (stock + pending)
                if needed > 0:
                    receivers.append({
                        'type': '潛在缺貨補貨', 'priority': 2, 'data': row,
                        'needed_qty': needed
                    })
                    
    return senders, receivers

def estimate_transfer_potential(df):
    """
    為兩種模式預先計算潛在的可轉出和需求數量，
    以便在運行完整分析前向用戶展示。
    """
    df_copy = df.copy()
    df_copy['Effective Sold Qty'] = np.where(df_copy['Last Month Sold Qty'] > 0, df_copy['Last Month Sold Qty'], df_copy['MTD Sold Qty'])

    senders_A, receivers = _calculate_candidates(df_copy, 'A: 保守轉貨')
    senders_B, _ = _calculate_candidates(df_copy, 'B: 加強轉貨')

    total_needed = sum(r['needed_qty'] for r in receivers)
    potential_A = sum(s['available_qty'] for s in senders_A)
    potential_B = sum(s['available_qty'] for s in senders_B)

    return {
        "potential_transfer_A": int(potential_A),
        "potential_transfer_B": int(potential_B),
        "total_needed": int(total_needed)
    }

def generate_recommendations(df, transfer_mode):
    """
    根據所選模式，通過匹配轉出和接收候選來生成調貨建議。
    """
    recommendations = []
    df['Effective Sold Qty'] = np.where(df['Last Month Sold Qty'] > 0, df['Last Month Sold Qty'], df['MTD Sold Qty'])
    
    senders, receivers = _calculate_candidates(df, transfer_mode)
    
    # 根據新的業務邏輯調整排序
    # 1. ND 類型 (priority 1) 優先處理
    # 2. RF 類型 (priority 2) 根據以下規則排序:
    #    - 優先處理可轉出數量 >= 2 的店舖
    #    - 其次，按當前庫存量從高到低排序
    nd_senders = [s for s in senders if s['priority'] == 1]
    rf_senders = [s for s in senders if s['priority'] == 2]
    
    # 複合排序：(可轉出>=2, 當前庫存) -> 降序
    rf_senders.sort(key=lambda x: (x['available_qty'] >= 2, x['current_stock']), reverse=True)
    
    senders = nd_senders + rf_senders
    receivers.sort(key=lambda x: x['priority'])

    for sender in senders:
        for receiver in receivers:
            if sender['available_qty'] > 0 and receiver['needed_qty'] > 0 and \
               sender['data']['Article'] == receiver['data']['Article'] and \
               sender['data']['OM'] == receiver['data']['OM'] and \
               sender['data']['Site'] != receiver['data']['Site']:
                
                transfer_qty = min(sender['available_qty'], receiver['needed_qty'])
                
                # 新的排序邏輯已取代舊的單件轉出規則
                final_transfer_qty = min(transfer_qty, sender['current_stock'])
                
                if final_transfer_qty > 0:
                    sender_type = sender['type']
                    # B模式下，根據轉出後庫存是否低於安全庫存，重新定義轉出類型
                    if transfer_mode.startswith('B') and sender['type'] == 'RF加強轉出':
                        remaining_stock = sender['current_stock'] - final_transfer_qty
                        safety_stock = sender['data']['Safety Stock']
                        if remaining_stock >= safety_stock:
                            sender_type = 'RF過剩轉出'
                        # 如果不滿足，sender_type 保持為 'RF加強轉出'

                    recommendations.append({
                        'Article': sender['data']['Article'],
                        'Product Desc': sender['data']['Article Description'],
                        'OM': sender['data']['OM'],
                        'Transfer Site': sender['data']['Site'],
                        'Receive Site': receiver['data']['Site'],
                        'Transfer Qty': final_transfer_qty,
                        'Original Stock': sender['current_stock'],
                        'After Transfer Stock': sender['current_stock'] - final_transfer_qty,
                        'Safety Stock': sender['data']['Safety Stock'],
                        'MOQ': sender['data']['MOQ'],
                        'Notes': f"{sender_type} -> {receiver['type']}",
                        '_sender_type': sender_type,
                        '_receiver_type': receiver['type']
                    })
                    sender['available_qty'] -= final_transfer_qty
                    receiver['needed_qty'] -= final_transfer_qty
                    sender['current_stock'] -= final_transfer_qty

    if not recommendations:
        return pd.DataFrame(), {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    rec_df = pd.DataFrame(recommendations)

    kpi_metrics = {
        "總調貨建議數量": len(rec_df),
        "總調貨件數": int(rec_df['Transfer Qty'].sum()),
        "涉及產品數量": int(rec_df['Article'].nunique()),
        "涉及OM數量": int(rec_df['OM'].nunique())
    }

    stats_by_article = rec_df.groupby('Article').agg(
        總調貨件數=('Transfer Qty', 'sum'),
        調貨行數=('Article', 'count'),
        涉及OM數量=('OM', 'nunique')
    ).reset_index().round(2)

    stats_by_om = rec_df.groupby('OM').agg(
        總調貨件數=('Transfer Qty', 'sum'),
        調貨行數=('OM', 'count'),
        涉及產品數量=('Article', 'nunique')
    ).reset_index().round(2)

    transfer_type_dist = rec_df.groupby('_sender_type').agg(
        總件數=('Transfer Qty', 'sum'),
        涉及行數=('_sender_type', 'count')
    ).reset_index().round(2)

    receive_type_dist = rec_df.groupby('_receiver_type').agg(
        總件數=('Transfer Qty', 'sum'),
        涉及行數=('_receiver_type', 'count')
    ).reset_index().round(2)
    
    return rec_df, kpi_metrics, stats_by_article, stats_by_om, transfer_type_dist, receive_type_dist

def create_om_transfer_chart(recommendations_df, transfer_mode):
    if recommendations_df.empty:
        return plt.figure()

    df = recommendations_df.copy()

    nd_transfer = df[df['_sender_type'] == 'ND轉出'].groupby('OM')['Transfer Qty'].sum()
    rf_surplus_transfer = df[df['_sender_type'] == 'RF過剩轉出'].groupby('OM')['Transfer Qty'].sum()

    transfer_data_dict = {
        'ND Transfer Out': nd_transfer,
        'RF Surplus Transfer Out': rf_surplus_transfer
    }

    if transfer_mode.startswith('B'):
        rf_enhanced_transfer = df[df['_sender_type'] == 'RF加強轉出'].groupby('OM')['Transfer Qty'].sum()
        transfer_data_dict['RF Enhanced Transfer Out'] = rf_enhanced_transfer
        
    urgent_receive = df[df['_receiver_type'] == '緊急缺貨補貨'].groupby('OM')['Transfer Qty'].sum()
    potential_receive = df[df['_receiver_type'] == '潛在缺貨補貨'].groupby('OM')['Transfer Qty'].sum()

    all_oms = df['OM'].unique()
    
    transfer_data = pd.DataFrame(transfer_data_dict).reindex(all_oms).fillna(0)
    
    receive_data = pd.DataFrame({
        'Urgent Shortage Receive': urgent_receive, 
        'Potential Shortage Receive': potential_receive
    }).reindex(all_oms).fillna(0)
    
    chart_data = pd.concat([transfer_data, receive_data], axis=1).fillna(0)

    fig, ax = plt.subplots(figsize=(18, 10))
    
    chart_data.plot(kind='bar', ax=ax, width=0.8)

    for p in ax.patches:
        if p.get_height() > 0:
            ax.annotate(f'{int(p.get_height())}', 
                        (p.get_x() + p.get_width() / 2., p.get_height()), 
                        ha='center', va='center', 
                        xytext=(0, 9), 
                        textcoords='offset points',
                        fontsize=9)

    ax.set_title('OM Transfer vs Receive Analysis', fontsize=18, weight='bold')
    ax.set_xlabel('OM Unit', fontsize=14)
    ax.set_ylabel('Transfer Quantity', fontsize=14)
    ax.tick_params(axis='x', rotation=45, labelsize=12)
    ax.tick_params(axis='y', labelsize=12)
    
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    
    ax.legend(title='Transfer/Receive Type', fontsize=12)
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()
    return fig

def generate_excel_export(rec_df, kpis, stats_article, stats_om, transfer_dist, receive_dist, transfer_mode):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        column_order = [
            'Article', 'Product Desc', 'OM', 'Transfer Site', 'Receive Site', 
            'Transfer Qty', 'Original Stock', 'After Transfer Stock', 
            'Safety Stock', 'MOQ', 'Notes'
        ]
        
        export_rec_df = rec_df.copy()
        for col in column_order:
            if col not in export_rec_df.columns:
                export_rec_df[col] = ''
        
        export_rec_df = export_rec_df[column_order]
        export_rec_df.to_excel(writer, sheet_name='調貨建議', index=False)

        summary_sheet_name = '統計摘要'
        start_row = 0

        def write_df_with_title(df, title, row):
            pd.DataFrame([title]).to_excel(writer, sheet_name=summary_sheet_name, startrow=row, index=False, header=False)
            df.to_excel(writer, sheet_name=summary_sheet_name, startrow=row + 2, index=False)
            return row + len(df) + 5

        kpi_df = pd.DataFrame([kpis])
        start_row = write_df_with_title(kpi_df, "KPI概覽", start_row)
        
        start_row = write_df_with_title(stats_article, "按Article統計", start_row)
        
        start_row = write_df_with_title(stats_om, "按OM統計", start_row)
        
        start_row = write_df_with_title(transfer_dist, "轉出類型分佈", start_row)
        
        write_df_with_title(receive_dist, "接收類型分佈", start_row)

    return output.getvalue()