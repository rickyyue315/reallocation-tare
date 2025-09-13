import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import datetime

def process_data(df):
    # 數據預處理與驗證
    integer_columns = ['SaSa Net Stock', 'Pending Received', 'Safety Stock', 'Last Month Sold Qty', 'MTD Sold Qty']
    for col in integer_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    string_columns = ['OM', 'RP Type', 'Site']
    for col in string_columns:
        df[col] = df[col].fillna('')

    df['Safety Stock'] = df['Safety Stock'].fillna(0)
    df['Last Month Sold Qty'] = df['Last Month Sold Qty'].fillna(0)
    df['MTD Sold Qty'] = df['MTD Sold Qty'].fillna(0)

    df['Notes'] = ''
    for col in ['Last Month Sold Qty', 'MTD Sold Qty']:
        df.loc[df[col] < 0, col] = 0
        out_of_range = df[col] > 100000
        df.loc[out_of_range, 'Notes'] += '銷量數據超出範圍 '
        df.loc[out_of_range, col] = 100000

    # 核心業務邏輯：調貨規則
    df['Effective Sold Qty'] = np.where(df['Last Month Sold Qty'] > 0, df['Last Month Sold Qty'], df['MTD Sold Qty'])

    recommendations = []
    
    for article_om, group in df.groupby(['Article', 'OM']):
        # 識別轉出候選
        source_candidates = []
        # 優先級 1: ND
        nd_sources = group[group['RP Type'] == 'ND'].copy()
        nd_sources['Transferable Qty'] = nd_sources['SaSa Net Stock']
        nd_sources['Priority'] = 1
        source_candidates.append(nd_sources)

        # 優先級 2: RF
        rf_sources = group[
            (group['RP Type'] == 'RF') &
            (group['SaSa Net Stock'] + group['Pending Received'] > group['Safety Stock'])
        ].copy()
        if not rf_sources.empty:
            max_effective_sold = group['Effective Sold Qty'].max()
            rf_sources = rf_sources[rf_sources['Effective Sold Qty'] < max_effective_sold]
            rf_sources['Transferable Qty'] = rf_sources['SaSa Net Stock'] + rf_sources['Pending Received'] - rf_sources['Safety Stock']
            rf_sources['Priority'] = 2
            source_candidates.append(rf_sources)

        # 識別接收候選
        destination_candidates = []
        # 優先級 1: 緊急缺貨
        urgent_dest = group[
            (group['RP Type'] == 'RF') &
            (group['SaSa Net Stock'] == 0) &
            (group['Effective Sold Qty'] > 0)
        ].copy()
        urgent_dest['Needed Qty'] = urgent_dest['Safety Stock']
        urgent_dest['Priority'] = 1
        destination_candidates.append(urgent_dest)

        # 優先級 2: 潛在缺貨
        potential_dest = group[
            (group['RP Type'] == 'RF') &
            (group['SaSa Net Stock'] + group['Pending Received'] < group['Safety Stock'])
        ].copy()

        if not urgent_dest.empty:
            potential_dest = potential_dest[~potential_dest.index.isin(urgent_dest.index)]

        if not potential_dest.empty:
            max_effective_sold = group['Effective Sold Qty'].max()
            potential_dest = potential_dest[potential_dest['Effective Sold Qty'] == max_effective_sold]
            if not potential_dest.empty:
                potential_dest['Needed Qty'] = potential_dest['Safety Stock'] - (potential_dest['SaSa Net Stock'] + potential_dest['Pending Received'])
                potential_dest['Priority'] = 2
                destination_candidates.append(potential_dest)

        # 執行匹配
        if source_candidates and destination_candidates:
            sources_df = pd.concat(source_candidates)
            dests_df = pd.concat(destination_candidates)

            if not sources_df.empty and not dests_df.empty:
                sources = sources_df.sort_values(by='Priority').to_dict('records')
                dests = dests_df.sort_values(by='Priority').to_dict('records')

                for s in sources:
                    for d in dests:
                        if s['Transferable Qty'] > 0 and d['Needed Qty'] > 0 and s['Site'] != d['Site']:
                            transfer_qty = min(s['Transferable Qty'], d['Needed Qty'])
                            
                            original_stock = group.loc[group['Site'] == s['Site'], 'SaSa Net Stock'].iloc[0]
                            if transfer_qty > original_stock:
                                transfer_qty = original_stock

                            if transfer_qty > 0:
                                recommendations.append({
                                    'Article': s['Article'],
                                    'Product Desc': s.get('Product Desc', ''),
                                    'OM': s['OM'],
                                    'Transfer Site': s['Site'],
                                    'Receive Site': d['Site'],
                                    'Transfer Qty': int(transfer_qty),
                                    'Notes': s['Notes']
                                })
                                s['Transferable Qty'] -= transfer_qty
                                d['Needed Qty'] -= transfer_qty
    return recommendations

def create_excel_report(recommendations, df):
    if not recommendations:
        return None

    rec_df = pd.DataFrame(recommendations)
    
    rec_df = rec_df[rec_df['Transfer Qty'] > 0]
    rec_df = rec_df[rec_df['Transfer Site'] != rec_df['Receive Site']]

    if rec_df.empty:
        return None

    # 統計摘要
    total_recommendations = len(rec_df)
    total_transfer_qty = rec_df['Transfer Qty'].sum()

    article_summary = rec_df.groupby('Article').agg(
        total_transfer_qty_per_article=pd.NamedAgg(column='Transfer Qty', aggfunc='sum'),
        om_count=pd.NamedAgg(column='OM', aggfunc='nunique')
    ).reset_index()

    om_summary = rec_df.groupby('OM').agg(
        total_transfer_qty_per_om=pd.NamedAgg(column='Transfer Qty', aggfunc='sum'),
        article_count=pd.NamedAgg(column='Article', aggfunc='nunique')
    ).reset_index()
    
    rec_df_merged = rec_df.merge(df[['Site', 'RP Type']], left_on='Transfer Site', right_on='Site', how='left')
    transfer_type_summary = rec_df_merged.groupby('RP Type').agg(
        recommendation_count=pd.NamedAgg(column='Article', aggfunc='size'),
        total_qty=pd.NamedAgg(column='Transfer Qty', aggfunc='sum')
    ).reset_index()

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        rec_df.to_excel(writer, sheet_name='調貨建議 (Transfer Recommendations)', index=False)
        
        summary_sheet = writer.book.create_sheet(title='統計摘要 (Summary Dashboard)')
        summary_sheet.cell(row=1, column=1, value="總調貨建議數量:")
        summary_sheet.cell(row=1, column=2, value=total_recommendations)
        summary_sheet.cell(row=2, column=1, value="總調貨件數:")
        summary_sheet.cell(row=2, column=2, value=total_transfer_qty)
        
        article_summary.to_excel(writer, sheet_name='統計摘要 (Summary Dashboard)', startrow=4, index=False)
        om_summary.to_excel(writer, sheet_name='統計摘要 (Summary Dashboard)', startrow=4 + len(article_summary) + 2, index=False)
        transfer_type_summary.to_excel(writer, sheet_name='統計摘要 (Summary Dashboard)', startrow=4 + len(article_summary) + 2 + len(om_summary) + 2, index=False)

    output.seek(0)
    return output

st.set_page_config(page_title="調貨建議工具", layout="wide")

st.title("調貨建議應用程式")
st.write("請上傳 Excel 檔案以生成調貨建議報告。")

uploaded_file = st.file_uploader("選擇一個 Excel 檔案", type=["xlsx"])

if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file, dtype={'Article': str})
        st.success("檔案上傳成功！")

        if st.button("開始處理"):
            with st.spinner('正在處理中，請稍候...'):
                recommendations = process_data(df.copy())
                
                if not recommendations:
                    st.warning("沒有生成任何調貨建議。")
                else:
                    excel_report = create_excel_report(recommendations, df.copy())
                    if excel_report:
                        st.success("處理完成！點擊下方按鈕下載報告。")
                        
                        filename = f"調貨建議_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                        st.download_button(
                            label="下載調貨建議報告",
                            data=excel_report,
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        st.warning("生成的建議為空，無法建立報告。")

    except Exception as e:
        st.error(f"處理檔案時發生錯誤: {e}")