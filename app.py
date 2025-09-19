import streamlit as st
import pandas as pd
import time
from io import BytesIO
from utils import (
    preprocess_data, 
    estimate_transfer_potential,
    generate_recommendations, 
    create_om_transfer_chart, 
    generate_excel_export
)

# 1. 頁面配置
st.set_page_config(
    page_title="調貨建議生成系統",
    page_icon="📦",
    layout="wide"
)

# 2. 側邊欄設計
with st.sidebar:
    st.header("系統資訊")
    st.info(""" 
    **版本：v1.7** 
    **開發者:Ricky** 
    
    **核心功能：**  
    - ✅ ND/RF類型智慧識別 
    - ✅ 優先順序調貨匹配 
    - ✅ RF過剩轉出限制 
    - ✅ 統計分析和圖表 
    - ✅ Excel格式匯出 
    """)
    st.sidebar.header("操作指引")
    st.sidebar.markdown("""
    1.  **上傳 Excel 文件**：點擊瀏覽文件或拖放文件到上傳區域。
    2.  **啟動分析**：點擊「啟動分析」按鈕開始處理。
    3.  **查看結果**：在主頁面查看KPI、建議和圖表。
    4.  **下載報告**：點擊下載按鈕獲取 Excel 報告。
    """)

# 3. 頁面頭部
st.title("📦 調貨建議生成系統")
st.markdown("---")

# 4. 主要區塊
# 4.1. 資料上傳區塊
st.header("1. 資料上傳")
uploaded_file = st.file_uploader(
    "請上傳包含庫存和銷量數據的 Excel 文件",
    type=["xlsx", "xls"],
    help="必需欄位：Article, Article Description, RP Type, Site, OM, SaSa Net Stock, Pending Received, Safety Stock, Last Month Sold Qty, MTD Sold Qty"
)

if uploaded_file is not None:
    progress_bar = st.progress(0, text="準備開始處理文件...")
    try:
        # 文件上傳驗證
        progress_bar.progress(10, text="正在驗證文件格式...")
        engine = 'openpyxl' if uploaded_file.name.lower().endswith('xlsx') else 'xlrd'
        df = pd.read_excel(uploaded_file, engine=engine)
        progress_bar.progress(25, text="文件讀取成功！正在驗證內容...")

        if df.empty:
            st.error("錯誤：上傳的文件為空，請檢查文件內容。")
            st.stop()

        st.success("文件上傳與初步驗證成功！")

        # 4.2. 資料預覽區塊
        with st.expander("基本統計和資料樣本展示", expanded=False):
            st.subheader("資料基本統計")
            st.dataframe(df.describe())
            st.subheader("資料樣本（前100行）")
            st.dataframe(df.head(100))

        # 數據預處理
        progress_bar.progress(40, text="正在進行數據預處理與驗證...")
        processed_df, logs = preprocess_data(df.copy())
        progress_bar.progress(60, text="數據預處理完成！")

        # 顯示預處理日誌
        if logs:
            with st.expander("查看數據預處理日誌"):
                for log in logs:
                    if "錯誤" in log:
                        st.error(log)
                    elif "警告" in log:
                        st.warning(log)
                    else:
                        st.info(log)
        
        if processed_df is not None:
            st.session_state.cleaned_df = processed_df

            # 4.3. 分析按鈕區塊
            st.header("2. 分析與建議")

            # 預先計算潛在調貨量
            with st.spinner("正在預先計算潛在調貨量..."):
                potential = estimate_transfer_potential(st.session_state.cleaned_df.copy())
            
            st.subheader("潛在調貨量預估")
            col1, col2, col3 = st.columns(3)
            col1.metric("總需求量", f"{potential['total_needed']} 件")
            col2.metric("A模式潛在可轉出", f"{potential['potential_transfer_A']} 件")
            col3.metric("B模式潛在可轉出", f"{potential['potential_transfer_B']} 件")

            transfer_mode = st.radio(
                "請根據預估選擇轉貨力度：",
                ('A: 保守轉貨', 'B: 加強轉貨'),
                key='transfer_mode',
                help="A模式優先保障安全庫存，B模式則更積極地處理滯銷品。"
            )
            
            st.info(f"當前選擇的模式為： **{transfer_mode}**")

            if st.button("🚀 啟動分析生成調貨建議", type="primary"):
                progress_bar.progress(70, text="正在分析數據並生成建議...")
                with st.spinner("演算法運行中，請稍候..."):
                    (
                        recommendations_df, 
                        kpi_metrics, 
                        stats_by_article, 
                        stats_by_om, 
                        transfer_type_dist, 
                        receive_type_dist
                    ) = generate_recommendations(st.session_state.cleaned_df.copy(), transfer_mode)
                    time.sleep(1) # 模擬耗時操作
                progress_bar.progress(90, text="分析完成！正在準備結果展示...")

                if not recommendations_df.empty:
                    st.success("分析完成！")
                    
                    # 4.4. 結果展示區塊
                    st.header("3. 分析結果")
                    
                    # KPI 指標卡
                    st.subheader("關鍵指標 (KPIs)")
                    cols = st.columns(len(kpi_metrics))
                    for i, (k, v) in enumerate(kpi_metrics.items()):
                        cols[i].metric(k, v)
                    
                    st.markdown("---")

                    # 調貨建議表格
                    st.subheader("調貨建議清單")
                    st.dataframe(recommendations_df)

                    st.markdown("---")

                    # 統計圖表
                    st.subheader("詳細統計分析 (Detailed Statistical Analysis)")
                    
                    col1, col2 = st.columns(2)

                    with col1:
                        st.write("#### 按產品統計 (Statistics by Article)")
                        st.dataframe(stats_by_article)
                        st.write("#### 轉出類型分佈 (Transfer Type Distribution)")
                        st.dataframe(transfer_type_dist)

                    with col2:
                        st.write("#### 按OM統計 (Statistics by OM)")
                        st.dataframe(stats_by_om)
                        st.write("#### 接收類型分佈 (Receive Type Distribution)")
                        st.dataframe(receive_type_dist)
                    
                    st.markdown("---")

                    # Display the OM Transfer vs Receive Analysis Chart
                    st.subheader("OM 調貨分析圖表 (OM Transfer vs Receive Analysis Chart)")
                    om_chart_fig = create_om_transfer_chart(recommendations_df, transfer_mode)
                    st.pyplot(om_chart_fig)

                    st.success("Analysis complete! You can now download the recommendations.")

                    excel_data = generate_excel_export(
                        recommendations_df,
                        kpi_metrics,
                        stats_by_article,
                        stats_by_om,
                        transfer_type_dist,
                        receive_type_dist,
                        transfer_mode 
                    )

                    st.download_button(
                        label="📥 下載調貨建議 (Excel)",
                        data=excel_data,
                        file_name=f"調貨建議_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    progress_bar.progress(100, text="處理完畢！")
                else:
                    st.info("根據當前規則，沒有生成任何調貨建議。")
                    progress_bar.progress(100, text="處理完畢！")

    except Exception as e:
        st.error(f"處理文件時發生嚴重錯誤: {e}")
        st.exception(e) # 顯示詳細的錯誤追蹤信息
        if 'progress_bar' in locals():
            progress_bar.progress(100, text="處理失敗！")