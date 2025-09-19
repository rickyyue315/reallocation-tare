import pandas as pd
import os
from utils import preprocess_data, generate_recommendations, create_om_transfer_chart

def create_test_data_v1_6():
    """生成v1.6版本的模擬Excel測試數據"""
    data = {
        'Article': ['A001', 'A001', 'A001', 'A002', 'A002', 'A003', 'A003', 'A004', 'A004', 'A005', 'A005', 'A006'],
        'Article Description': ['Product A', 'Product A', 'Product A', 'Product B', 'Product B', 'Product C', 'Product C', 'Product D', 'Product D', 'Product E', 'Product E', 'Product F'],
        'RP Type': ['ND', 'RF', 'RF', 'RF', 'RF', 'ND', 'RF', 'RF', 'RF', 'RF', 'RF', 'ND'],
        'Site': ['S01', 'S02', 'S03', 'S01', 'S04', 'S05', 'S06', 'S02', 'S07', 'S03', 'S08', 'S09'],
        'OM': ['OM1', 'OM1', 'OM1', 'OM1', 'OM2', 'OM2', 'OM2', 'OM1', 'OM3', 'OM1', 'OM3', 'OM3'],
        'MOQ': [10, 10, 10, 5, 5, 20, 20, 8, 8, 12, 12, 15],
        'SaSa Net Stock': [50, 100, 5, 0, 80, 30, 10, 15, 5, 20, 0, 25],
        'Pending Received': [0, 20, 0, 10, 10, 0, 5, 5, 0, 0, 10, 0],
        'Safety Stock': [0, 30, 10, 15, 25, 0, 12, 10, 8, 15, 5, 0],
        'Last Month Sold Qty': [0, 15, 2, 5, 8, 0, 3, 20, 1, 18, 0, 0],
        'MTD Sold Qty': [0, 5, 1, 2, 4, 0, 1, 8, 0, 6, 3, 0]
    }
    df = pd.DataFrame(data)
    
    # 邊界情況
    # 庫存為負
    df.loc[len(df)] = ['A007', 'Product G', 'RF', 'S10', 'OM1', 10, -5, 0, 10, 5, 2, 'Negative Stock']
    # 銷量異常
    df.loc[len(df)] = ['A008', 'Product H', 'RF', 'S11', 'OM2', 10, 100, 0, 10, 120000, 2, 'High Sales']
    # 空值
    df.loc[len(df)] = ['A009', 'Product I', 'RF', 'S12', 'OM3', 10, 50, 0, 10, 5, None, 'Null MTD Sales']


    # 確保目錄存在
    if not os.path.exists('test_data'):
        os.makedirs('test_data')
        
    file_path = os.path.join('test_data', 'test_data_v1.6.xlsx')
    df.to_excel(file_path, index=False)
    print(f"測試數據已生成於: {file_path}")
    return file_path

def run_test_v1_6(file_path, mode):
    """使用v1.6的邏輯運行測試並驗證圖表生成"""
    print(f"\n--- 正在以模式 '{mode}' 運行測試 ---")
    df = pd.read_excel(file_path)
    
    # 1. 數據預處理
    processed_df, logs = preprocess_data(df.copy())
    print("數據預處理日誌:")
    for log in logs:
        print(log)
    
    if processed_df is None:
        print("數據預處理失敗，測試終止。")
        return

    # 2. 生成推薦
    recommendations_df, _, _, _, _, _ = generate_recommendations(processed_df.copy(), mode)
    
    if recommendations_df.empty:
        print("沒有生成任何調貨建議。")
    else:
        print("生成的調貨建議:")
        print(recommendations_df)

        # 3. 驗證圖表生成
        try:
            fig = create_om_transfer_chart(recommendations_df, mode)
            chart_path = f'test_chart_v1.6_{mode.replace(":", "_")}.png'
            fig.savefig(chart_path)
            print(f"圖表已成功生成並保存為: {chart_path}")
        except Exception as e:
            print(f"圖表生成失敗: {e}")

if __name__ == "__main__":
    # 生成測試數據
    test_file = create_test_data_v1_6()
    
    # 運行模式A測試
    run_test_v1_6(test_file, 'A: 保守轉貨')
    
    # 運行模式B測試
    run_test_v1_6(test_file, 'B: 加強轉貨')