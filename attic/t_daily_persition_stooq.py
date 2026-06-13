#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import datetime as dt
import os
import argparse
import glob

def analyze_price_position(csv_path):
    """
    Analyze price position relative to 6-month high and low
    """
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort by date in descending order to have the latest data first
    df = df.sort_values('date', ascending=False)
    
    # Get the latest price
    latest_price = df.iloc[0]['close']
    latest_date = df.iloc[0]['date'].strftime('%Y-%m-%d')
    
    # Calculate 6 months ago from the latest date
    latest_date_dt = df.iloc[0]['date']
    six_months_ago = latest_date_dt - dt.timedelta(days=180)
    
    # Filter data for the last 6 months
    df_6m = df[(df['date'] >= six_months_ago) & (df['date'] <= latest_date_dt)]
    
    # Find 6-month maximum and minimum prices
    six_month_max = df_6m['close'].max()
    six_month_min = df_6m['close'].min()
    
    # Calculate dates when max and min occurred
    max_date = df_6m.loc[df_6m['close'] == six_month_max, 'date'].iloc[0].strftime('%Y-%m-%d')
    min_date = df_6m.loc[df_6m['close'] == six_month_min, 'date'].iloc[0].strftime('%Y-%m-%d')
    
    # Calculate percentage drop from maximum
    pct_drop_from_max = ((latest_price - six_month_max) / six_month_max) * 100
    
    # Calculate percentage rise from minimum
    pct_rise_from_min = ((latest_price - six_month_min) / six_month_min) * 100
    
    # Print results
    print(f"分析结果 - {os.path.basename(csv_path)}:")
    print(f"当前日期: {latest_date}")
    print(f"当前收盘价 (P): {latest_price:.2f}")
    print(f"6个月最高价 (Pmax): {six_month_max:.2f} ({max_date})")
    print(f"6个月最低价 (Pmin): {six_month_min:.2f} ({min_date})")
    print(f"当前价格相对最高价: {pct_drop_from_max:.2f}% (从高点跌了 {abs(pct_drop_from_max):.2f}%)")
    print(f"当前价格相对最低价: {pct_rise_from_min:.2f}% (从低点涨了 {abs(pct_rise_from_min):.2f}%)")
    print("-" * 50)
    
    # Return the analysis results as a dictionary
    return {
        'filename': os.path.basename(csv_path),
        'latest_date': latest_date,
        'latest_price': latest_price,
        'six_month_max': six_month_max,
        'six_month_max_date': max_date,
        'six_month_min': six_month_min, 
        'six_month_min_date': min_date,
        'pct_drop_from_max': pct_drop_from_max,
        'pct_rise_from_min': pct_rise_from_min
    }

def analyze_all_indices(directory):
    """
    Analyze all CSV files in the given directory
    """
    print(f"分析目录中的所有指数: {directory}")
    print("=" * 60)
    
    # Get all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    
    if not csv_files:
        print(f"错误: 在目录 '{directory}' 中没有找到CSV文件。")
        return
    
    # Sort files by name
    csv_files.sort()
    
    # Collect all results
    results = []
    
    # Analyze each file
    for csv_file in csv_files:
        try:
            result = analyze_price_position(csv_file)
            results.append(result)
        except Exception as e:
            print(f"处理文件 {os.path.basename(csv_file)} 时出错: {str(e)}")
    
    # Print summary table
    print("\n总结: 所有指数分析")
    print("=" * 100)
    print(f"{'指数名称':<15} {'当前价格':<10} {'6个月最高':<10} {'跌幅%':<8} {'6个月最低':<10} {'涨幅%':<8}")
    print("-" * 100)
    
    for r in results:
        print(f"{r['filename']:<15} {r['latest_price']:<10.2f} {r['six_month_max']:<10.2f} {abs(r['pct_drop_from_max']):<8.2f} {r['six_month_min']:<10.2f} {abs(r['pct_rise_from_min']):<8.2f}")
    
    print("=" * 100)

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze stock price position relative to 6-month high and low')
    parser.add_argument('path', nargs='?', default='/home/ryan/DATA/DAY_Global/stooq/US_INDEX/NASDAQ100.csv',
                        help='Path to a CSV file or directory containing CSV files')
    parser.add_argument('--all', '-a', action='store_true', 
                        help='Analyze all CSV files in the directory')
    args = parser.parse_args()
    
    # Determine if the path is a file or directory
    if os.path.isdir(args.path) or args.all:
        # If path is a directory or --all flag is used, analyze all files
        dir_path = args.path if os.path.isdir(args.path) else os.path.dirname(args.path)
        analyze_all_indices(dir_path)
    elif os.path.isfile(args.path):
        # If path is a file, analyze just that file
        analyze_price_position(args.path)
    else:
        print(f"错误: 路径 '{args.path}' 不存在。")
        exit(1)
