# coding: utf-8
import finlib
'''
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np

import pandas
import math
import re
from scipy import stats
import finlib
import datetime
import traceback
import sys
import tushare.util.conns as ts_cs

#import matplotlib.pyplot as plt
#import matplotlib.dates as mdates

from sklearn.cluster import KMeans
from calendar import monthrange

import stockstats
import tabulate
import os
import tabulate
'''

import matplotlib.pyplot as plt  # For ploting
import numpy as np  # to work with numerical data efficiently


def draw_sin(t, cycle, start, start_year, k=0, a=1):
    #t: how many years
    #a = 1  #Amplitude, 振幅，
    #k = 0  #偏距，反映在坐标系上则为图像的上移或下移
    #start_year: x_axis start, e.g 1700
    #start: start of the 1st cycle on x_axis
    #cycle: diam of a cycle, y from 0 -> Highest -> 0, a half round
    #
    '''

    #cycle_in_year = 10
    #mi = np.log2(cycle_in_year)
    #omega = np.pi/(2**(mi - 1))
    #y10 = A * np.sin(omega * t  + phi) + k


    y=A * sin(ωx+φ)+k
    A——振幅，当物体作轨迹符合正弦曲线的直线往复运动时，其值为行程的1/2。  <<<<< Y Axis
    (ωx+φ)——相位，反映变量y所处的状态。
    φ——初相，x=0时的相位；反映在坐标系上则为图像的左右移动。<<< maybe.
    k——偏距，反映在坐标系上则为图像的上移或下移。
    ω——角速度， 控制正弦周期(单位弧度内震动的次数)。<<<<< X Axis, 角速度ω变大，则波形在X轴上收缩（波形变紧密）；角速度ω变小，则波形在X轴上延展（波形变稀疏）
    角频率，圆频率,angular frequency；circular frequency,  单位 弧度/秒(rad/s) ,旋转矢量单位时间内转过的弧度,  ω = 2πf = 2π/T,
    角频率数值上等于谐振动系统中旋转矢量的转动的角速度。频率（f）、角频率(ω)和周期(T)的关系为ω = 2πf = 2π/T。

    sin(2x)的速度快一倍
    sin(x/2)的速度慢一倍


    y = A * np.sin(2*np.pi * t  + phi) + k  # 1 cycle = 1 Unit.
    y = A * np.sin(1*np.pi * t  + phi) + k  # 1 cycle = 2 Unit.
    y = A * np.sin(np.pi/2 * t  + phi) + k  # 1 cycle = 4 Unit.
    y = A * np.sin(np.pi/4 * t  + phi) + k  # 1 cycle = 8 Unit.
    y = A * np.sin(np.pi/8 * t  + phi) + k  # 1 cycle = 16 Unit.

    '''
    t = np.arange(0, t, 0.01)
    a = np.log(cycle)
    rtn = abs(a * np.sin(np.pi / (2**(np.log2(cycle * 2) - 1)) * t - np.pi * ((start - start_year) % cycle) / cycle)) + k
    return (rtn)


def main():

    t = 350  # t: how many years
    start_year = 1700

    a = 1  #Amplitude
    k = 0

    #基钦周期由英国人提出，3 - 4年一轮回
    cycle = 4.5  #reventory
    start = 2002  #[2002,2006 ]
    y3 = draw_sin(t, cycle, start, start_year, k, a)

    #朱格拉周期由法国人提出，10年一轮回. 设备投资占GDP的比例
    cycle = 9  #???
    start = 1995
    y10 = draw_sin(t, cycle, start, start_year, k, a)

    #库兹涅茨周期由美国人提出，18 - 20 年一轮回
    cycle = 18  #Realestate
    start = 1998  #[1998,2018], cycle: 1998-2018
    y20 = draw_sin(t, cycle, start, start_year, k, a)

    #康德拉季耶夫周期由苏俄人提出，50 - 60年一轮回
    #第一个长周期从18世纪80年代到1842年，是“产业革命时期”；
    # 第二个长周期从1842年到1897年，是“蒸汽和钢铁时期”；
    # 第三个长周期从1897年今后，是“电气、化学和轿车时期”

    cycle = 55  #New Technology
    start = 1771  #[1710, ]
    start = 1842  #[1710, ]
    y60 = draw_sin(t, cycle, start, start_year, k, a)

    #y = y3 + y10 + y20 + y60
    y = y3 + y10 + y20 + y60

    x = np.arange(0, t, 0.01) + start_year

    ymax = max(y)
    xpos = ymax.__index__()
    xmax = x[xpos]

    plt.plot(x, y3)
    plt.plot(x, y10)
    plt.plot(x, y20)
    plt.plot(x, y60)
    plt.plot(x, y)

    plt.annotate(
        'local max',
        xy=(xmax, ymax),
        xytext=(xmax, ymax + 5),
        arrowprops=dict(facecolor='black', shrink=0.05),
    )

    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.title("SINE WAVE")
    plt.grid(True, which='both')
    plt.axhline(y=0, color='k')

    print("plotted")
    plt.show()
    pass


### MAIN ####
if __name__ == '__main__':
    main()
