from enum import Enum


class QueIntervalModel(str, Enum):
    n25 = 'd1'
    n50 = 'd2'
    n75 = 'd3'
    n100 = 'd4' 
    n125 = 'd5'
    n150 = 'd6'
    n200 = 'w1'
    n400 = 'w2'
    n600 = 'w3'
    n4 = 'h4'
    n8 = 'h8'
    n12 = 'h12'
    n16 = 'h16'
    n20 = 'h20'
