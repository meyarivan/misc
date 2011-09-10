
import os, sys
import java.lang


jls = java.lang.String

def cmp_region_bounds(*args):
    v = []
    for i in range(len(args)):
        r = args[i]
        v.extend([(r.getStartKey(), (i,'s')), (r.getEndKey(), (i,'e'))])
        
    v.sort()
    return v

def get_region_name(hri):
    return jls(hri.getRegionName())


