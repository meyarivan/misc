# Read all regions from meta, run basic checks
#
# List of checks:
# 
# 1. verify that the region info is accessible
# 2. verify that there are no holes (start -> end -> start -> end ..)
# 3. verify that there are no duplicate start, end keys
# 

# TODO
# if errors > 0, print status of region (offline ?)

import os, sys
import java.lang

from org.apache.hadoop.hbase import HBaseConfiguration
from org.apache.hadoop.hbase.client import HTable
from org.apache.hadoop.hbase.client import MetaScanner
from org.apache.hadoop.hbase.util import Bytes
from org.apache.hadoop.hbase import HConstants
from org.apache.hadoop.hbase.client import Scan
from org.apache.hadoop.hbase.util import Writables

import hbaseutils

# setup table/connection
conf = HBaseConfiguration()
meta_table = HTable(conf, HConstants.META_TABLE_NAME)
scanner = meta_table.getScanner(Scan())

# utility funcs
jls = java.lang.String

prev_region = curr_region = None

regions = []
start_keys = {}
end_keys = {}

while True:

    errors = 0

    result = scanner.next()

    if not result: # end of table
        break

    rowid = Bytes.toString(result.getRow())
    rowidStr = jls(rowid)
    bytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER)

    try:
        curr_region = Writables.getHRegionInfo(bytes)
    except java.lang.NullPointerException:
        print >> sys.stderr, 'ERROR: %s error while reading region info' % \
           (jls(result.getRow()))

        errors += 1

        prev_region = None
        continue

    s_key = jls(curr_region.getStartKey())
    e_key = jls(curr_region.getEndKey())

    curr_region_name = hbaseutils.get_region_name(curr_region)
    prev_region_name = None

    if prev_region is not None:
        prev_region_name = hbaseutils.get_region_name(curr_region)

    tbl_name = curr_region.getTableDesc().getName()

    qs_key = '%s,%s' % (tbl_name, s_key)
    qe_key = '%s,%s' % (tbl_name, e_key)

    # simple check for ordering
    if s_key > e_key:
        print >> sys.stderr, 'ERROR: %s start key > end key ?!' % (curr_region_name)
        errors += 1

    # check if there are other regions with same start key
    same_key_reg = start_keys.get(qs_key, None)

    if same_key_reg is not None:
        print >> sys.stderr, 'ERROR: %s duplicate start key found %s; previous occurence in %s ' % \
            (curr_region_name, s_key, hbaseutils.get_region_name(same_key_reg)), \
            ' < '.join([str(i[1]) for i in hbaseutils.cmp_region_bounds(curr_region, same_key_reg)])

        errors += 1

    else:
        start_keys[qs_key] = curr_region

    # check if there are other regions with same end key
    same_key_reg = end_keys.get(qe_key, None)

    if same_key_reg is not None:
        print >> sys.stderr, 'ERROR: %s duplicate end key found %s; previous occurence in %s ' % \
            (curr_region_name, e_key, hbaseutils.get_region_name(same_key_reg)), \
            ' < '.join([str(i[1]) for i in hbaseutils.cmp_region_bounds(curr_region, same_key_reg)])

        errors += 1

    else:
        end_keys[qe_key] = curr_region
    

    if prev_region is None:
        pass

    elif prev_region.isOffline() and (prev_region.getStartKey() == curr_region.getStartKey()):
        print >> sys.stderr, 'WARN: %s Offlined parent ? curr region: %s' % \
            (prev_region_name, curr_region_name)

        errors += 1

    elif prev_region.getEndKey() == curr_region.getStartKey():
        pass

    else:
        print >> sys.stderr, 'ERROR: %s hole between %s %s' % (curr_region_name, prev_region_name, curr_region_name)
        
        errors += 1

    
    if errors > 0:
        print >> sys.stderr, "# ^ errors for %s", curr_region_name
        print >> sys.stderr

    prev_region = curr_region
