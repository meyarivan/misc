
import os, sys

from region import PHBaseRegion, PHBaseException
from optparse import OptionParser

import hbaseutils

from org.apache.hadoop.hbase import HBaseConfiguration
from org.apache.hadoop.hbase.util import Bytes
from org.apache.hadoop.hbase.client import HTable
from org.apache.hadoop.hbase.client import Scan
from org.apache.hadoop.hbase.filter import KeyOnlyFilter
from org.apache.hadoop.hbase.util import Writables

import java.lang

jls = java.lang.String

class RegionTool(object):

    tools = {
        'list' : {}, 
        'info' : {},
        }

    def __init__(self, conf, *args):
        self.conf = conf

    def tool_list(self, *args):
        """ List all regions """

        pass

    def tool_info(self, regionx, *args):
        """ Print info about a region """

        try:
            phb = PHBaseRegion(self.conf, regionx)
        except PHBaseException, e:
            print >> sys.stderr, "ERROR", str(e)
            sys.exit(1)

        print 'region name:\t%s Offline: %s' % (phb.name, phb.offline)
        print 'start key:\t%s' % (phb.start_key)
        print 'end key:\t%s' % (phb.end_key)

        # costly ?
        s = Scan(Bytes.toBytes(phb.start_key), Bytes.toBytes(phb.end_key))
        s.setCacheBlocks(False)
        s.setFilter(KeyOnlyFilter(True))
        s.setCaching(500)

        scanner = phb.table_name.getScanner(s)
        n = 0

        cont = True

        while cont:
            x = scanner.next(500)

            if not x:
                break

            # Sample the results instead of checking every row
            # add cmdline args to specify % sample
            for r in x:
                r_hri = phb.table_name.getRegionLocation(r.getRow()).getRegionInfo()
                
                if r_hri != phb.obj:
                    print >> sys.stderr, "row belongs to different region !", jls(r_hri.getRegionName())
                    print >> sys.stderr, 'Order of keys : %s' % ('<'.join([str(i[1]) for i in hbaseutils.cmp_region_bounds(\
                                    phb.obj, r_hri)]))
                    cont = False
                    break

            if cont:
                n += len(x)
        
        print 'no of rows:\t%d' % (n)

        
def parse_args():
    
    usage = """%s : simple tool to explore HBase regions

Available tools: 

    list
    info

""" % (sys.argv[0])

    if (len(sys.argv) < 2) or (sys.argv[1] not in RegionTool.tools.keys()):
        print >> sys.stderr, usage
        sys.exit(2)

    tool = sys.argv[1]

    if tool == 'info':
        if len(sys.argv) < 3:
            print >> sys.stderr, '%s requires region name/id as the only argument' % (tool)
            sys.exit(2)

        return tool, (sys.argv[2],), {}


if __name__ == '__main__':
    conf = HBaseConfiguration.create()
    tool, args, kwargs = parse_args()
    rt = RegionTool(conf)
    handler = getattr(rt, 'tool_%s' % (tool))
    
    apply(handler, args, kwargs)



