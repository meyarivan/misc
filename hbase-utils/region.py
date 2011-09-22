
import os, sys

import java.lang

from org.apache.hadoop.hbase import HBaseConfiguration
from org.apache.hadoop.hbase.client import HTable
from org.apache.hadoop.hbase.client import MetaScanner
from org.apache.hadoop.hbase.util import Bytes
from org.apache.hadoop.hbase import HConstants
from org.apache.hadoop.hbase.client import Scan
from org.apache.hadoop.hbase.util import Writables
from org.apache.hadoop.hbase.client import Get

jls = java.lang.String

class PHBaseException(Exception):
    pass

class PHBaseRegion(object):
    """ Represent an underlying HBaseRegion """

    def __init__(self, conf, id_region):
        self.conf = conf
        self.cache = {}
        self.id_region = id_region

        self.start_key = self.end_key = None
        self.offline = False
        self.table_name = None

        self.sync_region_info()

    def sync_region_info(self, id_region = None):

        if id_region is None:
            id_region = self.id_region

        meta_table = HTable(self.conf, HConstants.META_TABLE_NAME)
        gobj = Get(Bytes.toBytes(id_region))
        result = meta_table.get(gobj)
        bytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER)
        hri = None

        try:
            hri = Writables.getHRegionInfo(bytes)
        except java.lang.NullPointerException:
            raise PHBaseException('could to retrieve region info for %s' % (id_region))
        
        self.start_key = jls(hri.getStartKey())
        self.end_key = jls(hri.getEndKey())
        self.name = jls(hri.getRegionName())
        self.table_name = HTable(hri.getTableDesc().getName())
        self.offline = hri.isOffline()

        self.obj = hri

if __name__ == '__main__':
    conf = HBaseConfiguration()
    try:
        phb = PHBaseRegion(conf, sys.argv[1])
        print phb.start_key
    except PHBaseException, e:
        print >> sys.stderr, "ERROR", str(e)
        sys.exit(1)
