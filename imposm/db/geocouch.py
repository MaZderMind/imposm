# Copyright 2011 Omniscale (http://omniscale.com)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

import logging

try:
    import simplejson as json
except ImportError:
    import json
    
import httplib2
log = logging.getLogger(__name__)

from imposm.mapping import UnionView, GeneralizedTable, Mapping

class GeoCouchDB(object):
    def __init__(self, db_conf):
        self.db_conf = db_conf
        self.db_url = 'http://%(host)s:%(port)s/%(db)s/_bulk_docs' % db_conf
        self._insert_stmts = {}
        self.conn = httplib2.Http()

    def commit(self):
        pass
    
    def reconnect(self):
        pass
    
    def insert(self, mapping, insert_data, tries=0):
        extra_field_names = mapping.extra_field_names()
        features = []
        for data in insert_data:
            feature = {
                'osm_id': data[0],
                'name': data[1],
                'type': data[2],
                'class': mapping.name,
                'geometry': data[3],
            }
            for extra, value in zip(extra_field_names, data[4:]):
                feature[extra] = value
            
            features.append(feature)
            
        body = json.dumps({'docs': features})
        resp, content = self.conn.request(self.db_url, 'POST', body=body,
            headers={'content-type': 'application/json'})
        # TODO check for resp
    
    def geom_wrapper(self, geom):
        return geom.__geo_interface__
    
    def create_tables(self, mappings):
        for mapping in mappings:
            self.create_table(mapping)

    def create_table(self, mapping):
        print 'create_table'
    
    def swap_tables(self, new_prefix, existing_prefix, backup_prefix):
        print 'swap_tables', new_prefix, existing_prefix, backup_prefix
        
    def remove_tables(self, prefix):
        print 'remove_tables', prefix

    def remove_views(self, prefix):
        print 'remove_views', prefix
    
    def create_views(self, mappings, ignore_errors=False):
        print 'create_views'
    
    def create_generalized_tables(self, mappings):
        print 'create_generalized_tables'

    def optimize(self, mappings):
        print 'optimize'
