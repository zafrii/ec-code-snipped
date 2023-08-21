import os
from database import connect_to_postgres
from elasticsearch import Elasticsearch, helpers
import tqdm
from elasticsearch.helpers import streaming_bulk

class Main:
    def __init__(self):
        self.db = connect_to_postgres()
        self.cursor = self.db.cursor()
        self.es = Elasticsearch(
            cloud_id = "",
            basic_auth=("elastic", "")
        )

    def get_counties(self):
        STATE = os.getenv('STATE',None)
        query = "select county_name from tigerline_counties where county_name = 'Broward' and statefp in (select statefp from tigerline_states where state_name = %s) order by county_name" 
        self.cursor.execute(query, (STATE,))
        rows = self.cursor.fetchall()
        for row in rows:
            print('State: ', STATE, ' County: ', row[0])
            self.startProcess(row[0], STATE)

    def generate_documents(self, index_name, documents):
        for document in documents:
            yield {
                "_index": index_name,
                "_source": document
            }
    
    def bulkIndex(self, index_name, rows):
        # try:
        #     success, _ = bulk(self.es, self.generate_documents(index_name, rows))
        # except BulkIndexError as e:
        #     for item in e.errors:
        #         print("Error:", item['index'], item['error'])
        # if success:
        #     print("Bulk indexing completed successfully.")
        # else:
        #     print("Bulk indexing failed.")

        # try:
        #     for success in helpers.bulk(self.es, self.generate_documents(index_name, rows)):
        #         if success:
        #             print('Bulk indexing completed successfully.')
        #         else:
        #             print("Bulk indexing failed.")
        # except helpers.BulkIndexError  as e:
        #     print(f"{len(e.errors)} document(s) failed to index.")
        #     for error_info in e.errors:
        #         index_name = error_info['index']
        #         if 'error' in error_info and 'reason' in error_info['error']:
        #             error_reason = error_info['error']['reason']
        #             print(f"Document in index '{index_name}' failed to index. Reason: {error_reason}")
        #         else:
        #             print(f"Document in index '{index_name}' failed to index. Reason: Unknown")

        progress = tqdm.tqdm(unit="docs", total=len(rows))
        successes = 0
        for ok, action in streaming_bulk(
            client=self.es, index=index_name, actions=rows,
        ):
            progress.update(1)
            successes += ok
        print("Indexed %d/%d documents" % (successes, len(rows)))

    def startProcess(self, county, state):
        parcel_table = 'parcel_%s_%s' % (str(state).lower().replace(' ','_'), str(county).lower().replace(' ','_').replace('-','_').replace('.', ''))
        index_exist = self.es.indices.exists(index=parcel_table) #parcel_elk_california_alameda
        if index_exist:
            print("Index already exists: ",parcel_table)
        else:
            print('creating table', parcel_table) 
            query = f"create table if not exists {parcel_table} as SELECT pg.id, parcel_id, Round(pg.area, 2) AS area, pg.address AS address,z.city_id, z.zone_code, max_building_height_ft, ROUND(ST_X(ST_Transform(pg.geom_pnt, 4326)):: NUMERIC,6) lng, ROUND(ST_Y(ST_Transform(pg.geom_pnt, 4326)):: NUMERIC,6) lat,ST_Transform(pg.geom, 4326) as geom,pg.state,pg.county,z.zone_type, z.plu_jsonb->>'class' as class,z.plu_jsonb->>'subclass_full_form' as subclass_full_form, pg.geom_pnt, pg.owner_info->>'owner_name' as owner_name, pg.owner_info->>'owner_address' as owner_address, pg.land_use, pg.city_name FROM p_usa pg LEFT JOIN geometries g ON ST_Intersects(pg.geom_pnt, g.geom) left JOIN zones z ON z.id = g.zone_id where st_isvalid(pg.geom) = true AND pg.state = '{state}' AND pg.county = '{county}'"
            self.cursor.execute(query, (parcel_table, state, county))

            
            limit = 1000
            offset = 50000
            total_count = 0
            query = "select count(*) from %s" % (parcel_table)
            
            self.cursor.execute(query)
            result = self.cursor.fetchone() 
            count = result[0]
            
            if count != 0:
                index_name = parcel_table
                mapping = {
                    'properties': {
                        'geometry': {
                            'type': 'geo_shape'
                        },
                        'geom_pnt': {
                            'type': 'geo_point'
                        },
                        "class": {
                            "type": "text"
                        },
                        "subclass_full_form": {
                            "type": "text"
                        }
                    }
                }
                self.es.indices.create(index=index_name,mappings=mapping)
                print('ELK Index Created: %s' % (parcel_table))
                print(count)
                # sys.exit()
                while total_count <= count:
                    # print('in if statement')
                    # print('Limit is : ',limit)
                    print('offset is: ',offset)
                    query = "select id,ST_AsText(geom),parcel_id,area,address,zone_code,max_building_height_ft,lng,lat,state,county,city_id,zone_type, class, subclass_full_form, ST_Transform(geom_pnt, 4326) as geom_pnt, owner_name, owner_address, land_use, city_name from %s where st_isvalid(geom) order by id asc limit %s offset %s" % (parcel_table,limit,offset)
                    self.cursor.execute(query)
                    rows = self.cursor.fetchall()
                    # print("keke")
                    print('Data from pstgres received')
                    
                    body = []
                    
                    for row in rows:
                        document = {
                            'id' : row[0],
                            'geometry': row[1] ,
                            'parcel_id': row[2],
                            'area': row[3],
                            'address' : row[4],
                            'zone_code': row[5],
                            'max_building_height_ft': row[6],
                            'lng': row[7],
                            'lat': row[8],
                            'state': row[9],
                            'county': row[10],
                            'city_id': row[11],
                            'zone_type': row[12],
                            'class': row[13],
                            'subclass_full_form': row[14],
                            'geom_pnt': [row[7],row[8]],
                            'owner_name': row[16],
                            'owner_address': row[17],
                            'land_use': row[18],
                            'city_name': row[19]
                        }
                        body.append(document)
                        # print(len(body))
                    # print('documents added are: ',len(body))
                    self.bulkIndex(index_name=index_name, rows=body)
                    total_count += limit
                    offset += limit
                print('total count added is: ',total_count)
                
                # for key, value in body.items():
                #     print(key)
                #     print(value)
                # print(body)
                # batch_size=conf.batch_size
                # es.batchIndexing(body,index_name,batch_size)
            # self.es.close()
            # sys.exit()
            query = f"drop table if exists {parcel_table}"
            self.cursor.execute(query)
            print('Temp Table Dropped : %s' % (parcel_table))

main = Main()
main.get_counties()
main.cursor.close()
