# ======================================================================= #
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        #
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     #
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. #
#  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      #
#  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  #
#  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  #
#  OTHER DEALINGS IN THE SOFTWARE.                                        #
#                                                                         #
#  For more information, please refer to <http://unlicense.org>           #
# ======================================================================= #
import json
import os

from dragonfly import dragonfly


class RESTClient(dragonfly.DataReader):
    def __init__(self):
        self.data = [{
            "id": 1,
            "customerId": 1234,
            "total": 120.0,
            "status": "payment_confirmation",
            "origin": "online",
            "seller": {
                "id": 321,
                "name": "Seller Co.",
                "address": "123 Some Street",
                "crn": "12345678-12"
            }
        },
                     {
                         "id": 2,
                         "customerId": 4321,
                         "total": 1000.0,
                         "status": "payment_confirmed",
                         "origin": "retail",
                         "seller": {
                             "id": 322,
                             "name": "Retailer Co.",
                             "address": "246 Retail Street",
                             "crn": "87654321-09"
                         }
                     }]

    def fetch(self, entity_name, metadata):
        yield self.data


class FileAdapter(dragonfly.PersistenceAdapter):
    def upsert(self, collection, record, metadata):
        output_file = f"{collection}.txt"

        if os.path.isfile(output_file):
            with open(output_file) as stream:
                existing_records: dict = json.loads(stream.read() or "{}")
            existing_records[record['id']] = record
        else:
            existing_records = {record['id']: record}

        with open(output_file, 'w+') as stream:
            json.dump(existing_records, stream)

    def remove(self, collection, record, metadata=None):
        output_file = f"{collection}.txt"
        with open(output_file) as stream:
            existing_records: dict = json.loads(stream.read() or "{}")
        if record['id'] in existing_records:
            del existing_records['id']
        with open(output_file, 'w') as stream:  # 582,23
            json.dump(existing_records, stream)

    def close(self):
        pass
