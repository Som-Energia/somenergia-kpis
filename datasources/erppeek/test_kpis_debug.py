from erppeek import Client
import unittest
import dbconfig


class QueryERPTest(unittest.TestCase):

    def setUp(self):
        self.erppeek = dbconfig.erppeek
        self.erp_client = Client(**self.erppeek)

    # TODO use the csv from dbt seeds
    def test__query_erppeek_model(self):
        model_name = 'giscedata.switching'
        obj = self.erp_client.model(model_name)
        model_filter = [("step_id.name","ilike","06"),("proces_id.name","ilike","C%"), ("date",">=","2022-09-29"), ("date", "<=", "2022-09-29")]
        cxt = {}
        # model_filter = "[(""state"", ""="", ""erroni"")]"
        # cxt = "{""type"":""out_invoice""}"
        entries_ids = obj.search(model_filter, context = cxt)
        obj.read(entries_ids)

