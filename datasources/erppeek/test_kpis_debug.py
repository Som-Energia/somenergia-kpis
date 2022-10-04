from erppeek import Client
import unittest
import dbconfig


class QueryERPTest(unittest.TestCase):

    def setUp(self):
        self.erppeek = dbconfig.erppeek
        self.erp_client = Client(**self.erppeek)

    # TODO use the csv from dbt seeds
    def test__query_erppeek_switching_model(self):
        model_name = 'giscedata.switching'
        obj = self.erp_client.model(model_name)
        model_filter = [("step_id.name","ilike","06"),("proces_id.name","ilike","C%"), ("date",">=","2022-09-29"), ("date", "<=", "2022-09-29")]
        cxt = {}
        # model_filter = "[(""state"", ""="", ""erroni"")]"
        # cxt = "{""type"":""out_invoice""}"
        entries_ids = obj.search(model_filter, context = cxt)
        obj.read(entries_ids)

    def test__query_erppeek_atc_model(self):
        model_name = 'giscedata.atc'
        obj = self.erp_client.model(model_name)
        model_filter = [("section_id","ilike","%Reclama"),("state","=","open")]
        cxt = {}
        # model_filter = "[(""state"", ""="", ""erroni"")]"
        # cxt = "{""type"":""out_invoice""}"
        entries_ids = obj.search(model_filter, context = cxt)
        obj.read(entries_ids)

    def test__query_erppeek_atc_model__cacs_distribuidor(self):
        model_name = 'giscedata.atc'
        obj = self.erp_client.model(model_name)
        model_filter = [("state","=","pending"),("agent_actual","=","10")]
        cxt = {}
        import ipdb; ipdb.set_trace()
        # model_filter = "[(""state"", ""="", ""erroni"")]"
        # cxt = "{""type"":""out_invoice""}"
        entries_ids = obj.search(model_filter, context = cxt)
        print(len(entries_ids))