#!/usr/bin/env python3

from somutils.dbutils import runsql_cached, runsql, tsvread
from somutils.pathlib import Path
from consolemsg import step, warn, success, error
from yamlns import namespace as ns

def splitStreetType(address):
    import re
    locator = re.compile('^\s*('+('\\b|'.join((x[0] for x in streetTypes)))+r'\b)[-,\\/.\s]+(\b.+)')
    match = locator.match(address)
    if match:
        return match.groups()
    return (None, address)

streetTypes = [
    ('CALLE', 'CL'),
    ('CARRER', 'CL'),
    ('KALEA', 'CL'),
    ('PLAZA', 'PZ'),
    ('PLACA', 'PZ'),
    ('AVENIDA', 'AV'),
    ('AVINGUDA', 'AV'),
    ('PASEO', 'PS'),
    ('PASSEIG', 'PS'),
    ('PASSATGE', 'PS'),
    ('PASATGE', 'PS'),
    ('TRAVESSERA', 'XX'),
    ('TRAVESERA', 'XX'),
    ('TRAVESIA', 'XX'),
    ('CAMINO', 'CM'),
    ('CAMI', 'CM'),
    ('CARRETERA', 'CR'),
    ('RONDA', 'RD'),
    ('RAMBLA', 'XX'),
    ('RIERA', 'XX'),
    ('PARTIDA', 'PD'),
    ('PTDA', 'PD'),
    ('PLZA', 'PZ'),
    ('PZA', 'PZ'),
    ('PL', 'PZ'),
    ('AV', 'AV'),
    ('AVD', 'AV'),
    ('AVDA', 'AV'),
    ('AVGDA', 'AV'),
    ('RDA', 'RD'),
    ('TRV', 'XX'),
    ('TRAV', 'XX'),
    ('PS', 'XX'),
    ('PG', 'XX'),
    ('PTGE', 'XX'),
    ('CRTA', 'CR'),
    ('CNO', 'CM'),
    ('CR', 'CR'),
    ('CL', 'CL'),
    ('C', 'CL'),
]
stdtypes = dict(streetTypes)

notype = []


transliterations = str.maketrans(
    'àâáä' 'èêéë' 'ìîíï' 'òôóö' 'ùûúü' 'ñ' 'ç' ',.-',
    'aaaa' 'eeee' 'iiii' 'oooo' 'uuuu' 'n' 'c' '   ',
) 
commonwords = {
    "de", "la", "el", "l'", "d'", "del","d'en",'i','y',"po",
    "los", "les", "las"
}

def cleanStreet(street):
    "Returns extracts comparable and "
    cleaned = street.upper()
    cleaned = cleaned.translate(transliterations)
    streettype, cleaned = splitStreetType(cleaned)
    stdtype = stdtypes[streettype] if streettype else '??'
    def whilenotnumber(x):
        for item in x:
            if item.isdigit(): break
            yield item

    cleaned = ' '.join(
        x   
        for x in whilenotnumber(cleaned.split())
        if x.lower() not in commonwords
    )
    #print(street, '->', cleaned)
    return cleaned

import ine

class StreetDb:
    def __init__(self, path=None):
        self._data={}

    def loadCity(self, citycode):

        if citycode not in self._data:
            step(f"Loading street sections for city: {citycode}")
            #self._data[citycode] = ine.loadCityStreets(citycode)
            self._data[citycode] = ine.loadCityStreetSections(citycode)

        return self._data[citycode]

    def guessStreet(self, citycode, via):
        cityStreets = self.loadCity(citycode)
        namesbycode = {
            street.code: cleanStreet(street.name)
            for street in cityStreets.values()
        }

        from thefuzz import process, fuzz
        fixedvia = cleanStreet(via)
        print(via, '->', fixedvia)
        for result in process.extract(fixedvia, namesbycode, scorer=fuzz.token_set_ratio):
            print(result)



streetdb = StreetDb()

unsolved = ns.load('unsolved.yaml').unsolved

for address in runsql_cached('contractaddresses.sql', unsolved=list(sorted(unsolved))):
    # TODO: This is to execute just for Barcelona, remove!
    if address.citycode[:2] not in ('08',):
        continue
    print(address.citycode, '-', address.streetname)
    streetdb.guessStreet(address.citycode, address.streetname)


    





