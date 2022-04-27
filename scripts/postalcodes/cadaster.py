#!/usr/bin/env python3
import collections
from yamlns import namespace as ns
from consolemsg import step, warn, error
import requests
import xmltodict

# Uses https://www.catastro.meh.es/ws/Webservices_Libres.pdf

def od2ns(data):
    """Turns structures provided by xmltodict into namespaces
    that can be accessed either as attributes or subscripts
    and dumped as yaml.
    """
    if type(data) == list:
        return [od2ns(x) for x in data]
    if type(data) != collections.OrderedDict:
        return data
    return ns((k,od2ns(v)) for k,v in data.items())

def listify(l):
    """If it is not a list already, turn it into a single element list.

    Context: xmltodict parses a repeated tag as a list, but the same
    attribute will be a single element when it appears once.
    If you use the attribute as list you may get errors like
    "string indices must be integers" when iterating on a singleton element.
    If you use the attribute as singleton element you will get
    errors like "'list' object has no attribute 'X'".
    Wrap it under listify and use that attribute always as iterable.
    """
    if type(l) == list:
        return l
    return [l]

class Cadaster(object):

    def __init__(self):
        self._states = None
        self._cities = {}
        self._streets = {}

    def states(self):
        if not self._states:
            self.retrieveStates()
        return self._states

    def cities(self, stateCode):
        if stateCode not in self._cities:
            self.retrieveCities(stateCode)
        return self._cities[stateCode]

    _base_url = "http://ovc.catastro.meh.es/ovcservweb/OVCSWLocalizacionRC/OVCCallejero.asmx/"
    def _request(self, method, **kwds):
        #warn(f"Sending {method}: {kwds}")
        response = requests.get(self._base_url+method, params=kwds)
        try:
            d = xmltodict.parse(response.content, process_namespaces=False, xml_attribs=False)
        except:
            error(response.content)
            raise Exception(response.content)
        d = od2ns(d)
        #warn(f"Receiving:\n{d.dump()}")
        return d

    def _error(self, response):
        root = response[list(d.keys())[0]]
        if 'lerr' in root:
            return root.lerr.err.des

    def retrieveStates(self):
        result = self._request('ConsultaProvincia')
        self._states = {
            '{:0>2}'.format(p['cpine']): p['np']
            for p in listify(result.consulta_provinciero.provinciero.prov)
        }

    def retrieveCities(self, stateCode, citySearch=None):
        state = self.states()[stateCode]
        result = self._request('ConsultaMunicipio',
            Provincia=state,
            Municipio=citySearch or '',
        )
        self._cities[stateCode] = {
            '{:0>3}'.format(m['loine']['cm']):  m['nm']
            for m in listify(result.consulta_municipiero.municipiero.muni)
        }

    def retrieveStreets(self, stateCode, cityCode, streetName=''):
        state = self.states()[stateCode]
        city = self.cities(stateCode)[cityCode]
        result = self._request('ConsultaVia',
            Provincia=state,
            Municipio=city,
            TipoVia='',
            NombreVia=streetName,
        )
        #f 'lerr' in result.consulta_callejero:
        #   error(result.consulta_callejero.lerr.dump())
        if not 'callejero' in result.consulta_callejero:
            return []
        return [
            ns(
                code = v.dir.cv,
                type = v.dir.tv,
                name = v.dir.nv,
            )
            for v in listify(result.consulta_callejero.callejero.calle)
        ]

    def retrieveBuilding(self, state, city, streetType, streetName, number):
        def formatAddress(address):
            info = address.dt.locs.lous.lourb
            return (
                info.dp,
                info.dir.tv,
                info.dir.nv,
                '-'.join(
                    info.dir[field]
                    for field in ('pnp','plp','snp','slp')
                    if info.dir.get(field,'0') != '0'
                ),
            )
        result = self._request('Consulta_DNPLOC',
            Provincia=state,
            Municipio=city,
            Sigla=streetType,
            Calle=streetName,
            Numero=str(number),
            Bloque='',
            Escalera='',
            Planta='',
            Puerta='',
        )

        if 'bico' in result.consulta_dnp:
            return [
                formatAddress(result.consulta_dnp.bico.bi)
            ]
        elif 'lrcdnp' in result.consulta_dnp:
            return [
                formatAddress(address)
                for address in listify(result.consulta_dnp.lrcdnp.rcdnp)
            ]
        else:
            return []

    def guessAddress(self, stateCode, cityCode, streetName, number):
        results = []
        state = self.states()[stateCode]
        city = self.cities(stateCode)[cityCode]
        added = set()
        for street in self.retrieveStreets(stateCode, cityCode, streetName):
            #step(f"Consulting number {state}, {city}, {street.type}. {street.name}, {number}")
            for result in self.retrieveBuilding(state, city, street.type, street.name, number):
                if result in added: continue
                postalcode, foundStreetType, foundStreetName, foundNumber = result
                added.add(result)
                results.append(ns(
                    postalcode = postalcode,
                    streettype = foundStreetType,
                    streetname = foundStreetName,
                    number = foundNumber,
                ))
        return results


if __name__ == '__main__':

    import sys
    state,city,street,number,*rest = sys.argv[1:] + ['']*4
    cadaster = Cadaster()
    if '--demo' in sys.argv:
        for case in [
            ('08','217', 'Frederic', 9), # Regular case
            ('08','217', 'Frederic', "1"), # 1 is duplicated as 1A, 1B...
            ('08','223', 'Canigó', "3"), # Tildes
            ('08','217', 'Jacint', 42), # Partial match
            ('08','101', 'Montserrat', 1),
            ('08','019', 'Diagonal', 100), # Missing
            ('08','019', 'Diagonal', 2),
            ('02','003', 'Cristobal colon', '3'), # same street name in diferent towns in the municipality
        ]:
            step("Searching: {} {} {} {}", *case)
            print(ns(streets=cadaster.guessAddress(*case)).dump())
    elif state=='':
        print(ns(states=cadaster.states()).dump())
    elif city=='':
        print(ns(cities=cadaster.cities(state)).dump())
    elif street=='':
        for item in sorted(cadaster.retrieveStreets(state, city), key=lambda x:int(x.code)):
            print("{code}: {type}. {name}".format(**item))
    elif number=='':
        for item in sorted(cadaster.retrieveStreets(state, city, street), key=lambda x:int(x.code)):
            print("{code}: {type}. {name}".format(**item))
    else:
        print(ns(streets=cadaster.guessAddress(state,city,street,number)).dump())





