from pathlib import Path
from yamlns import ns

# Functions to process callejero-censo-electoral
# Needs to be downloaded from: https://www.ine.es/prodyser/callejero/caj_esp/caj_esp_072021.zip
# Updated versions can be obtained at:
# https://www.ine.es/ss/Satellite?L=es_ES&c=Page&cid=1259952026632&p=1259952026632&pagename=ProductosYServicios%2FPYSLayout

ine_data_folder = Path('callejero-censo-electoral')
ine_data_date = '210630'

def loadPostalCodes():
    tramosIne = ine_data_date / f'TRAMOS_NAL_F{ine_data_date}'
    with path.open(encoding='latin1') as dbfile:
        extracted= list(sorted({
        (
            line[0:5], # ine municipio
            line[42:47], # codigo postal
            #line[78:82], # ine nucleo poblacional
            #line[85:110].strip(), # nombre entidad colectiva
            #line[110:135].strip(), # nombre entidad singular
            #line[135:160].strip(), # nombre nucleo/diseminado
        )
        for line in dbfile
    }))
    with Path('ine-cp-from-census.tsv').open('w', encoding='utf8') as f:
        f.write(f"municipio_id\tcodigo_postal\n")
        for municipio_id, codigo_postal, *a in extracted:
            f.write(f"{municipio_id}\t{codigo_postal}\n")
    return [
        dict(
            municipio_id=municipio_id,
            codigo_postal=codigo_postal,
        )
        for municipio_id, codigo_postal, *a in extracted
    ]

def loadStreetSections(citycode,):
    path = ine_data_date / f'TRAMOS_NAL_F{ine_data_date}'
    
    with path.open(encoding='latin1') as dbfile:
        return list(sorted({
            ns(
                citycode   = line[ 0: 5],
                streetcode = line[20:25],
                postalcode = line[42:47],
                numbertype = line[47:48],
                numberlow  = line[48:53],
                numberhigh = line[53:58],
            )
            for line in dbfile
        }))


def loadCityStreets(self, citycode):

    path = ine_data_date / f'VIAS_NAL_F{ine_data_date}'
    with path.open(encoding='latin1') as dbfile:
        return {
            street.code : street
            for street in (
                ns(
                    city = line[ 0: 5],
                    code = line[ 5:10].strip(),
                    name = line[33:83].strip(),
                )
                for line in dbfile
            )
            if street.city == citycode
        }


def loadCityStreetSections(citycode):

    path = ine_data_date / f'TRAMOS_NAL_F{ine_data_date}'
    with path.open(encoding='latin1') as dbfile:
        return {
            street.code : street
            for street in (
                ns(
                    city       = line[ 0: 5],
                    code       = line[20:25],
                    postalcode = line[42:47],
                    numbertype = line[47:48],
                    numberlow  = line[48:53],
                    numberhigh = line[53:58],
                    name       = line[165:190].strip(),
                )
                for line in dbfile
            )
            if street.city == citycode
        }


