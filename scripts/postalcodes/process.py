#!/usr/bin/env python3

import typer
from pathlib import Path
from typing import List
import re
from svgis import projection, bounding
import fiona

app = typer.Typer()

# fix ine_codes

@app.command()
def fix_geojsons(
    geojsonfiles: List[Path] = typer.Argument(
        ...,
        metavar='FILE.geojson',
        exists=True,
        readable=True,
    ),
    crs: str='file',
    latlon: bool=False,
):
    # Source does not consider CODIGO_INE a string and strips leading zeroes
    # Add zero pading to 5 digits to CODIGO_INE and quote it in order to be a string
    fivedigits = re.compile(r'"CODIGO_INE": ([0-9]{5})\b')
    fourdigits = re.compile(r'"CODIGO_INE": ([0-9]{4})\b')
    for path in geojsonfiles:
        typer.echo(f"Procesando {path}...")
        content = path.read_text(encoding='utf8')
        content = re.sub(fivedigits, r'"CODIGO_INE": "\1"', content)
        content = re.sub(fourdigits, r'"CODIGO_INE": "0\1"', content)
        path.write_text(content, encoding='utf8')

    allbounds = [
        float('inf'),
        float('inf'),
        float('-inf'),
        float('-inf'),
    ]

    if latlon:
        fmt = '{0}: {1[1]} {1[0]} {1[3]} {1[2]}'
    else:
        fmt = '{0}: {1[0]} {1[1]} {1[2]} {1[3]}'

    # Compute layer union boundary
    for path in geojsonfiles:
        with fiona.Env():
            with fiona.open(path, "r") as f:
                meta = {'bounds': f.bounds}
                meta.update(f.meta)
        out_crs = projection.pick(crs, meta['bounds'], file_crs=meta['crs'])
        result = bounding.transform(meta['bounds'], in_crs=meta['crs'], out_crs=out_crs)

        typer.echo(fmt.format(path, result))
        allbounds = [
            min(allbounds[0], result[0]),
            min(allbounds[1], result[1]),
            max(allbounds[2], result[2]),
            max(allbounds[3], result[3]),
        ]

    typer.echo(fmt.format('Collected', allbounds))






if __name__ == '__main__':
    app()

