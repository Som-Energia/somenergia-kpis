#!/usr/bin/env python3

from somutils.dbutils import runsql_cached
from consolemsg import step, warn, success, error
from yamlns import namespace as ns


unsolved = ns.load('unsolved.yaml').unsolved
notype = []

from cadaster import Cadaster
cadaster = Cadaster()
for address in runsql_cached('contractaddresses.sql', unsolved=unsolved):
    print(address)
    street = address.streetname.lower().translate(transliterations)
    streettype, remaining = splitStreetType(street)
    stdtype = stdtypes[streettype] if streettype else '??'
    if not streettype:
        notype.append(remaining)
    print(remaining)
    streetparts = [
        x for x in remaining.split()
        if x not in commonwords
    ]
    if address.citycode[:2] in ('01',):
        warn("Euskadi castastroak trasapareik da")
        continue
    streetCandidates = {}
    state = address.citycode[:2]
    city = address.citycode[2:]
    number = None
    for part in streetparts:
        if not part.strip():
            continue
        if part.isdigit():
            number = part
            break
        if len(part.strip())<2:
            continue
        for item in sorted(
            cadaster.retrieveStreets(state, city, part),
            key=lambda x:int(x.code),
        ):
            streetCandidates.setdefault(item.code, item).setdefault('votes',0)
            streetCandidates[item.code]['votes']+=1
            if item.type == stdtype.upper():
                streetCandidates[item.code]['votes']+=1

    for candidate in sorted(streetCandidates.values(), key=lambda x: -x['votes']):
        print(f"\t{address.streetname} ({number} {stdtype}) -> {candidate.votes} -> {candidate}")




print(f"Sense tipus: {len(notype)}/{len(unsolved)}")





    





