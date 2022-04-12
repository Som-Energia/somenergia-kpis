#!/usr/bin/env python3

from somutils.dbutils import runsql_cached, runsql, tsvread
from somutils.pathlib import Path
from consolemsg import step, warn, success, error
from yamlns import namespace as ns

"""
Postal codes to clean up appear in `res.partner.address` and `giscedata.cups_ps`.
Both models relate to res.municipi which has the INE code as attribute.

Each pair of 'ine code' and 'postal code' values is a 'case'.
The input data is a tsv that contains at least this relations: citycode, postalcode, ids.
The field `ids` are the comma separated ids of the objects having the same citycode and postalcode.
"""
class CPSolver():
    def __init__(self):
        self.solved = {} # id -> corrected postalcode
        self.unsolved = set() # unsolved ids
        self.caseReport = ns() # Identified cases: tag -> list(ids)

    def _idsFromCase(self, case):
        """extract the ids of the case"""
        return (int(id) for id in case['ids'].split(','))

    def solve(self, id, postalcode):
        """Solves an id by assigning it a postal code"""
        self.unsolved.remove(id)
        self.solved[id] = postalcode

    def solveCase(self, case, postalcode):
        """Solves all the ids of a case by assigning them a postal code.
        A case is the set of ids which match both cp and ine.
        """
        for id in self._idsFromCase(case):
            self.solve(id, postalcode)
        return True

    def pendingCases(self, cases):
        """Adds the ids of the cases as unsolved"""
        for case in cases:
            #print(f"Cases: {case['ids']}")
            self.unsolved.update(self._idsFromCase(case))

    def _countIds(self, cases):
        # Hack: number of cases + commas in ids (there is a comma per additional id)
        return len(cases) + sum(case['ids'].count(',') for case in cases)

    def report(self, cases, tag, message, solveByIne=True, detail=True):
        """Report a class of cases identified as the tag using the message as description..
        If solveByIne, tries to solve them if case's ine has a single CP.

        """

        step(f"{message}: {self._countIds(cases)} (diferents {len(cases)})")
        # Append the classified ids to the tag
        for case in cases:
            caseIds = self.caseReport.setdefault(tag, [])
            caseIds.extend(self._idsFromCase(case))

        # solveByIne tells us that, for this case we can try to solve it from the INE
        if solveByIne:
            solveableCases = self.solvedBySingleCpIne(cases)
            step(f"\tAmb fix directe perque l'INE només té un CP: {self._countIds(solveableCases)}"
                f" (diferents {len(solveableCases)})")
            for case in solveableCases:
                solution = self.ine2postalcodes[case['citycode']][0]
                self.solveCase(case, solution)
        if not detail: return
        self.printCases(cases)

    def printCases(self, cases):
        lastcity = None
        for case in cases:
            if case['citycode'] in self.citiesWithOnePostalCode:
                success("\t{citycode}\t{postalcode}\t{ncontracts}\t{solutions}\t{ids}".format(
                    solutions=','.join(str(s) for s in self.ine2postalcodes[case['citycode']]),
                    **case))
            elif case['citycode'] in self.ine2postalcodes:
                if lastcity != case['citycode']:
                    solutions=','.join(str(s) for s in self.ine2postalcodes[case['citycode']])
                    warn(f"\t\tINE {case['citycode']} -> CP {solutions}")
                warn("\t{citycode}\t{postalcode}\t{ncontracts}\t{solutions}\t{ids}".format(
                    solutions="MANYCPS",
                    **case))
            else:
                error("\t{citycode}\t{postalcode}\t{ncontracts}\t{solutions}\t{ids}".format(
                    solutions="BADINE",
                    **case))
            lastcity = case['citycode']

    def solvedBySingleCpIne(self, cases):
        return [x for x in cases
            if x['citycode'] in self.citiesWithOnePostalCode
        ]

    def analyze(self):

        import ine

        existingCpInePairs = ine.loadPostalCodes()

        # Fix the input data, INE codes are not zero padded
        for cp in existingCpInePairs:
            if len(cp['municipio_id']) == 4:
                cp['municipio_id'] = '0' + cp['municipio_id']

        # Valid INE-CP pairs (an INE may have many CP's and a CP may have many INE's)
        validIneCp = {
            f"{d['codigo_postal']}-{int(d['municipio_id']):05d}"
            for d in existingCpInePairs
        }
        # All CP's in Spanish census
        validCp = {
            d['codigo_postal']
            for d in existingCpInePairs
        }

        # Given an INE which CP's are valid
        self.ine2postalcodes = {}
        for cp in existingCpInePairs:
            self.ine2postalcodes.setdefault(cp['municipio_id'], []).append(cp['codigo_postal'])

        # Those are resolutable without any search
        self.citiesWithOnePostalCode = {
            k for k,v in self.ine2postalcodes.items()
            if len(v)==1
        }

        # Those would require more inteligence
        self.citiesWithManyPostalCodes = {
            k for k,v in self.ine2postalcodes.items()
            if len(v)>1
        }

        step(f"Cities with many postal codes: {len(self.citiesWithManyPostalCodes)}")
        step(f"Cities with a single postal code: {len(self.citiesWithOnePostalCode)}")


        def loadAllCases():
            for d in runsql_cached('allcups.sql'):
                yield ns(d)

        def loadWrongCases():
            return [
                d for d in loadAllCases()
                if "{postalcode}-{citycode}".format(**d) not in validIneCp
            ]

        def partition(alist, criteria):
            falsists, truists = [], []
            for item in alist:
                if criteria(item):
                    truists.append(item)
                else:
                    falsists.append(item)
            return falsists, truists

        remaining = loadWrongCases()
        self.pendingCases(remaining)
        self.report(remaining, 'toreview', "CUPS amb el codi postal erroni", detail=False, solveByIne=False)
        step("Dels quals:")
        """
        for case in remaining:
            if case['postalcode_cadaster'].strip():
                print(f"{a}")
        """
        remaining, badIne = partition(remaining, lambda x:
            x['citycode'] not in self.ine2postalcodes
        )
        self.report(badIne, 'badine', "L'INE d'entrada no existeix", detail=False, solveByIne=False)

        remaining, noneCases = partition(remaining, lambda x:
            x['postalcode'] == "None" or
            x['postalcode'].strip() == "" or
            x['postalcode'].strip() == "0"
        )
        self.report(noneCases, 'empty', "Codi postal buit", detail=False)

        remaining, nondigitCases = partition(remaining, lambda x:
            not x['postalcode'].strip().isdigit()
        )
        self.report(nondigitCases, 'dirty', "Codi postal amb brossa (caracters no numerics)", detail=False)

        remaining, shorter = partition(remaining, lambda x:
            x['postalcode'].isdigit() and len(x['postalcode'].strip())!=5
        )
        shorter, leadingZero = partition(shorter, lambda x:
            f"{x['postalcode'].strip():0>5}-{x['citycode']}".format(**x) in validIneCp
            and self.solveCase(x, f"{x['postalcode'].strip():0>5}")
        )

        self.report(leadingZero, 'zerounpadded', "Es poden arreglar afegint zeros a l'esquerra, perque resulta un CP del INE", solveByIne=False, detail=False)

        self.report(shorter, 'short', "Li falten altres digits", detail=False)

        remaining, madeupCases = partition(remaining, lambda x:
            x['postalcode'] not in validCp
        )
        self.report(madeupCases, 'madeup', "Codis postals amb format correcte pero que no existeixen", detail=False)

        self.report(remaining, 'inemissmatch', "Codis postals existents que no coincideixen amb l'INE", solveByIne=False, detail=False)

        step(f"Es poden resoldre automaticament {len(self.solved)}")
        step(f"Pendents {len(self.unsolved)}")

        def acceptDifferences(old, new):
            Path(new).rename(old)

        def showDifferences(old, new):
            red = '\033[31m'
            green = '\033[32m'
            reset = '\033[0m'

            def safeload(filename):
                path = Path(filename)
                if not path.exists(): return ns()
                return ns.load(path)

            olddata = safeload(old)
            newdata = safeload(new)
            for tag in olddata.keys() | newdata.keys():
                oldids = set(olddata.get(tag, []))
                newids = set(newdata.get(tag, []))
                added = newids - oldids
                removed = oldids - newids
                if added:
                    print(f"\tNous {tag} ({red}{len(added)}{reset}): {','.join(str(x) for x in added)}{reset}")
                if removed:
                    print(f"\tFixats {tag} ({green}{len(removed)}{reset}): {','.join(str(x) for x in removed)}{reset}")


        def dump(data, filename, description):
            step(f"Dumping {filename}: {description}")
            ns(data).dump(filename)

        dump(self.caseReport, 'report.yaml', "Informe de casos")

        showDifferences(old='report-old.yaml', new='report.yaml')
        #acceptDifferences(old='report-old.yaml', new='report.yaml')


        dump(self.solved, 'solved.yaml', "Casos resolts per codipostal unic")
        unsolved=list(sorted(self.unsolved))
        dump(ns(unsolved=unsolved), 'unsolved.yaml', "Casos no resolts per codi postal unic")

solver = CPSolver()
solver.analyze()




    





