import re

DEPARTMENTS = {
    "Ain": "01",
    "Aisne": "02",
    "Allier": "03",
    "Alpes-de-Haute-Provence": "04",
    "Hautes-Alpes": "05",
    "Alpes-Maritimes": "06",
    "Ardèche": "07",
    "Ardennes": "08",
    "Ariège": "09",
    "Aube": "10",
    "Aude": "11",
    "Aveyron": "12",
    "Bouches-du-Rhône": "13",
    "Calvados": "14",
    "Cantal": "15",
    "Charente": "16",
    "Charente-Maritime": "17",
    "Cher": "18",
    "Corrèze": "19",
    "Corse-du-Sud": "2A",
    "Corse du Sud": "2A",
    "Haute-Corse": "2B",
    "Côte-d'Or": "21",
    "Côtes-d'Armor": "22",
    "Creuse": "23",
    "Dordogne": "24",
    "Doubs": "25",
    "Drôme": "26",
    "Eure": "27",
    "Eure-et-Loir": "28",
    "Finistère": "29",
    "Gard": "30",
    "Haute-Garonne": "31",
    "Gers": "32",
    "Gironde": "33",
    "Hérault": "34",
    "Ille-et-Vilaine": "35",
    "Indre": "36",
    "Indre-et-Loire": "37",
    "Isère": "38",
    "Jura": "39",
    "Landes": "40",
    "Loir-et-Cher": "41",
    "Loire": "42",
    "Haute-Loire": "43",
    "Loire-Atlantique": "44",
    "Loiret": "45",
    "Lot": "46",
    "Lot-et-Garonne": "47",
    "Lozère": "48",
    "Maine-et-Loire": "49",
    "Manche": "50",
    "Marne": "51",
    "Haute-Marne": "52",
    "Mayenne": "53",
    "Meurthe-et-Moselle": "54",
    "Meuse": "55",
    "Morbihan": "56",
    "Moselle": "57",
    "Nièvre": "58",
    "Nord": "59",
    "Oise": "60",
    "Orne": "61",
    "Pas-de-Calais": "62",
    "Puy-de-Dôme": "63",
    "Pyrénées-Atlantiques": "64",
    "Hautes-Pyrénées": "65",
    "Pyrénées-Orientales": "66",
    "Bas-Rhin": "67",
    "Haut-Rhin": "68",
    "Rhône": "69",
    "Haute-Saône": "70",
    "Saône-et-Loire": "71",
    "Sarthe": "72",
    "Savoie": "73",
    "Haute-Savoie": "74",
    "Paris": "75",
    "Seine-Maritime": "76",
    "Seine-et-Marne": "77",
    "Yvelines": "78",
    "Deux-Sèvres": "79",
    "Somme": "80",
    "Tarn": "81",
    "Tarn-et-Garonne": "82",
    "Var": "83",
    "Vaucluse": "84",
    "Vendée": "85",
    "Vienne": "86",
    "Haute-Vienne": "87",
    "Vosges": "88",
    "Yonne": "89",
    "Territoire de Belfort": "90",
    "Essonne": "91",
    "Hauts-de-Seine": "92",
    "Seine-Saint-Denis": "93",
    "Val-de-Marne": "94",
    "Val-d'Oise": "95",
    "Guadeloupe": "971",
    "Martinique": "972",
    "Guyane": "973",
    "La Réunion": "974",
    "Saint-Pierre-et-Miquelon": "975",
    "Mayotte": "976",
    "Saint-Martin": "978",
}

AUTHORITY_KEYWORDS = {
    "Guadeloupe": "971",
    "Martinique": "972",
    "Guyane": "973",
    "La Réunion": "974",
    "Saint-Pierre-et-Miquelon": "975",
    "Mayotte": "976",
    "Saint-Martin": "978",
}

REGIONS = {
    "Auvergne-Rhône-Alpes": [
        "01",
        "03",
        "07",
        "15",
        "26",
        "38",
        "42",
        "43",
        "63",
        "69",
        "73",
        "74",
    ],
    "Bourgogne-Franche-Comté": ["21", "25", "39", "58", "70", "71", "89", "90"],
    "Bretagne": ["35", "22", "56", "29"],
    "Centre-Val de Loire": ["18", "28", "36", "37", "41", "45"],
    "Centre-Val-de-Loire": ["18", "28", "36", "37", "41", "45"],
    "Corse": ["2A", "2B"],
    "Grand Est": ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"],
    "Guadeloupe": ["971"],
    "Guyane": ["973"],
    "Hauts-de-France": ["02", "59", "60", "62", "80"],
    "Île-de-France": ["75", "77", "78", "91", "92", "93", "94", "95"],
    "La Réunion": ["974"],
    "Martinique": ["972"],
    "Midi-Pyrénées": ["09", "12", "31", "32", "46", "65", "81", "82"],
    "Normandie": ["14", "27", "50", "61", "76"],
    "Nouvelle-Aquitaine": [
        "16",
        "17",
        "19",
        "23",
        "24",
        "33",
        "40",
        "47",
        "64",
        "79",
        "86",
        "87",
    ],
    "Occitanie": [
        "09",
        "11",
        "12",
        "30",
        "31",
        "32",
        "34",
        "46",
        "48",
        "65",
        "66",
        "81",
        "82",
    ],
    "Pays de la Loire": ["44", "49", "53", "72", "85"],
    "Provence-Alpes-Côte d'Azur": ["04", "05", "06", "13", "83", "84"],
}


def department_from_authority(authority):
    """Match department from authority field. Returns 1 dept code as string or an empty string."""

    department = ""
    for keyword in AUTHORITY_KEYWORDS:

        if keyword in authority:
            department = AUTHORITY_KEYWORDS[keyword]

    return department


def departments_from_project_name(project_name):
    """Match departments from project name, via regex"""

    departments = []

    # Find parentheses with possible matches in project
    matches_parentheses = re.findall(
        r"\(([A-B0-9 \-,;//\+]+(?: et[A-B0-9 \-,;//]+)?)\)", project_name
    )
    #

    # Extract departments from matches
    for m in matches_parentheses:
        # Replacing + by space, as it is not considered a word boundary
        m = m.replace("+", " ")

        match_dept_nos = re.findall(
            r"\b([02][1-9]|2[AB]|[1345678][0-9]|9[012345]|97[1-8])\b", m
        )

        if match_dept_nos:
            for d in match_dept_nos:
                departments.append(d)

    # By department name in parentheses
    if not departments:
        for dept in DEPARTMENTS:
            # escaped_dept = dept = dept.replace("'", "'")
            dept_no_hyphens = dept.replace("-", " ")

            if re.search(rf"\({dept}\)", project_name, re.IGNORECASE) or re.search(
                rf"\({dept_no_hyphens}\)$", project_name, re.IGNORECASE
            ):
                departments.append(DEPARTMENTS[dept])

    # By Region name
    if not departments:
        for reg in REGIONS:
            # escaped_region = reg.replace("'", "'")
            reg_no_hyphens = reg.replace("-", " ")

            if re.search(
                rf"\brégion {reg}\b", project_name, re.IGNORECASE
            ) or re.search(
                rf"\brégion {reg_no_hyphens}\b", project_name, re.IGNORECASE
            ):
                for d in REGIONS[reg]:
                    departments.append(d)

    # Remove duplicates & order
    departments = sorted(list(set(departments)))

    return departments
