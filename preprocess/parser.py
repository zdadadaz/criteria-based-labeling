import os
import copy
from multiprocessing import Pool, cpu_count
import json
import xml.etree.ElementTree as ET
import re
import pathlib
import numpy as np
import pickle

def read_xml(fn):
    tree = ET.parse(fn)
    root = tree.getroot()
    title = condition = criteria = None
    demo = {'gender': None, 'min_age': None, 'max_age': None, 'healthy':None}
    for child in root:
        if child.tag.lower() == 'brief_title':
            title = re.sub(r"[\n\t\r]*", "", root[2].text)
            title = re.sub(r"  ", "", title)
        elif child.tag.lower() == 'eligibility':
            for c in child:
                if c.tag.lower() == 'criteria':
                    criteria = c[0].text.replace('\"', '\'')
                elif c.tag.lower() == 'gender':
                    if c.text.lower().strip() == 'male':
                        demo['gender'] = 1
                    elif c.text.lower().strip() == 'female':
                        demo['gender'] = 0
                    else:
                        demo['gender'] = 2
                elif c.tag.lower() == 'minimum_age':
                    if 'year' in c.text.lower():
                        demo['min_age'] = int(c.text.split(' ')[0])
                    elif 'month' in c.text.lower() or 'day' in c.text.lower() or 'minutes' in c.text.lower() or 'week' in c.text.lower():
                        demo['min_age'] = int(1)
                    else:
                        demo['min_age'] = int(c.text[:-6]) if c.text != 'N/A' else np.nan
                elif c.tag.lower() == 'maximum_age':
                    if 'year' in c.text.lower():
                        demo['max_age'] = int(c.text.split(' ')[0])
                    elif 'month' in c.text.lower() or 'day' in c.text.lower() or 'minutes' in c.text.lower() or 'week' in c.text.lower() or 'hour' in c.text.lower():
                        demo['max_age'] = int(1)
                    else:
                        demo['max_age'] = int(c.text[:-6]) if c.text != 'N/A' else np.nan
                elif c.tag.lower == 'healthy_volunteers':
                    demo['healthy'] = c.text.lower()

        elif child.tag.lower() == 'condition':
            if not condition:
                condition = child.text.replace('\"', '\'')
            else:
                condition += ' and ' + child.text.replace('\"', '\'')

    return (title, condition, criteria, demo)


class study:
    def __init__(self, nct, name, condition, criteria):
        self.nct = nct
        self.name = name
        self.condition = condition
        self.criteria = criteria

        self.inclusion, self.exclusion = None, None
        self.patient, self.disease = None, None
        self.init_regex()

    def init_regex(self):
        self.reDeleteCriterion = re.compile(r'(?i)([^\n]+meet inclusion criteria|[^\n]*inclusion/exclusion criteria)\W? *(\n|$)')
        self.reMatchInclusions = re.compile(r'(?is)inclusions?(?: *:| criteria(?:[^:\n]*?:| *\n))(.*?)(?:[^\n]*\bexclusions?(?: *:| criteria(?:[^:\n]*?:| *\n))|$)')
        self.reMatchExclusions = re.compile(r'(?is)exclusions?(?: *:| criteria(?:[^:\n]*?:| *\n))(.*?)(?:[^\n]*\binclusions?(?: *:| criteria(?:[^:\n]*?:| *\n))|$)')

        self.reMatchDisease = re.compile(
            r'(?is)disease?(?: *:| characteristics(?:[^:\n]*?:| *\n))(.*?)(?:[^\n]*\bpatient?(?: *:| characteristics(?:[^:\n]*?:| *\n))|$)')
        self.reMatchPatients = re.compile(
            r'(?is)patient?(?: *:| characteristics(?:[^:\n]*?:| *\n))(.*?)(?:[^\n]*\bconcurrent?(?: *:| therapy(?:[^:\n]*?:| *\n))|$)')

        self.reMatchPrio = re.compile(
            r'(?is)concurrent?(?: *:| therapy(?:[^:\n]*?:| *\n))(.*?)(?:[^\n]*\bconcurrent?(?: *:| therapy(?:[^:\n]*?:| *\n))|$)')

        self.reTrimmer = re.compile(r'^(\s*-\s*)?(\s*\d+\.?\s*)?')
        self.reMatchTabs = re.compile(r'the following(\s+criteria)?(\s*:)?\s*\n\s*(-|\d+\.|[a-z]\s)\s*')
        self.reMatchTabLine = re.compile(r'the following')
        self.reMatchBulletLine = re.compile(r'^\s*(-|\d+\.|[a-z]\s)\s*')

    def Criteria(self):
        return self.criteria

    def Inclusion(self):
        return self.inclusion()

    def Exclusion(self):
        return self.exclusion()

    def Normalize(self, criterias):
        return self.reDeleteCriterion.sub("", criterias)

    def ExtractInclusion(self, criteria):
        return self._criteria(criteria, self.reMatchInclusions)

    def Extractexclusion(self, criteria):
        return self._criteria(criteria, self.reMatchExclusions)

    def ExtractPatient(self, criteria):
        return self._criteria(criteria, self.reMatchPatients)

    def ExtractDisease(self, criteria):
        return self._criteria(criteria, self.reMatchDisease)

    def ExtractPrio(self, criteria):
        return self._criteria(criteria, self.reMatchPrio)

    def _criteria(self, input, regex):
        out = []
        values = regex.findall(input)
        for value in values:
            lines = value.strip().split('\n\n')
            head = None
            for line in lines:
                if len(line) == 0:
                    continue
                a = line.replace('\n','').replace('  ', ' ').strip()
                atmp = []
                for j in a.split(' '):
                    if len(j) != 0:
                        atmp.append(j)
                a = ' '.join(atmp)
                if a[-1] == ':' and len(a) < 30:
                    head = a
                elif head and a[0] == "-":
                    out.append(head + a.replace('-',''))
                else:
                    out.append(a.replace('-','').strip())
        return out

    def ExtractCriteria(self):
        inclusions, exclusions, diseases, patients, prios = None, None, None, None, None
        normalized = self.Normalize(self.criteria)
        inclusions = self.ExtractInclusion(normalized)
        exclusions = self.Extractexclusion(normalized)
        diseases = self.ExtractDisease(normalized)
        patients = self.ExtractPatient(normalized)
        prios = self.ExtractPrio(normalized)
        if len(diseases) > 0:
            for i in diseases:
                inclusions.append(i)
        if len(patients) > 0:
            for i in patients:
                inclusions.append(i)
        if len(prios) > 0:
            for i in prios:
                inclusions.append(i)

        self.filter_short_criteria(inclusions)
        self.filter_short_criteria(exclusions)
        return inclusions, exclusions

    def filter_short_criteria(self, criteria_list):
        for each_item in criteria_list:
            if len(each_item) < 10:
                if 'Niacin' not in each_item and 'Ezetemibe' not in each_item and 'warfarin' not in each_item and 'pregnancy' not in each_item and 'Pregancy' not in each_item and 'sepsis' not in each_item and 'Dementia' not in each_item and 'Diabetes' not in each_item and 'phenytoin' not in each_item:
                    criteria_list.remove(each_item)

def run_parser(fn):
    nct = fn.split('/')[-1].split('.')[0]
    # out_path = './data_parsed/json/'
    # if os.path.exists(os.path.join(out_path, nct + '.json')):
    #     return
    # pathlib.Path(out_path).mkdir(parents=True, exist_ok=True)
    title, condition, criteria, demo = read_xml(fn)
    inclusions, exclusions= None, None
    if criteria:
        cur_study = study(nct, title, condition, criteria)
        inclusions, exclusions = cur_study.ExtractCriteria()

        # criteria = re.sub(r"[\n\t\r]*", "", criteria.lower().strip())
        # criteria = re.sub(r"   ", "", criteria)
        criteria_list = []
        if "\n\n" in criteria:
            for i in criteria.split("\n\n"):
                i = re.sub(r"[\n\t\r]*", "", i.lower().strip())
                i = re.sub(" +", " ", i)
                atmp = []
                for j in i.split(' '):
                    if len(j) != 0:
                        atmp.append(j)
                i = ' '.join(atmp)
                criteria_list.append(i)
        else:
            criteria = re.sub(r"[\n\t\r]*", "", criteria.lower().strip())
            criteria = re.sub(" +", " ", criteria)
            atmp = []
            for j in criteria.split(' '):
                if len(j) != 0:
                    atmp.append(j)
            criteria = ' '.join(atmp)
            criteria_list.append(criteria)

    if not inclusions and criteria and criteria_list:
        inclusions = criteria_list

    return (nct, condition, inclusions, exclusions)
