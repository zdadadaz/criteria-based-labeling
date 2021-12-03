import pickle
import os
from tqdm import tqdm
import readfile as rf
import re
import numpy as np
import xml.etree.ElementTree as ET
import json
from parser import read_xml, run_parser
import random
from multiprocessing import Pool, cpu_count

def gen_4_cfg():
    qrels_path = '../../data/test_collection/qrels-clinical_trials.tsv'
    root = '../../data/test_collection/clinicaltrials_xml/'
    path_to_out = './utils_parse_cfg/data/'
    path_to_out_doc = './utils_parse_cfg/data/doclist.txt'

    # qrels_path = None
    # root = '../../data/TRECCT2021/clinicaltrials_xml/'
    # path_to_out = './utils_parse_cfg/data/'
    # path_to_out_doc = path_to_out + 'doclist.txt'

    if qrels_path:
        qrels = rf.read_qrel(qrels_path)
        doc_set = set()
        for qid in qrels:
            for doc in qrels[qid]:
                doc_set.add(doc)

    filelist = []
    name_list = []
    for path, subdirs, files in os.walk(root):
        for name in files:
            if name[0] != '.' and name.split('.')[-1] == 'xml':# and name.split('.')[0] == 'NCT01007877': # and name.split('.')[0] in doc_set:
                filelist.append(os.path.join(path, name))
                name_list.append(name.split('.')[0] + '\n')

    with open(path_to_out_doc, 'w') as f:
        f.writelines(name_list)

    with open(os.path.join(path_to_out, "tc_0.csv"), "w") as fout:
        fout.write("#nct_id,title,has_us_facility,conditions,eligibility_criteria\n")
        for ii in tqdm(range(len(filelist))):
            title, condition, criteria, demo = read_xml(filelist[ii])
            nctid = filelist[ii].split('/')[-1].split('.')[0]
            if condition:
                condition = condition.replace('\"', '\'')
            if criteria:
                criteria = criteria.replace('\"','\'')
            title = 'None'
            # criteria = criteria.replace('DISEASE CHARACTERISTICS', 'Inclusion Criteria')
            fout.write(
                "{},\"{}\",{},\"{}\",\"{}\"\n".format(nctid, title, 'false', condition, criteria))
            fout.flush()

def check_and_parse_rest():
    root = '../../data/test_collection/clinicaltrials_xml/clinicaltrials_gov_16_dec_2015/'
    path_all_doc = './utils_parse_cfg/data/doclist.txt'
    root_out = './utils_parse_cfg/parsed_data/'
    path_to_extracted = root_out + 'extracted_judge_tc_0.csv'
    path_out_missed_doc = root_out + 'missed_judged_doclist.txt'
    path_out_pickle = root_out + 'completed_parsed_doc_dict'

    all_doc = set([l.strip().replace('\n', '') for l in open(path_all_doc, 'r')])

    exsit = set()
    trial_dict = {}
    for l in open(path_to_extracted):
        if l[0] == '#':
            continue
        nct, t, criteria = l.strip().split('\t')
        if nct not in trial_dict:
            trial_dict[nct] = {'number':nct,'inclusion':[], 'exclusion':[]}
        trial_dict[nct][t].append(criteria)
        exsit.add(nct)

    missed_doc = list(all_doc.difference(exsit))
    with open(path_out_missed_doc, 'w') as f:
        f.writelines([i+'\n' for i in missed_doc])

    missed_doc_list =[]
    for i in tqdm(range(len(missed_doc))):
        doc = missed_doc[i]
        doc_path = root + doc + '.xml'
        nct, condition, inclusions, exclusions = run_parser(doc_path)
        trial_dict[nct] = {'number':nct,'inclusion':inclusions, 'exclusion':exclusions}
        missed_doc_list.append({'number':nct,'inclusion':inclusions, 'exclusion':exclusions})

    print(len(all_doc), len(trial_dict))

    pickle.dump(trial_dict, open(path_out_pickle, 'wb'))
    # json_object = json.dumps(missed_doc_list, indent=2)
    # with open('./utils_parse_cfg/parsed_data/completed_parsed_doc.json', "w") as outfile:
    #     outfile.write(json_object)

def write_to_clean_data_ct21():
    path_to_data = 'data_parsed/clean_list_ct21'
    root_out = './utils_parse_cfg/parsed_data/'
    path_to_extracted = root_out + 'extracted_judge_ct21_0.csv'
    path_out_clean = './utils_parse_cfg/parsed_data/clean_data_cfg_ct21'
    path_out_missed_doc = root_out + 'missed_judged_doclist.txt'
    path_all_doc = '../../data/TRECCT2021/doclist_ct21.txt'

    all_doc = set([l.strip().replace('\n', '') for l in open(path_all_doc, 'r')])
    exsit = set()
    trial_dict = {}
    for l in open(path_to_extracted):
        if l[0] == '#':
            continue
        nct, t, criteria = l.strip().split('\t')
        if nct not in trial_dict:
            trial_dict[nct] = {'number': nct, 'inclusion': [], 'exclusion': []}
        trial_dict[nct][t].append(criteria)
        exsit.add(nct)

    missed_doc = list(all_doc.difference(exsit))
    with open(path_out_missed_doc, 'w') as f:
        f.writelines([i + '\n' for i in missed_doc])

    trials = pickle.load(open(path_to_data, 'rb'))
    for idx, trial in enumerate(tqdm(trials)):
        doc = trial['number']
        if doc in trial_dict:
            for ctype in ['inclusion', 'exclusion']:
                if ctype in trial_dict[doc] and trial_dict[doc][ctype]:
                    trial[ctype + '_list'] = trial_dict[doc][ctype]
    pickle.dump(trials, open(path_out_clean, 'wb'))

def write_to_clean_data():
    path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg'
    path_to_parsed_doc_list = './utils_parse_cfg/parsed_data/completed_parsed_doc_dict'
    path_out_clean = './utils_parse_cfg/parsed_data/clean_data_cfg_v2'

    trials = pickle.load(open(path_to_data, 'rb'))
    doc_dict = pickle.load(open(path_to_parsed_doc_list, 'rb'))

    for idx, trial in enumerate(tqdm(trials)):
        doc = trial['number']
        for ctype in ['inclusion', 'exclusion']:
            if ctype in doc_dict[doc] and doc_dict[doc][ctype]:
                trial[ctype + '_list'] = doc_dict[doc][ctype]
    pickle.dump(trials, open(path_out_clean, 'wb'))


def write_to_csv():
    # path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg'
    path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21_umls'
    trials = pickle.load(open(path_to_data, 'rb'))
    path_to_csv = f'utils_parse_cfg/parsed_data/ct21_all_data_umls.csv'
    qrels_path = '../../data/test_collection/qrels-clinical_trials.tsv'
    qrels = rf.read_qrel(qrels_path)
    from collections import defaultdict
    doc2qid = defaultdict(set)
    posDoc = {}
    negDoc = {}
    for qid in qrels:
        posDoc[qid] = set()
        negDoc[qid] = set()
        for doc in qrels[qid]:
            if int(qrels[qid][doc])>0:
                posDoc[qid].add(doc)
            else:
                negDoc[qid].add(doc)

    for qid in qrels:
        sampleNum = min(len(posDoc[qid]), 10)
        poss = random.choices(list(posDoc[qid]), k=sampleNum)
        for pos in poss:
            doc2qid[pos].add(qid + '_p')
        negs = random.choices(list(negDoc[qid]), k=sampleNum)
        for neg in negs:
            doc2qid[neg].add(qid + '_n')

    with open(path_to_csv, 'w') as f:
        cnt = 0
        f.write("sid,trial_id,nctid,type,typeid,text,flag,umls\n")
        for idx, trial in enumerate(tqdm(trials)):
            if trial['number'] in doc2qid:
                qrel = ' '.join(doc2qid[trial['number']])
            else:
                qrel = ''

            for ctype in ['inclusion_list', 'exclusion_list']:
                if ctype in trial and trial[ctype]:
                    for i_idx, i in enumerate(trial[ctype]):
                        # if len(qrel) != 0:
                        f.write(
                            "{},{},{},{},{},\"{}\",{},\"{}\"\n".format(cnt, idx, trial['number'], ctype[0], i_idx, i,
                                                                    trial[ctype[0]+'_flag'][i_idx], trial[ctype[0]+'_umls'][i_idx]))
                        cnt += 1
            for ctype in ['condition']:
                if ctype in trial and trial[ctype]:
                    # if len(qrel) != 0:
                    f.write("{},{},{},{},{},\"{}\",{},\"{}\"\n".format(cnt, idx, trial['number'], ctype[0], 0,
                                                                       trial[ctype].replace('"', '').replace(',', ' '),
                                                                     trial[ctype[0]+'_flag'], trial[ctype[0]+'_umls']))
                    cnt += 1
            f.flush()


def gen_desc_boolean_pair():
    path_out = 'utils_parse_cfg/data/desc_bool_cond.csv'
    root_path = '../../data/test_collection'
    query_desc = {}
    rf.read_ts_topic(query_desc, root_path + '/topics-2014_2015-description.topics')
    query_bool = rf.read_json(root_path+'/boolean_qid.json')
    query_bool_dict = {k['qqid']:[v['keywords'] for v in k['keywords']] for k in query_bool}

    path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg'
    trials = pickle.load(open(path_to_data, 'rb'))
    path_to_csv = 'utils_parse_cfg/parsed_data/judeged_only_data.csv'

    qrels_path = '../../data/test_collection/qrels-clinical_trials.tsv'
    qid2pos = {}
    qrels = rf.read_qrel(qrels_path)
    for qid in qrels:
        if qid not in qid2pos:
            qid2pos[qid] = set()
        for doc in qrels[qid]:
            if int(qrels[qid][doc])>0:
                qid2pos[qid].add(doc)
    qid2cond = {}
    for idx, trial in enumerate(tqdm(trials)):
        doc = trial['number']
        for qid in qid2pos:
            if qid not in qid2cond:
                qid2cond[qid] = set()
            if doc in qid2pos[qid]:
                for cond in trial['condition'].split('and'):
                    qid2cond[qid].add(cond.strip())

    with open(path_out, 'w') as f:
        for qid in query_desc:
            desc = query_desc[qid]['text'].replace('"','')
            bool = ' | '.join(query_bool_dict[qid])
            bool = bool.replace('"','')
            cond = ' | '.join(qid2cond[qid]) if qid in qid2cond else None
            if cond:
                cond = cond.replace('"', '')
            f.write("{},\"{}\",\"{}\",\"{}\"\n".format(qid, desc, bool, cond))
        f.flush()

def run_umls(cpu):
    from filter import Quickumls
    quickumls = Quickumls()
    path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21'
    trials = pickle.load(open(path_to_data, 'rb'))
    path_to_csv = f'utils_parse_cfg/parsed_data/intermediate/ct21_all_data_umls_{cpu}.json'

    tot_cpu = 8
    interval = len(trials)// tot_cpu
    start = cpu * interval
    end = (cpu+1) * interval if cpu != (tot_cpu-1) else len(trials)

    trials_umls = []
    for idx, trial in enumerate(tqdm(trials[start:end])):
        if trial['number'] not in trials_umls:
            tmp_trial = {'number': trial['number'],'i_umls':[], 'e_umls':[], 'i_flag':[], 'e_flag':[]}
        for ctype in ['inclusion_list', 'exclusion_list']:
            if ctype in trial and trial[ctype]:
                for i_idx, i in enumerate(trial[ctype]):
                    umls, maxclass = quickumls.run(i)
                    i = i.lower()
                    flag = maxclass
                    if ((' consent' in i or ' signed' in i) and \
                        (' age' not in i and ' year' not in i)) and \
                        len(umls) <= 3:
                        flag = 0
                    tmp_trial[ctype[0] + '_flag'].append(flag)
                    tmp_trial[ctype[0] + '_umls'].append(umls)
        for ctype in ['condition']:
            if ctype in trial and trial[ctype]:
                umls, maxclass = quickumls.run(trial[ctype].replace('"', '').replace(',', ' '))
                tmp_trial[ctype[0] + '_flag'] = maxclass
                tmp_trial[ctype[0] + '_umls'] = umls
        trials_umls.append(tmp_trial)
    json_object = json.dumps(trials_umls, indent=2)
    with open(path_to_csv, "w") as outfile:
        outfile.write(json_object)


import argparse
def multiprocess():
    parser = argparse.ArgumentParser()
    parser.add_argument('--i', help="split", default=0)
    args = parser.parse_args()
    run_umls(int(args.i))

def reduce():
    path_to_file = 'utils_parse_cfg/parsed_data/intermediate/'
    path_to_cfg = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21'
    path_out_clean = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21_umls'
    trials = pickle.load(open(path_to_cfg, 'rb'))

    trials_umls = []
    for i in range(8):
        path_json = path_to_file + f'ct21_all_data_umls_{i}.json'
        with open(path_json) as f:
            ct21_umls = json.load(f)
        trials_umls += ct21_umls
    for idx, trial in enumerate(tqdm(trials)):
        if trial['number'] == trials_umls[idx]['number']:
            for k, v in trials_umls[idx].items():
                trial[k] = v
        else:
            print('not match nctid')
    pickle.dump(trials, open(path_out_clean, 'wb'))

if __name__ == '__main__':
    # gen_4_cfg()
    # check_and_parse_rest()
    write_to_csv()
    # multiprocess()
    # write_to_clean_data()
    # write_to_clean_data_ct21()
    # gen_desc_boolean_pair()
    # reduce()
