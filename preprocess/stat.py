import pickle
from tqdm import tqdm
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import readfile as rf
import re
import json

def criteria_stat():
    path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21'
    trials = pickle.load(open(path_to_data, 'rb'))
    min_c = [float('inf')]*2
    max_c = [0]*2
    acc = [0]*2
    cnt = [0]*2
    all_num = [[],[]]
    total = [0]*2
    for idx, trial in enumerate(tqdm(trials)):
        doc = trial['number']
        for ci, ctype in enumerate(['inclusion_list', 'exclusion_list']):
            if ctype in trial and trial[ctype]:
                # min_c[ci] = min(min_c[ci], len(trial[ctype]))
                # max_c[ci] = max(max_c[ci], len(trial[ctype]))
                # acc[ci] += len(trial[ctype])
                all_num[ci].append(len(trial[ctype]))
                total[ci] += len(trial[ctype])
                c_cnt = 0
                for c in trial[ctype]:
                    flag = True
                    for out_scope in ['willing', 'written consent', 'signed']:
                        if out_scope in c:
                            flag = False
                    if flag:
                        c_cnt+=1
                min_c[ci] = min(min_c[ci], c_cnt)
                max_c[ci] = max(max_c[ci], c_cnt)
                acc[ci] += c_cnt
                cnt[ci] += 1
                all_num[ci].append(c_cnt)
                total[ci] += c_cnt
    print(len(trials))
    print(min_c)
    print(max_c)
    print(total)
    print(cnt)
    print(acc[0]/cnt[0], acc[1]/cnt[1])

    # counts, bins = np.histogram(np.array(all_num[0]), len(set(all_num[0])))
    # plt.hist(bins[:-1], bins, weights=counts)
    # counts, bins_1 = np.histogram(np.array(all_num[1]), len(set(all_num[0])))
    # plt.hist([bins[:-1], bins_1[:-1]], bins, )
    bins = np.linspace(0, 40, 40)
    plt.hist([np.array(all_num[0]), np.array(all_num[1])], bins, label=['inclusion', 'exclusion'])
    plt.axvline(x=acc[0]/cnt[0], c='blue')
    plt.axvline(x=acc[1] / cnt[1], c='orange')
    plt.xlim(0, 40)
    plt.legend(loc='upper right')
    plt.savefig('utils_parse_cfg/parsed_data/hist.png')

def check_phase():
    path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21'
    trials = pickle.load(open(path_to_data, 'rb'))
    phase = {}
    phase_criteria = {}

    for idx, trial in enumerate(tqdm(trials)):
        doc = trial['number']
        if trial['phase'] not in phase:
            phase[trial['phase']] = 0
            phase_criteria[trial['phase']] = [0,0,0,0]
        phase[trial['phase']] += 1
        for ci, ctype in enumerate(['inclusion_list', 'exclusion_list']):
            if ctype in trial and trial[ctype]:
                phase_criteria[trial['phase']][ci] += len(trial[ctype])
                phase_criteria[trial['phase']][ci+2] += 1
    tot = sum([phase[i] for i in phase])
    ratio = [(i, phase[i], phase[i]/tot) for i in phase]
    ratio.sort(key=lambda k: k[0], reverse=True)
    print(ratio)
    out = []
    for ph in phase_criteria:
        out.append((ph,phase_criteria[ph][0]/phase_criteria[ph][2], phase_criteria[ph][1]/phase_criteria[ph][3]))
    out.sort(key=lambda k:k[1],reverse=True)
    print(out)

def check_subgroup():
    subgroup = re.compile(r'For (.*?)')
    path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21'
    trials = pickle.load(open(path_to_data, 'rb'))
    subgroup_docs = set()
    for idx, trial in enumerate(tqdm(trials)):
        doc = trial['number']
        ctype = 'inclusion_list'
        if ctype in trial and trial[ctype]:
            for c in trial[ctype]:
                # if ' group' in c:
                if len(subgroup.findall(c)) > 0:
                    subgroup_docs.add(doc)
                    break
    print(len(subgroup_docs))
    with open('./utils_parse_cfg/parsed_data/subgroup_list.txt', "w") as outfile:
        outfile.writelines([i+'\n' for i in subgroup_docs])


def test_collection():
    qrels_path = '../../data/test_collection/qrels-clinical_trials.tsv'
    qrels = rf.read_qrel(qrels_path)
    doc_set = {}
    total = 0
    for qid in qrels:
        if qid not in doc_set:
            doc_set[qid] = 0
        for doc in qrels[qid]:
            if int(qrels[qid][doc])>0:
                doc_set[qid]+=1
                total+= 1

    print(doc_set)
    print(total, total/len(doc_set))

if __name__ == '__main__':
    # criteria_stat()
    # test_collection()
    # check_phase()
    check_subgroup()