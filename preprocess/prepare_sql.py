import copy
import pickle
import readfile as rf
import random
from tqdm import tqdm
import json
from collections import defaultdict


def write_query_to_sql():
    # path_to_topics = '../../data/TRECCT2021/topics2021.xml'
    # query_org = rf.read_topics_ct21(path_to_topics)
    path_out_sql = './utils_parse_cfg/sqlfile/query.txt'
    path_to_topics = '../../data/test_collection/topics-2014_2015-description.topics'
    query_org = {}
    rf.read_ts_topic(query_org, path_to_topics)
    # with open(path_out_sql, 'w') as f:
    #     f.write("INSERT INTO \"query\" (\"Qid\", \"text\") VALUES\n")
    #     for idx, query in query_org.items():
    #         f.write("({},\"{}\"),\n".format(idx, query.replace('\"','')[1:-1]))
    #     f.flush()

    out = [{'id': i, 'text': j} for i, j in query_org.items()]
    json_object = json.dumps(out, indent=2)
    with open('./utils_parse_cfg/sqlfile/queries.json', "w") as outfile:
        outfile.write(json_object)


def write_document_criteria_to_sql():
    # from filter import Quickumls
    # quickumls = Quickumls()
    path_to_data = 'utils_parse_cfg/parsed_data/clean_data_cfg'
    trials = pickle.load(open(path_to_data, 'rb'))
    path_to_csv = 'utils_parse_cfg/sqlfile/judged_10_data.txt'
    qrels_path = '../../data/test_collection/qrels-clinical_trials.tsv'

    path_out_criteria = 'utils_parse_cfg/sqlfile/criteria.txt'
    path_out_criteria = 'utils_parse_cfg/sqlfile/doc.txt'
    path_out_criteria = 'utils_parse_cfg/sqlfile/docCriteria.txt'

    qrels = rf.read_qrel(qrels_path)
    doc2qid = defaultdict(set)
    posDoc = {}
    negDoc = {}
    for qid in qrels:
        posDoc[qid] = set()
        negDoc[qid] = set()
        for doc in qrels[qid]:
            if int(qrels[qid][doc]) > 0:
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

    cnt = 0
    document = ["INSERT INTO \"doc\" (\"Did\") VALUES\n"]
    criteria = ["INSERT INTO \"criteria\" (\"Cid\",\"text\",\"type\",\"typeId\") VALUES\n"]
    out_json = []
    query_doclist = {}
    assignment = []
    for idx, trial in enumerate(tqdm(trials)):
        if trial['number'] in doc2qid:
            qrel = ' '.join(doc2qid[trial['number']])
        else:
            qrel = ''
        if len(qrel) != 0:
            for qdoc in doc2qid[trial['number']]:
                if qdoc[:-2] not in query_doclist:
                    query_doclist[qdoc[:-2]] = []
                query_doclist[qdoc[:-2]].append(trial['number'])
                assignment.append(
                    {'UserID': random.choice([1, 2]), 'QueryID': qdoc[:-2], 'DocumentID': trial['number']})
            dict_tmp = {'number': trial['number'], 'criteriaNumber': 0, 'criteria': []}
            type_id_cnt = 1
            for type in ['inclusion_list', 'exclusion_list', 'condition']:
                if type in trial and trial[type]:
                    if type[0] == 'c':
                        dict_tmp['criteria'].append({'text': trial[type], 'type': 'c', 'type_id': type_id_cnt})
                        type_id_cnt += 1
                    elif trial[type][0]:
                        for ii in trial[type]:
                            dict_tmp['criteria'].append({'text': ii, 'type': type[0], 'type_id': type_id_cnt})
                            type_id_cnt += 1
            dict_tmp['criteriaNumber'] = type_id_cnt - 1
            out_json.append(dict_tmp)
            # document.append("(\"{}\"),\n".format(trial['number']))
            # for ctype in ['inclusion_list', 'exclusion_list']:
            #     if ctype in trial and trial[ctype]:
            #         for i_idx, i in enumerate(trial[ctype]):
            #             criteria.append("(\"{}\",\"{}\",\"{}\",\"{}\"),\n".format(cnt, i, ctype[0], i_idx))
            #             cnt += 1
            # for ctype in ['condition']:  # ['title', 'condition', 'kw']:
            #     if ctype in trial and trial[ctype]:
            #         txt = trial[ctype].replace('"', '').replace(',',' ')
            #         criteria.append("(\"{}\",\"{}\",\"{}\",\"{}\"),\n".format(cnt, txt, ctype[0], 0))
            #         cnt += 1

    json_object = json.dumps(out_json, indent=2)
    with open('./utils_parse_cfg/sqlfile/document-entities.json', "w") as outfile:
        outfile.write(json_object)

    query_doclist_out = []
    for qid in query_doclist:
        query_doclist_out.append({'QID': qid, 'NCTID': query_doclist[qid]})
    json_object = json.dumps(query_doclist_out, indent=2)
    with open('./utils_parse_cfg/sqlfile/query_assignment.json', "w") as outfile:
        outfile.write(json_object)

    json_object = json.dumps(assignment, indent=2)
    with open('./utils_parse_cfg/sqlfile/query_assignment_entity.json', "w") as outfile:
        outfile.write(json_object)


class Anno:
    def __init__(self):
        self.write_csv_for_annotation()

    def write_csv_for_annotation(self):
        self.path_to_file = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21_umls'
        self.path_to_qrels = '../../data/TRECCT2021/trec-ct2021-qrels.txt'
        self.out_path = 'utils_parse_cfg/anno/ct21_judged.json'
        self.remove_doc = set()
        self.readfile()
        self.prepare_document()
        self.write_out()

    def readfile(self):
        self.qrels = rf.read_qrel(self.path_to_qrels)
        self.trials = pickle.load(open(self.path_to_file, 'rb'))
        random.seed(123)

        PosNegDoc = {}
        for qid in self.qrels:
            PosNegDoc[qid] = [[] for i in range(3)]
            for doc in self.qrels[qid]:
                PosNegDoc[qid][int(self.qrels[qid][doc])].append(doc)

        totJudged = [0 for i in range(3)]
        for qid in PosNegDoc:
            for i in range(3):
                totJudged[i] += len(PosNegDoc[qid][i])

        self.PosNegDoc = PosNegDoc
        print('total judged each label', totJudged)
        print('avg judged each label', [i / 75 for i in totJudged])
        print('total judged', sum([i for i in totJudged]))
        print('avg judged each topic', sum([i for i in totJudged]) / 75)

    def prepare_document(self):
        self.doc2qid = defaultdict(set)
        for qid in self.qrels:
            for i, suffix in enumerate(['_u', '_n', '_p']):
                sample_num = 10 if i == 2 else 5
                chosed = random.choices(list(set(self.PosNegDoc[qid][i]).difference(self.remove_doc)), k=sample_num)
                for doc in chosed:
                    self.doc2qid[doc].add(qid + suffix)

    def write_out(self):
        out_json = []
        for idx, trial in enumerate(tqdm(self.trials)):
            if trial['number'] in self.doc2qid:
                qrel = ' '.join(self.doc2qid[trial['number']])
            else:
                qrel = ''
            if len(qrel) != 0:
                dict_tmp = {'number': trial['number'], 'criteriaNumber': 0, 'criteria': [], 'qrels': qrel,
                            'valid': True}
                type_id_cnt = 1
                for type in ['inclusion_list', 'exclusion_list', 'condition']:
                    if type in trial and trial[type]:
                        if type[0] == 'c':
                            dict_tmp['criteria'].append(
                                {'text': trial[type], 'type': 'c', 'type_id': type_id_cnt, 'c_flag': trial['c_flag']})
                            type_id_cnt += 1
                        elif trial[type][0]:
                            for idx, ii in enumerate(trial[type]):
                                dict_tmp['criteria'].append({'text': ii, 'type': type[0], 'type_id': type_id_cnt,
                                                             f'{type[0]}_flag': trial[f'{type[0]}_flag'][idx]})
                                type_id_cnt += 1
                dict_tmp['criteriaNumber'] = type_id_cnt - 1
                out_json.append(dict_tmp)

        criteria_per_topic = defaultdict(int)
        for i in out_json:
            for qrel in i['qrels'].split(' '):
                criteria_per_topic[qrel[:-2]] += i['criteriaNumber']

        if not criteria_per_topic:
            return
        print('criteria_per_topic', criteria_per_topic)
        print('tot criteria', sum([criteria_per_topic[i] for i in criteria_per_topic]))
        print('min criteria', min([criteria_per_topic[i] for i in criteria_per_topic]))
        print('max criteria', max([criteria_per_topic[i] for i in criteria_per_topic]))

        json_object = json.dumps(out_json, indent=2)
        with open(self.out_path, "w") as outfile:
            outfile.write(json_object)


class Anno_refine(Anno):
    def __init__(self):
        self.refine_anno_doc()

    def refine_anno_doc(self):
        self.path_to_file = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21_umls'
        self.path_to_qrels = '../../data/TRECCT2021/trec-ct2021-qrels.txt'
        self.out_path = 'utils_parse_cfg/anno/ct21_judged_4.json'
        self.out_path_done = 'utils_parse_cfg/anno/ct21_judged_done.json'
        self.path_to_local_exists = ['utils_parse_cfg/anno/ct21_judged_lv1.0.json',
                                     'utils_parse_cfg/anno/ct21_judged_lv2.json',
                                     'utils_parse_cfg/anno/ct21_judged_lv3.json',
                                     'utils_parse_cfg/anno/ct21_judged_lv4.json',
                                     'utils_parse_cfg/anno/ct21_judged_lv5.json']

        self.remove_doc = set()
        self.read_exist()
        self.readfile()
        self.prepare_document()
        self.write_out()
        self.write_done_file()

    def write_done_file(self):
        out_json = []
        qid2docNum = {}
        for d in self.local_json_done:
            doc = self.local_json_done[d]
            out_json.append(doc)
            for q in doc['qrels'].split(' '):
                qid = q.split('_')[0]
                if qid not in qid2docNum:
                    qid2docNum[qid] = 0
                qid2docNum[qid] += 1

        print('check total qid-doc pair', sum([v for k, v in qid2docNum.items()]), 75 * 20)
        print('check insufficient qid', [(k, v) for k, v in qid2docNum.items() if v < 20])
        json_object = json.dumps(out_json, indent=2)
        with open(self.out_path_done, "w") as outfile:
            outfile.write(json_object)

    def read_exist(self):
        local_json_done = {}
        for idx, path_to_file in enumerate(self.path_to_local_exists):
            with open(path_to_file, 'r') as j:
                local_json = json.loads(j.read())
            for doc in local_json:
                self.remove_doc.add(doc['number'])
                if doc['valid']:
                    if doc['number'] in local_json_done:
                        raise ValueError("repeat doc {}".format(doc['number']))
                    local_json_done[doc['number']] = doc
        qid2docNum = {}
        for d in local_json_done:
            doc = local_json_done[d]
            for q in doc['qrels'].split(' '):
                if q not in qid2docNum:
                    qid2docNum[q] = 0
                qid2docNum[q] += 1

        self.qid2docNum = qid2docNum
        self.local_json_done = local_json_done

    def prepare_document(self):
        self.doc2qid = defaultdict(set)
        for qid in self.qrels:
            for i, suffix in enumerate(['_u', '_n', '_p']):
                if qid + suffix in self.qid2docNum:
                    sample_num = 10 - self.qid2docNum[qid + suffix] if i == 2 else 5 - self.qid2docNum[qid + suffix]
                    if qid + suffix == '21_n':
                        a = 1
                    if len(set(self.PosNegDoc[qid][i]).difference(self.remove_doc)) < sample_num:
                        chosed = copy.deepcopy(
                            list(set(self.PosNegDoc[qid][i]).difference(set(self.local_json_done.keys()))))
                        left_num = sample_num - len(chosed)
                        chosed += random.choices(list(set(self.PosNegDoc[qid][0]).difference(self.remove_doc)),
                                                 k=left_num)
                    else:
                        chosed = random.choices(list(set(self.PosNegDoc[qid][i]).difference(self.remove_doc)),
                                                k=sample_num)
                    if len(chosed) != 0:
                        print(qid + suffix, self.qid2docNum[qid + suffix], len(chosed))
                        for doc in chosed:
                            if doc in set(self.local_json_done.keys()):
                                raise ValueError("repeat doc {} in qid {}".format(doc, qid + suffix))
                            self.doc2qid[doc].add(qid + suffix)


def change_sequence_id_and_flag():
    path_to_file = 'utils_parse_cfg/parsed_data/clean_data_cfg_ct21_umls'
    path_to_all_doc = 'utils_parse_cfg/anno/ct21_judged_done.json'
    out_path = 'utils_parse_cfg/anno/docuemtn-entities.json'
    out_path_assignment = 'utils_parse_cfg/anno/query-assignments.json'

    trials = pickle.load(open(path_to_file, 'rb'))
    with open(path_to_all_doc, 'r') as j:
        alldoc = json.loads(j.read())

    alldoc_dict = {}
    for doc in alldoc:
        alldoc_dict[doc['number']] = doc

    assignment = [{"QID": "1", "NCTID": []}]
    for idx, trial in enumerate(tqdm(trials)):
        if trial['number'] in alldoc_dict:
            doc = alldoc_dict[trial['number']]
            cnt = 0
            re_cnt = 1
            for type in ['inclusion_list', 'exclusion_list', 'condition']:
                if type in trial and trial[type]:
                    if type[0] != 'c':
                        for idx_c, ii in enumerate(trial[type]):
                            if doc['criteria'][cnt]['text'] != ii:
                                raise ValueError("data is different {}", trial)
                            if len(trial[type[0] + '_umls'][idx_c]) < 3 and (
                                    'speak' in ii.lower() or 'willing' in ii.lower() or 'protocol' in ii.lower() or
                                    'telephone' in ii.lower() or 'language' in ii.lower()) and \
                                    trial[type[0] + '_flag'][idx_c] != 0:
                                doc['criteria'][cnt][type[0] + '_flag'] = 0
                            if ii == 'none' or ii == 'no' or not ii:
                                doc['criteria'][cnt][type[0] + '_flag'] = 0
                            if doc['criteria'][cnt][type[0] + '_flag'] != 0:
                                doc['criteria'][cnt]['type_id'] = re_cnt
                                re_cnt += 1
                            else:
                                doc['criteria'][cnt]['type_id'] = 0
                            del doc['criteria'][cnt][type[0] + '_flag']
                            cnt += 1
                    else:  # condition
                        doc['criteria'][cnt]['type_id'] = re_cnt
                        del doc['criteria'][cnt][type[0] + '_flag']
                        re_cnt += 1
                        cnt += 1
            doc['criteriaNumber'] = re_cnt - 1
            if '1' == doc['qrels'].split('_')[0]:
                assignment[0]['NCTID'].append(trial['number'])
            # del doc['qrels']
            # del doc['valid']
    # create assignment
    json_object = json.dumps(assignment, indent=2)
    with open(out_path_assignment, "w") as outfile:
        outfile.write(json_object)

    # statistic
    qid2docNum = {}
    for d, doc in alldoc_dict.items():
        for q in doc['qrels'].split(' '):
            qid = q.split('_')[0]
            if qid not in qid2docNum:
                qid2docNum[qid] = [0, 0]  # doc, criteria
            qid2docNum[qid][0] += 1
            qid2docNum[qid][1] += doc['criteriaNumber']

    stat_list = [0] * 75
    for qid, v in qid2docNum.items():
        stat_list[int(qid) - 1] = v[1]

    print('min', min(stat_list))
    print('max', max(stat_list))
    print('tot', sum(stat_list))
    print('avg', sum(stat_list) / len(stat_list))

    out_json = [v for k, v in alldoc_dict.items()]
    json_object = json.dumps(out_json, indent=2)
    with open(out_path, "w") as outfile:
        outfile.write(json_object)


def create_query_json():
    path_to_file = '../../data/TRECCT2021/topics2021.xml'
    topics = rf.read_topics_ct21(path_to_file)
    out = [{'id': k, 'text': v} for k, v in topics.items()]
    json_object = json.dumps(out, indent=2)
    with open('utils_parse_cfg/anno/queries.json', "w") as outfile:
        outfile.write(json_object)


if __name__ == '__main__':
    # write_query_to_sql()
    # write_document_criteria_to_sql()
    # Anno_refine()
    change_sequence_id_and_flag()
    # create_query_json()
