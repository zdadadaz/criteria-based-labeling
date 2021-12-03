from quickumls import QuickUMLS

def argmax(lst):
    return max(range(len(lst)), key=lst.__getitem__)

class Quickumls:
    def __init__(self):
        quickumls_fp = '../umls/quickumls_prog/'
        similarity_name = 'cosine'
        overlapping_criteria = 'score'
        path_to_stypeset = './utils_parse_cfg/data/chosen_semantic_type_set'
        stypeset = set([i.strip().replace('\n', '').split('|')[1] for i in open(path_to_stypeset, 'r') if len(i)>1])
        threshold = 0.9
        self.matcher = QuickUMLS(quickumls_fp, overlapping_criteria=overlapping_criteria, threshold=threshold,
                            similarity_name=similarity_name, accepted_semtypes=stypeset)
        self.class_split = {}
        cur = 0
        for i in open(path_to_stypeset, 'r'):
            if len(i)<2:
                cur += 1
                continue
            self.class_split[i.strip().replace('\n', '').split('|')[1]] = cur
    # output: 1,2,3 : disease, treatment, lab
    def run(self, query):
        out = set()
        res = self.matcher.match(query, best_match=True, ignore_syntax=False)
        count_class = [0]*3
        for terms in res:
            for term in terms:
                name = term['ngram']
                cui = term['cui']
                cterm = term['term'].lower()
                semtype = list(term['semtypes'])
                for s in semtype:
                    if s in self.class_split:
                        count_class[self.class_split[s]] += 1
                comb_name = cterm.strip().replace('\n','') + ' ' + cui
                out.add(comb_name)
        maxclass = argmax(count_class)
        return list(out), maxclass+1


if __name__ == '__main__':
    Quickumls()

