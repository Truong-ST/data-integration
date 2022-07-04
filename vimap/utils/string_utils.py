from typing import Text, Optional, Tuple, List, Dict
import re
import jellyfish
import datetime
import json
from abc import ABC, abstractmethod


def no_accent_vietnamese(s):
    s = re.sub('[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub('[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
    s = re.sub('[èéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub('[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
    s = re.sub('[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub('[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
    s = re.sub('[ìíịỉĩ]', 'i', s)
    s = re.sub('[ÌÍỊỈĨ]', 'I', s)
    s = re.sub('[ùúụủũưừứựửữ]', 'u', s)
    s = re.sub('[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
    s = re.sub('[ỳýỵỷỹ]', 'y', s)
    s = re.sub('[ỲÝỴỶỸ]', 'Y', s)
    s = re.sub('Đ', 'D', s)
    s = re.sub('đ', 'd', s)
    return s


def levenshtein_score(s1: str, s2: str):
    return 1 - float(jellyfish.levenshtein_distance(s1, s2)/max(len(s1), len(s2)))


def string_simulate(s1: str, s2: str, no_accent_ratio=0.3):
    raw_score = levenshtein_score(s1, s2)
    no_accent_score = levenshtein_score(no_accent_vietnamese(s1), no_accent_vietnamese(s2))
    score = (1 - no_accent_ratio) * raw_score + no_accent_ratio * no_accent_score
    return score


class StringMapper(ABC):
    def __init__(
            self,
            ontology: Dict = None,
            threshold=0.8,
            **kwargs
    ):
        """
        Args:
            ontology: map from synonym value to root value
                eg. ontology = {
                    "root_value": [],
                    ...
                }
            threshold: threshold
            **kwargs:
        """
        self.ontology = ontology
        self.threshold = threshold

    def map(self, value, **kwargs):
        candidates = self.ontology.keys()
        candidate_scores = {
            candidate: string_simulate(value.lower(), candidate.lower()) for candidate in candidates
        }
        candidate_ranking = sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)
        best_syn_val, best_score = candidate_ranking[0]
        if best_score >= self.threshold:
            root_value = self.ontology[best_syn_val]
            score = best_score
        else:
            root_value = None
            score = 0.0

        return root_value, score

    def load(self, ontology_path, **kwargs):
        ontology = dict()

        if ontology_path.endswith('.json'):
            with open(ontology_path, 'r') as pf:
                data = json.load(pf)

            for root_value in data:
                for value in data[root_value]:
                    ontology[value] = root_value
        else:
            print(
                f"[Warning] `ontology_path`:{ontology_path} not exists or wrong format. \n"
                f"Extension format must be .json. \n"
                f"Return `ontology=dict()`. \n"
            )

        self.ontology = ontology
