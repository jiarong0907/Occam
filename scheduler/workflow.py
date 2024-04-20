import random
from tools.util import gen_regex, AccType, Status
from typing import List, Set


class WfObj:
    def __init__(self, regex: str, duration: int = 0, atype: AccType = AccType.READ, arrival_time: float = -1.0):
        self._regex: str = regex
        self._atype: AccType = atype
        self._duration: float = duration
        self._arrival_time: float = arrival_time


class Workflow:
    def __init__(self, name: str):
        self._name = name
        self._objs: List[WfObj] = []
        # self.gen_objs()
        self._cur_obj = -1
        self._exlock: List[TreeObjRegex] = []
        self._shlock: List[TreeObjRegex] = []
        self._ishlock: List[TreeObjRegex] = []
        self._iexlock: List[TreeObjRegex] = []
        self._dep_wfs: Set[Workflow] = set()
        self._dp_valid: bool = False
        self._status = Status.PENDING

    def reset(self):
        self._cur_obj = -1
        self._exlock: List[TreeObjRegex] = []
        self._shlock: List[TreeObjRegex] = []
        self._ishlock: List[TreeObjRegex] = []
        self._iexlock: List[TreeObjRegex] = []
        self._dep_wfs: Set[Workflow] = set()
        self._status = Status.PENDING
        self._dp_valid: bool = False

    def get_cur_obj(self) -> WfObj:
        return self._objs[self._cur_obj]

    def runnable(self) -> bool:
        return len(self._ishlock) == 0 and len(self._iexlock) == 0

    def is_last_obj(self) -> bool:
        return self._cur_obj == len(self._objs) - 1

    def add_obj(self, obj: WfObj) -> None:
        self._objs.append(obj)

    def gen_objs(self) -> None:
        num_objs = random.randint(1, 5)
        for _ in range(num_objs):
            regex = gen_regex()
            duration = random.randint(1, 5)
            atype = AccType.READ if random.randint(1, 2) == 1 else AccType.WRITE
            obj = WfObj(regex, duration, atype)
            self._objs.append(obj)
