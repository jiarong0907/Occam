from __future__ import annotations
import re
from typing import TYPE_CHECKING

import random
import enum
from typing import List


if TYPE_CHECKING:
    from regex.regextree import RegexTreeRegex, TreeObjRegex
    from scheduler.workflow import Workflow

try:
    from enum import auto as enum_auto
except ImportError:
    __acc_type_auto_enum = 0

    def enum_auto() -> int:
        global __acc_type_auto_enum
        i = __acc_type_auto_enum
        __acc_type_auto_enum += 1
        return i


class AccType(enum.Enum):
    READ = enum_auto()
    WRITE = enum_auto()
    RW = enum_auto()


class Status(enum.Enum):
    PENDING = enum_auto()  # one obj is waiting for its locks
    RUNNING = enum_auto()  # one obj is running


EVENT_ID = 0


def alloc_event_id():
    """An ID for all events. Can use it for track the log."""
    global EVENT_ID
    EVENT_ID += 1
    return EVENT_ID


def has_lock_in(objs: List[TreeObjRegex], type: AccType) -> bool:
    for co in objs:
        if type == AccType.READ:
            if len(co._shlock) > 0:
                return True
        elif type == AccType.WRITE:
            if len(co._exlock) > 0:
                return True
        elif type == AccType.RW:
            if len(co._shlock) > 0 or len(co._exlock) > 0:
                return True
    return False


def has_lock_in_containment(objtree: RegexTreeRegex, obj: TreeObjRegex, type: AccType, proper: bool) -> bool:
    """Check whether there is an specific type of lock in the containment"""
    contain_objs: List[TreeObjRegex] = objtree.get_containment(objtree._root, obj, proper=proper)

    return has_lock_in(contain_objs, type)


def get_wfs_in_containment(objtree: RegexTreeRegex, obj: TreeObjRegex, type: AccType, proper: bool) -> list:
    """get all wfs in the containment that have got the lock"""
    contain_objs: List[TreeObjRegex] = objtree.get_containment(objtree._root, obj, proper=proper)
    wfs = set()
    for co in contain_objs:
        if type == AccType.READ:
            wfs.update(set(co._shlock))
        elif type == AccType.WRITE:
            wfs.update(set(co._exlock))
    return list(wfs)


def has_lock_in_path_to_root(objtree: RegexTreeRegex, obj: TreeObjRegex, type: AccType, proper: bool) -> bool:
    """Check whether there is an specific type of lock in the path from root to this obj"""
    path_objs: List[TreeObjRegex] = objtree.find_path(objtree._root, obj)
    if proper:
        path_objs.remove(obj)
    return has_lock_in(path_objs, type)


def has_lock_in_children(objtree: RegexTreeRegex, obj: TreeObjRegex, type: AccType) -> bool:
    """Check whether there is an specific type of lock in all the children this obj"""
    child_objs: List[TreeObjRegex] = objtree.get_all_children(obj)
    return has_lock_in(child_objs, type)


def only_wf_in_children(objtree: RegexTreeRegex, obj: TreeObjRegex, wf: Workflow) -> bool:
    """Check whether all the children has locks only allocated to wf"""
    child_objs: List[TreeObjRegex] = objtree.get_all_children(obj)
    # if not child_objs:
    #     return False
    for co in child_objs:
        for allocated_wf in co._shlock + co._exlock:
            if allocated_wf != wf:
                return False
    return True


def only_wf_in_path(objtree: RegexTreeRegex, obj: TreeObjRegex, wf: Workflow) -> bool:
    """Check whether all objs in the path to the root has locks only allocated to wf"""
    path_objs: List[TreeObjRegex] = objtree.find_path(objtree._root, obj)
    path_objs.remove(obj)
    # if not path_objs:
    #     return False
    for co in path_objs:
        for allocated_wf in co._shlock + co._exlock:
            if allocated_wf != wf:
                return False
    for ancestor in path_objs:
        if wf in ancestor._shlock + ancestor._exlock:
            for child in ancestor.get_children():
                if child not in path_objs:
                    if not only_wf_in_children(objtree, child, wf):
                        return False
    return True


def num_lock_in_containment(objtree: RegexTreeRegex, obj: TreeObjRegex, type: AccType, proper: bool) -> int:
    """Check the number of an specific type of lock in the containment"""
    contain_objs: List[TreeObjRegex] = objtree.get_containment(objtree._root, obj, proper=proper)
    num = 0
    for co in contain_objs:
        if type == AccType.READ and co._shlock:
            num += len(co._shlock)
        elif type == AccType.READ and co._exlock:
            num += 1
        else:
            raise Exception("Undefined scheduling policy!")

    return num


def num_lock_in_path(objtree: RegexTreeRegex, obj: TreeObjRegex, type: AccType, proper: bool) -> int:
    """Check the number of an specific type of lock in the path to the root"""
    path_objs: List[TreeObjRegex] = objtree.find_path(objtree._root, obj)
    num = 0
    for co in path_objs:
        if type == AccType.READ and co._shlock:
            num += len(co._shlock)
        elif type == AccType.WRITE and co._exlock:
            num += 1

    return num


def get_matched_devices(runner, regex: str):
    if runner and regex in runner.reg2dev_map:
        all_match = runner.reg2dev_map[regex]
    else:
        if runner:
            devices = runner.device_cache
        else:
            devices = open("workload/devices.txt", "r").read().splitlines()
        # takes a lot of time
        all_match = [d for d in devices if re.match(regex, d)]
    return all_match


def get_matched_dcs(runner, regex: str):
    # optimize the performance
    matched_dcs = set()
    if runner and regex in runner.reg2dev_map:  # using the fast path
        return runner.reg2dc_map[regex]

        dc_substr = re.findall(r"(_dc\d{4}_\d+|_dc\d{4}_\\d\+)", regex)
        assert dc_substr
        if len(dc_substr) == 1 and re.match(r"_dc\d{4}_\d+", dc_substr[0]):
            matched_dcs.add(dc_substr[0])
        else:
            dcs = runner.dc_cache
            for dc_s in dc_substr:
                all_match = [d for d in dcs if re.match(dc_s, d)]
                matched_dcs.update(set(all_match))
        return list(matched_dcs)
    else:
        all_match = get_matched_devices(runner, regex)
        for am in all_match:
            dc = re.search("(_dc\\d{4}_\\d+|_dc\\d{4}_[a-z]+)", am)
            if dc:
                matched_dcs.add(dc[0])
            else:
                assert 0
        return list(matched_dcs)


def gen_range(low, high):
    return "[" + str(low) + "-" + str(high) + "]" if low < high else str(low)


def gen_regex():
    type_num = random.randint(1, 3)
    res = ""
    low = random.randint(0, 9)
    high = random.randint(low, 9)
    if type_num == 1:
        res += "tor" + gen_range(low, high)
    elif type_num == 2:
        res += "agg" + gen_range(low, high)
    else:
        res += "core" + gen_range(low, high)
    if type_num == 1 or type_num == 2:
        low = random.randint(0, 9)
        high = random.randint(low, 9)
        res += "pod" + gen_range(low, high)
    low = random.randint(0, 9)
    high = random.randint(low, 9)
    res += "dc" + gen_range(low, high)
    return res
