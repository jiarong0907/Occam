from __future__ import annotations
from typing import TYPE_CHECKING

from regex.regextool import RegexTool
from scheduler.workflow import Status
from tools.util import *
from regex.greenery.fsm import fsm

if TYPE_CHECKING:
    from scheduler.workflow import Workflow
    from regex.regextree import RegexTreeRegex, TreeObjRegex


def check_workflow(wf_list: List[Workflow]):
    # no duplicate wfs
    if len(wf_list) != len(set(wf_list)):
        raise Exception("There are duplicated workflows.")

    for wf in wf_list:
        # only wfs that get all locks can run
        if wf._status == Status.RUNNING and (wf._iexlock or wf._ishlock):
            raise Exception("Only wfs that get all locks can run.")

        # only wfs that hasn't got all locks can pend
        if wf._status == Status.PENDING and not wf._iexlock and not wf._ishlock and (wf._exlock or wf._shlock):
            raise Exception("Only wfs that that has not got all locks can pend.")

        # wf lock is consistent to the obj lock
        for obj in wf._ishlock:
            if wf not in obj._ishlock:
                raise Exception("The ishlock is in consistent.")

        for obj in wf._iexlock:
            if wf not in obj._iexlock:
                raise Exception("The iexlock is in consistent.")

        for obj in wf._shlock:
            if wf not in obj._shlock:
                raise Exception("The shlock is in consistent.")

        for obj in wf._exlock:
            if wf not in obj._exlock:
                raise Exception("The exlock is in consistent.")

        # wf fsm is consistent to the obj fsm
        obj_fsm: fsm = None
        for obj in wf._ishlock + wf._iexlock + wf._shlock + wf._exlock:
            if not obj_fsm:
                obj_fsm = obj._fsm
            else:
                obj_fsm = obj_fsm.union(obj._fsm)

        wf_fsm: fsm = None
        for i in range(wf._cur_obj + 1):
            wfobj = wf._objs[i]
            if not wf_fsm:
                wf_fsm = RegexTool.to_fsm(wfobj._regex)
            else:
                wf_fsm = wf_fsm.union(RegexTool.to_fsm(wfobj._regex))

        if obj_fsm != wf_fsm:
            raise Exception("The fsms are inconsistent.")


def check_regextree(objtree: RegexTreeRegex):
    all_objs = objtree.get_all_children(objtree._root)
    for obj in all_objs:
        check_single_node(obj)
        check_containment(objtree, obj)


def check_single_node(obj: TreeObjRegex):
    # only has shlock or exlock
    if obj._shlock and obj._exlock:
        raise Exception("One obj can only has either shlock or exlock.")

    # only one obj can get exlock
    if obj._exlock and len(obj._exlock) != 1:
        raise Exception("only one obj can get exlock.")

    # there is no duplicated wfs in the list
    if len(obj._shlock) != len(set(obj._shlock)):
        raise Exception("There are duplicate wfs in the shlock list.")

    if len(obj._exlock) != len(set(obj._exlock)):
        raise Exception("There are duplicate wfs in the exlock list.")

    if len(obj._ishlock) != len(set(obj._ishlock)):
        raise Exception("There are duplicate wfs in the ishlock list.")

    if len(obj._iexlock) != len(set(obj._iexlock)):
        raise Exception("There are duplicate wfs in the iexlock list.")

    # one wf can appear only at exlock or shlock or iexlock or ishlock or shlock and iexlock
    if len(obj._shlock + obj._exlock) != len(set(obj._shlock + obj._exlock)):
        raise Exception("There are duplicate wfs in the shlock and exlock list.")

    if len(obj._ishlock + obj._iexlock) != len(set(obj._ishlock + obj._iexlock)):
        raise Exception("There are duplicate wfs in the ishlock and iexlock list.")

    if (
        set(obj._shlock) & set(obj._exlock)
        or set(obj._ishlock) & set(obj._iexlock)
        or set(obj._ishlock) & set(obj._shlock)
        or set(obj._iexlock) & set(obj._exlock)
        or set(obj._ishlock) & set(obj._exlock)
    ):
        raise Exception("There are duplicate wfs getting read and writing locks.")


def check_containment(objtree: RegexTreeRegex, obj: TreeObjRegex):
    if not obj._exlock and not obj._shlock:
        return
    # has only shlock or exlock in the containment
    if has_lock_in_containment(objtree, obj, AccType.WRITE, proper=False) and has_lock_in_containment(
        objtree, obj, AccType.READ, proper=False
    ):
        raise Exception("The containment has both read and writing locks.")

    # only one exlock in the path to root
    if num_lock_in_path(objtree, obj, AccType.WRITE, proper=False) > 1:
        raise Exception("The path to root has more than one writing locks.")
