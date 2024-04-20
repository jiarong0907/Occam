from __future__ import annotations
from tools.util import get_matched_dcs, get_matched_devices
from typing import TYPE_CHECKING

import re

from regex.regextool import *
from scheduler.workflow import Workflow, AccType
from tools.mylogging import logger

if TYPE_CHECKING:
    from scheduler.runner import Runner
    from typing import List, Set


class TreeObjRegex:
    def __init__(self, regex: str, using_trace: bool, runner: Runner):
        self._regex = regex
        self._runner: Runner = runner
        # logger.debug('The input regex is {}'.format(self._regex))
        if self._runner and self._regex in self._runner.fsm_cache:
            self._fsm = self._runner.fsm_cache[self._regex].copy()
            # self._dev:Set = set(self._runner.reg2dev_map[self._regex])
        else:
            self._fsm = RegexTool.to_fsm(self._regex)
            # self._dev:Set = set()
        self._dev: Set = set(get_matched_devices(self._runner, self._regex))
        self._exlock: List[Workflow] = []
        self._shlock: List[Workflow] = []
        self._ishlock: List[Workflow] = []
        self._iexlock: List[Workflow] = []
        self._children_obj = []
        self._lo: str = ""
        self._hi: str = ""
        self._using_trace = using_trace
        self.set_bound()

    def set_fsm(self, fsm):
        self._regex = RegexTool.to_regex(fsm)
        self._fsm = fsm
        # if self._runner and self._runner.use_regextree_dev_opt and self._regex in self._runner.fsm_cache:
        #     self._dev:Set = set(self._runner.reg2dev_map[self._regex])
        # else:
        #     self._dev:Set = set()
        self._dev: Set = set(get_matched_devices(self._runner, self._regex))
        self.set_bound()

    def set_bound_workload(self):
        """
        set_bound for the real workload. Get all matched devices from the device list
        and get their dc names.
        """
        logger.debug("original_str = {}".format(self._regex))
        if self._regex == ".*":
            # fake dc names that cover all real dc names
            self._lo = "_dc0000_"
            self._hi = "_dc9999_"
            return

        matched_dcs = get_matched_dcs(self._runner, self._regex)
        # TODO: handle this, the intersection does not match any device
        if not matched_dcs:
            # assert 0
            self._lo = "_dc0000_"
            self._hi = "_dc0000_"
        else:
            matched_dcs = sorted(matched_dcs)
            self._lo = matched_dcs[0]
            self._hi = matched_dcs[-1]
        logger.debug("self._lo = {}, self._hi = {}".format(self._lo, self._hi))

    def set_bound(self):
        """
        extra the dc part of the regex, generate all accepted strings,
        and select the smallest and highest ones.
        """
        if self._using_trace:
            self.set_bound_workload()
            return

        logger.debug("original_str = {}".format(self._regex))

        if self._regex == ".*":
            self._lo = "dc1"
            self._hi = "dc99999"
            return

        # TODO: more efficient way?
        # TODO: how to optimize core switch
        min_dc = None
        max_dc = None
        for s in self._fsm.strings():
            s_str = "".join(s)
            dc_str = s_str[s_str.find("dc") :]
            if not min_dc:
                min_dc = dc_str
                max_dc = dc_str
                continue

            if min_dc > dc_str:
                min_dc = dc_str
            if max_dc < dc_str:
                max_dc = dc_str
        assert min_dc != None and max_dc != None
        self._lo = min_dc
        self._hi = max_dc
        logger.debug("self._lo = {}, self._hi = {}".format(self._lo, self._hi))

    def get_lo(self) -> str:
        return self._lo

    def get_hi(self) -> str:
        return self._hi

    def insert_child(self, index, obj):
        self._children_obj.insert(index, obj)

    def append_child(self, obj):
        self._children_obj.append(obj)

    def add_child(self, obj):
        if not self._children_obj:
            self.insert_child(0, obj)
            return

        for i in range(len(self._children_obj)):
            child = self._children_obj[i]
            if obj._lo < child._lo or (obj._lo == child._lo and obj._hi <= child._hi):
                self._children_obj.insert(i, obj)
                return

        self._children_obj.append(obj)

    def del_child(self, obj):
        return self._children_obj.remove(obj)

    def get_children(self):
        return self._children_obj

    def has_child(self, obj) -> bool:
        return obj in self._children_obj


class RegexTreeRegex:
    def __init__(self, using_trace: bool, runner: Runner):
        self._root = TreeObjRegex(".*", using_trace, runner)
        self._using_trace = using_trace
        self._runner = runner

    def get_all_children(self, root: TreeObjRegex) -> List[TreeObjRegex]:
        objs = []
        objs += root.get_children()

        for child in root.get_children():
            objs += self.get_all_children(child)
        return objs

    def _find_path(self, root: TreeObjRegex, obj: TreeObjRegex):
        if root == obj:
            return [root]

        if len(root.get_children()) == 0:
            return []

        for child in root.get_children():
            res = self._find_path(child, obj)
            if res:
                return [root] + res
        return []

    def find_path(self, root: TreeObjRegex, obj: TreeObjRegex):
        path = self._find_path(root, obj)
        # remove root
        path.remove(root)
        return path

    def get_containment(self, root: TreeObjRegex, obj: TreeObjRegex, proper: bool):
        path = self.find_path(root, obj)
        if proper:
            path.remove(obj)
        return path + self.get_all_children(obj)

    def insert_req_edge(self, obj: TreeObjRegex, wf: Workflow):
        if wf is None:
            return
        atype = wf.get_cur_obj()._atype
        if atype == AccType.READ:
            assert (wf not in obj._ishlock) and (obj not in wf._ishlock)
            obj._ishlock.append(wf)
            wf._ishlock.append(obj)
        elif atype == AccType.WRITE:
            assert wf not in obj._iexlock
            assert obj not in wf._iexlock
            obj._iexlock.append(wf)
            wf._iexlock.append(obj)
        else:
            raise Exception("Wrong access type!")

    def insert(self, root: TreeObjRegex, obj: TreeObjRegex, wf: Workflow):
        # has no child, insert at the front
        if not root.get_children():
            logger.debug("No child")
            root.insert_child(0, obj)
            self.insert_req_edge(obj, wf)
            return

        # less than all children, insert at the front
        if obj.get_hi() < root.get_children()[0].get_lo():
            logger.debug("less than all")
            root.insert_child(0, obj)
            self.insert_req_edge(obj, wf)
            return

        num_child = len(root.get_children())
        idx = 0
        flag_untouched = True
        contains = []
        overlaps = []
        commons = []

        # Stage 1: classify the relation of children in the current layer
        while idx < num_child:
            child: TreeObjRegex = root.get_children()[idx]
            logger.debug(
                "{} child={}, obj={}, idx={}, num_child={}".format(
                    "inter while,", child._regex, obj._regex, idx, num_child
                )
            )

            if child._lo > obj._hi:
                break

            # the inserted obj contains existing objects
            # we regard obj == child as obj contains child
            # if RegexTool.contain_regex(obj._fsm, child._fsm):
            if RegexTool.contain_regex_opt(obj, child, self._runner):
                logger.debug("{} child = {} obj = {}".format("insert: obj contains child", child._regex, obj._regex))
                flag_untouched = False
                contains.append(child)

            # an existing obj contains the inserted one, going into the next layer
            # elif RegexTool.contain_proper_regex(child._fsm, obj._fsm):
            elif RegexTool.contain_proper_regex_opt(child, obj, self._runner):
                logger.debug("{} child = {} obj = {}".format("insert: child contains obj", child._regex, obj._regex))
                flag_untouched = False
                if not child.get_children():
                    child.insert_child(0, obj)
                    self.insert_req_edge(obj, wf)
                else:
                    self.insert(child, obj, wf)
                break

            # overlapping
            # elif RegexTool.overlap_regex(obj._fsm, child._fsm):
            elif RegexTool.overlap_regex_opt(obj, child, self._runner):
                logger.debug("{} child = {} obj = {}".format("insert: overlapping", child._regex, obj._regex))
                flag_untouched = False
                overlaps.append(child)
            idx += 1

        if flag_untouched:
            logger.debug("insert untouch {}".format(obj._regex))
            root.append_child(obj)
            self.insert_req_edge(obj, wf)
            self.sort_layer(root)
            return

        if overlaps or contains:
            # Stage 2: partition for the overlapping child
            flag_remaining = True
            for ch in overlaps:
                logger.debug("{} obj = {} ch = {}".format("insert: iterate over overlaps, ", obj._regex, ch._regex))
                # after partitioning, the remaining obj == ch
                # if RegexTool.equal_regex(obj._fsm, ch._fsm):
                if RegexTool.equal_regex_opt(obj, ch, self._runner):
                    logger.debug("in equal_regex")
                    logger.debug(
                        "{} obj = {} ch = {}".format("partition overlapping: obj is equal to ch", obj._regex, ch._regex)
                    )
                    flag_remaining = False
                    obj.append_child(ch)
                    self.insert_req_edge(obj, wf)
                    # we should stop here because the rest cannot have overlapping with the current obj
                    overlaps = overlaps[: len(commons)]
                    break
                # after partitioning, the remaining obj is a subset of ch
                # elif RegexTool.contain_regex(ch._fsm, obj._fsm):
                elif RegexTool.contain_regex_opt(ch, obj, self._runner):
                    logger.debug("in contain_regex")
                    logger.debug(
                        "{} obj = {} ch = {}".format(
                            "partition overlapping: ch now contains obj", obj._regex, ch._regex
                        )
                    )
                    flag_remaining = False
                    if not ch.get_children():
                        ch.insert_child(0, obj)
                        self.insert_req_edge(obj, wf)
                    else:
                        self.insert(ch, obj, wf)
                    # we should stop here because the rest cannot have overlapping with the current obj
                    overlaps = overlaps[: len(commons)]
                    break
                intersec, obj_diff, ch_diff = RegexTool.intersec_regex(obj._fsm, ch._fsm)
                intersec_obj = TreeObjRegex(RegexTool.to_regex(intersec), self._using_trace, self._runner)
                ch.set_fsm(ch_diff)
                obj.set_fsm(obj_diff)
                commons.append(intersec_obj)

            # Stage 3: rebuild children for overlapping
            assert len(overlaps) == len(commons)
            for i in range(len(overlaps)):
                self.rebuild_edge(overlaps[i], commons[i])
                self.rebuild_child(commons[i], overlaps[i])

            # stage 4: update the child in this layer
            for i in range(len(contains)):
                obj.append_child(contains[i])
                root.del_child(contains[i])

            for i in range(len(commons)):
                root.append_child(commons[i])
                self.insert_req_edge(commons[i], wf)

            if flag_remaining:
                root.append_child(obj)
                self.insert_req_edge(obj, wf)

        self.sort_layer(root)
        self.sort_layer(obj)

    def rebuild_edge(self, child: TreeObjRegex, common: TreeObjRegex):
        """
        Rebuild the pos and req edges. When an obj is split, its edges also change.
        """
        for wf in child._exlock:
            wf._exlock.append(common)
            common._exlock.append(wf)

        for wf in child._shlock:
            wf._shlock.append(common)
            common._shlock.append(wf)

        for wf in child._iexlock:
            wf._iexlock.append(common)
            common._iexlock.append(wf)

        for wf in child._ishlock:
            wf._ishlock.append(common)
            common._ishlock.append(wf)

    def rebuild_child(self, intersec: TreeObjRegex, child: TreeObjRegex):
        """
        Rebuild the tree for overlapping. Child has wrong children now that is going to be adjusted.
        """
        logger.debug(
            "{} intersec = {}, child = {}".format("rebuild_child: intersec, child: ", intersec._regex, child._regex)
        )
        i = 0
        child_num = len(child.get_children())
        while i < child_num:
            ch: TreeObjRegex = child.get_children()[i]
            logger.debug("{} i = {} child_num = {}".format("rebuild_child: ", i, child_num))
            i += 1

            # The child is still contained by the node; do nothing.
            # if RegexTool.contain_regex(child._fsm, ch._fsm):
            if RegexTool.contain_regex_opt(child, ch, self._runner):
                logger.debug(
                    "{} child = {} intersec = {} ch = {}".format(
                        "rebuild_child: child contains", child._regex, intersec._regex, ch._regex
                    )
                )
                pass
            # The child is now contained by the intersec; move the obj to intersec.
            # elif RegexTool.contain_regex(intersec._fsm, ch._fsm):
            elif RegexTool.contain_regex_opt(intersec, ch, self._runner):
                logger.debug(
                    "{} child = {} intersec = {} ch = {}".format(
                        "rebuild_child: intersec contains", child._regex, intersec._regex, ch._regex
                    )
                )
                intersec.append_child(ch)
                child.del_child(ch)
                child_num -= 1
                i -= 1
            # overlap with intersec and child
            else:
                logger.debug(
                    "{} child = {} intersec = {} ch = {}".format(
                        "rebuild_child: overlap", child._regex, intersec._regex, ch._regex
                    )
                )

                insec_intersec, _, _ = RegexTool.intersec_regex(ch._fsm, intersec._fsm)
                insec_child, _, _ = RegexTool.intersec_regex(ch._fsm, child._fsm)

                logger.debug(
                    "{} child_part = {} common_part = {} ".format(
                        "rebuild_child:", RegexTool.to_regex(insec_child), RegexTool.to_regex(insec_intersec)
                    )
                )

                insec_intersec_obj = TreeObjRegex(RegexTool.to_regex(insec_intersec), self._using_trace, self._runner)
                self.rebuild_edge(ch, insec_intersec_obj)
                ch.set_fsm(insec_child)
                intersec.append_child(insec_intersec_obj)
                # rebuild the subtree recursively
                self.rebuild_child(insec_intersec_obj, ch)
        self.sort_layer(intersec)
        self.sort_layer(child)

    def sort_layer(self, root: TreeObjRegex):
        def dc_cmp(item: TreeObjRegex):
            return (item.get_lo(), item.get_hi())

        root._children_obj = sorted(root._children_obj, key=dc_cmp)

    def delete_obj_if_possible(self, obj: TreeObjRegex) -> bool:
        if len(obj._ishlock) == 0 and len(obj._iexlock) == 0 and len(obj._shlock) == 0 and len(obj._exlock) == 0:
            self.delete_obj(obj)
            return True
        return False

    def delete_obj(self, obj: TreeObjRegex):
        parent = self.find_parent(self._root, obj)
        if parent:
            for child in obj.get_children():
                parent.append_child(child)
            parent.del_child(obj)
            self.sort_layer(parent)
        else:
            self.show()
            raise Exception("Cannot find the parent in the regextree!")

    # TODO: add a parent field to avoid this
    def find_parent(self, root: TreeObjRegex, obj: TreeObjRegex):
        if root.has_child(obj):
            return root

        for child in root.get_children():
            p = self.find_parent(child, obj)
            if p:
                return p
        return None

    def delete(self, wf: Workflow) -> None:
        # TODO: a workflow asks for the same obj
        # self.show(self._root)
        for obj in wf._shlock:
            obj._shlock.remove(wf)
            if len(obj._ishlock) == 0 and len(obj._iexlock) == 0 and len(obj._shlock) == 0:
                self.delete_obj(obj)
        for obj in wf._exlock:
            obj._exlock.remove(wf)
            if len(obj._ishlock) == 0 and len(obj._iexlock) == 0:
                self.delete_obj(obj)

    def _show(self, root: TreeObjRegex, indent="", res=""):
        res += indent + str(root._regex) + "\n"
        indent += "   "
        for child in root.get_children():
            res += self._show(child, indent)
        return res

    def show(self, indent="", res=""):
        return self._show(self._root, indent, res)
