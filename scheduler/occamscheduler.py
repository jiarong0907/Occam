from __future__ import annotations
import time
from typing import TYPE_CHECKING

import functools
from heapq import heappush
from regex.greenery.fsm import fsm
from regex.regextool import RegexTool
from typing import List, Set
from abc import ABCMeta, abstractmethod

from regex.regextree import TreeObjRegex, logger
from .workflow import *
from .events import *
from tools.util import *
from .sanity_check import check_regextree, check_workflow
from .deadlock import *
from .scheduler import Scheduler

if TYPE_CHECKING:
    from scheduler.runner import Runner


class OccamScheduler(Scheduler):
    __metaclass__ = ABCMeta

    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)

    @abstractmethod
    def get_candidate(self, super_read_wf, write_wfs):
        pass

    def reset_depset(self):
        for wf in self.wf_list_running + self.wf_list_pending:
            wf._dp_valid = False

    def handle_EvWfArrival(
        self, ev_queue: list, ev_id: int, ev_time: float, event: EvWfArrival, task_to_metadata: dict
    ):
        self.records.append("EvWfArrival: ev_time = {}, wf_name = {}\n".format(ev_time, event._wf._name))
        logger.debug(
            "ev_time = {}, ev_id = {}, event = {}, wf_name = {}".format(
                ev_time, ev_id, "handle_EvWfArrival", event._wf._name
            )
        )

        wf: Workflow = event._wf
        # insert the first obj into the objtree
        wf._cur_obj = 0
        wfobj = wf.get_cur_obj()
        wfobj._arrival_time = ev_time
        start = time.time()
        self.objtree.insert(self.objtree._root, TreeObjRegex(wfobj._regex, self.using_trace, self.runner), wf)
        end = time.time()
        if task_to_metadata:
            task_to_metadata[event._wf._name]["insert_time"] += int((end - start) * 1000000)

        if task_to_metadata:
            task_to_metadata[wf._name]["schedule_time"] += self.lock_delay * 6

        self.wf_list_pending.append(wf)
        start = time.time()
        self.schedule(ev_queue=ev_queue, ev_time=ev_time)
        end = time.time()
        self.pending_q_len.append((ev_time, len(self.wf_list_pending)))
        self.active_netobj.append((ev_time, len(self.objtree.get_all_children(self.objtree._root))))
        if task_to_metadata:
            task_to_metadata[event._wf._name]["schedule_time"] += int((end - start) * 1000000)

    def handle_EvWfCompletion(
        self, ev_queue: list, ev_id: int, ev_time: float, event: EvWfCompletion, task_to_metadata: dict
    ):
        self.records.append("EvWfCompletion: ev_time = {}, wf_name = {}\n".format(ev_time, event._wf._name))
        logger.debug(
            "ev_time = {}, ev_id = {}, event = {}, wf_name = {}".format(
                ev_time, ev_id, "handle_EvWfCompletion", event._wf._name
            )
        )

        # step 1: remove the allocated edges and the obj if no wf waiting for it
        start = time.time()
        self.objtree.delete(event._wf)
        end = time.time()
        if task_to_metadata:
            task_to_metadata[event._wf._name]["delete_time"] += int((end - start) * 1000000)
        if task_to_metadata:
            task_to_metadata[event._wf._name]["schedule_time"] += self.lock_delay * 6

        # step 2: remove the wf from the active list
        self.wf_list_running.remove(event._wf)
        self.wf_list_complete.append(event._wf)
        # step 3: record completion time
        if task_to_metadata:
            task_to_metadata[event._wf._name]["finish_time"] = ev_time

        # step 4: reschedule active workflows
        start = time.time()
        self.schedule(ev_queue=ev_queue, ev_time=ev_time)
        end = time.time()
        self.pending_q_len.append((ev_time, len(self.wf_list_pending)))
        self.active_netobj.append((ev_time, len(self.objtree.get_all_children(self.objtree._root))))
        if task_to_metadata:
            task_to_metadata[event._wf._name]["schedule_time"] += int((end - start) * 1000000)

    def handle_EvObjStart(self, ev_queue: list, ev_id: int, ev_time: float, event: EvObjStart, task_to_metadata: dict):
        self.records.append(
            "EvObjStart: ev_time = {}, wf_name = {}, obj_id = {}\n".format(ev_time, event._wf._name, event._wf._cur_obj)
        )
        logger.debug(
            "ev_time = {}, ev_id = {}, event = {}, wf_name = {}, obj_id = {}".format(
                ev_time, ev_id, "handle_EvObjStart", event._wf_name, event._wf._cur_obj
            )
        )
        wf = event._wf
        wfobj = wf.get_cur_obj()
        # insert the EvObjEnd event
        heappush(
            ev_queue,
            (
                ev_time + wfobj._duration,
                alloc_event_id(),
                EvObjEnd(ev_time=ev_time + wfobj._duration, wf=wf, runner=self.runner),
            ),
        )
        if wf._cur_obj == 0 and task_to_metadata:
            task_to_metadata[wf._name]["actual_start_time"] = ev_time

    def handle_EvObjEnd(self, ev_queue: list, ev_id: int, ev_time: float, event: EvObjEnd, task_to_metadata: dict):
        self.records.append(
            "EvObjEnd: ev_time = {}, wf_name = {}, obj_id = {}\n".format(ev_time, event._wf._name, event._wf._cur_obj)
        )
        logger.debug(
            "ev_time = {}, ev_id = {}, event = {}, wf_name = {}, obj_id = {}".format(
                ev_time, ev_id, "handle_EvObjEnd", event._wf_name, event._wf._cur_obj
            )
        )
        wf = event._wf
        # we move to the next obj if this is not the last one
        if not wf.is_last_obj():
            assert 0
            wf._cur_obj += 1
            wfobj = wf.get_cur_obj()
            wfobj._arrival_time = ev_time
            wfobj_fsm: fsm = RegexTool.to_fsm(wfobj._regex)

            # check with allocated locks
            flag_all_get = False
            if wfobj._atype == AccType.READ:
                # ask for read, but the lock has been granted as read or write
                for obj in wf._shlock + wf._exlock:
                    if RegexTool.contain_regex(obj._fsm, wfobj_fsm):
                        flag_all_get = True
                        break
                    wfobj_fsm = RegexTool.subtract_regex(wfobj_fsm, obj._fsm)
            elif wfobj._atype == AccType.WRITE:
                # ask for read, but part of the lock has been granted as write
                for obj in wf._exlock:
                    if RegexTool.contain_regex(obj._fsm, wfobj_fsm):
                        flag_all_get = True
                        break
                    wfobj_fsm = RegexTool.subtract_regex(wfobj_fsm, obj._fsm)
            else:
                raise Exception("Undefined access type!")

            if not flag_all_get:
                # still ask for lock, update the regex
                self.objtree.insert(
                    self.objtree._root, TreeObjRegex(RegexTool.to_regex(wfobj_fsm), self.using_trace, self.runner), wf
                )
            # set the status to pending before scheduling the next obj
            wf._status = Status.PENDING
            self.wf_list_running.remove(wf)
            self.wf_list_pending.append(wf)
            self.schedule(ev_queue=ev_queue, ev_time=ev_time)
        # This is the last obj in this workflow, we need to insert a flow completion event
        else:
            heappush(ev_queue, (ev_time, alloc_event_id(), EvWfCompletion(ev_time=ev_time, wf=wf, runner=self.runner)))

    def allocate_shlock(self, obj: TreeObjRegex):
        """grant all read look for the obj"""
        for wf in obj._ishlock:
            wf._shlock.append(obj)
            wf._ishlock.remove(obj)
        obj._shlock = obj._shlock + obj._ishlock
        obj._ishlock.clear()

    def show_scheduling_info(self, wf: Workflow):
        print("=" * 40, "scheduling info", "=" * 40)
        print("wf = ", wf._name)
        print("objid = ", wf._cur_obj)
        print("iexlock:")
        for obj in wf._iexlock:
            print(obj._regex)
        print("ishlock:")
        for obj in wf._ishlock:
            print(obj._regex)
        print("exlock:")
        for obj in wf._exlock:
            print(obj._regex)
        print("shlock:")
        for obj in wf._shlock:
            print(obj._regex)

    def upgrade_to_exlock_children(self, obj: TreeObjRegex, wf: Workflow) -> None:
        child_objs: List[TreeObjRegex] = self.objtree.get_all_children(obj)
        for co in child_objs:
            if len(co._shlock) > 0:
                assert len(co._shlock) == 1
                assert co._shlock[0] == wf
                wf._shlock.remove(co)
                co._shlock.remove(wf)
                self.objtree.delete_obj_if_possible(obj)
            elif len(co._exlock) > 0:
                assert len(co._exlock) == 1
                assert co._exlock[0] == wf
                wf._exlock.remove(co)
                co._exlock.remove(wf)
                self.objtree.delete_obj_if_possible(obj)

    def get_depedent_wfs(self, objtree, wf: Workflow) -> Set[Workflow]:
        if wf._dp_valid:
            return wf._dep_wfs

        dep_wfs = set()
        dep_wfs.add(wf)

        for obj in wf._exlock + wf._shlock:
            contain_objs = objtree.get_containment(objtree._root, obj, proper=False)
            for co in contain_objs:
                for waited_wf in co._iexlock + co._ishlock:
                    if waited_wf != wf:
                        try:
                            dep_wfs.update(self.get_depedent_wfs(objtree, waited_wf))
                        except RecursionError:
                            logger.debug("----- deadlock detected -----")
                            raise DeadlockException(waited_wf)
        wf._dp_valid = True
        wf._dep_wfs = dep_wfs
        return dep_wfs

    def schedule(self, ev_queue: list, ev_time: float) -> None:
        reschedule = False
        has_deadlock = False

        # logger.debug(self.objtree.show())

        all_objs = self.objtree.get_all_children(self.objtree._root)
        for obj in all_objs:
            # delete the obj if no wfs ask for or hold it
            if self.objtree.delete_obj_if_possible(obj):
                logger.debug("delete {}".format(obj._regex))
                continue

            # only one type of lock is allowed.
            assert not (len(obj._shlock) > 0 and len(obj._exlock) > 0)

            # case 1: The obj is locked by shlock and there are objs asking for shlock
            if len(obj._shlock) > 0 and len(obj._ishlock) > 0:
                logger.debug("scheduling--case 1")
                assert not has_lock_in_containment(self.objtree, obj, AccType.WRITE, proper=False)
                self.allocate_shlock(obj)

            # case 2: The obj has no lock, but its containment has shlock
            if (
                len(obj._shlock) == 0
                and len(obj._exlock) == 0
                and len(obj._ishlock) > 0
                and has_lock_in_containment(self.objtree, obj, AccType.READ, proper=True)
            ):
                logger.debug("scheduling--case 2")
                # case 2-1: there is a shlock in the path, so it's safe to get the shlock
                if has_lock_in_path_to_root(self.objtree, obj, AccType.READ, proper=True):
                    self.allocate_shlock(obj)
                # case 2-2: otherwise, the shlock is in the children of obj
                else:
                    # case 2-2-1: there is no write lock in the children, it's safe to get the share look
                    if not has_lock_in_children(self.objtree, obj, AccType.WRITE):
                        self.allocate_shlock(obj)
                    # case 2-2-2: we need to do split. obj1==subobj_has_shlock, obj2=obj-obj1
                    else:
                        # we don't allow this because different the shlock will be granted eventually
                        # when the exlock in the children is released
                        # if this shlock --> exlock upgrade, we do allow in the following, otherwise there will be a deadlock
                        pass

            # case 3: The obj has no lock and its containment has no lock
            if (
                len(obj._exlock) == 0
                and len(obj._shlock) == 0
                and not has_lock_in_containment(self.objtree, obj, AccType.RW, proper=False)
            ):
                # and not has_lock_in_containment(self.objtree, obj, AccType.READ, proper=False):
                logger.debug("scheduling--case 3")
                assert len(obj._iexlock) != 0 or len(obj._ishlock) != 0
                contain_objs: List[TreeObjRegex] = self.objtree.get_containment(self.objtree._root, obj, proper=False)
                write_wfs: Set[Workflow] = set()
                read_wfs: Set[Workflow] = set()
                for co in contain_objs:
                    for wf in co._iexlock:
                        # filter out wfs that cannot get the lock
                        # we consider only wfs that has no other read or write locks in thier containment
                        if not has_lock_in_containment(self.objtree, co, AccType.RW, proper=False):
                            # if not has_lock_in_containment(self.objtree, co, AccType.READ, proper=False) \
                            #         and not has_lock_in_containment(self.objtree, co, AccType.WRITE, proper=False):
                            write_wfs.add(wf)
                    for wf in co._ishlock:
                        # filter out wfs that cannot get the lock
                        # we consider only wfs that has no other write locks in thier containment
                        if not has_lock_in_containment(self.objtree, co, AccType.WRITE, proper=False):
                            read_wfs.add(wf)

                super_read_wf = None
                for wf in read_wfs:
                    if not super_read_wf:
                        super_read_wf = Workflow(name="super")
                        # this regex can be any; it has no use.
                        super_read_wf.add_obj(WfObj("pod[1-8]dc1", 1, AccType.READ))
                        super_read_wf._cur_obj = 0
                        super_read_wf.get_cur_obj()._arrival_time = float("inf")
                    if super_read_wf.get_cur_obj()._arrival_time > wf.get_cur_obj()._arrival_time:
                        super_read_wf.get_cur_obj()._arrival_time = wf.get_cur_obj()._arrival_time

                # optimization
                if len(write_wfs) == 0 and len(read_wfs) > 0:
                    sched_wf = super_read_wf
                elif len(write_wfs) == 1 and len(read_wfs) == 0:
                    sched_wf = list(write_wfs)[0]
                else:
                    # reset the dp cache flag
                    self.reset_depset()
                    try:
                        for wf in read_wfs:
                            wf._dep_wfs = self.get_depedent_wfs(self.objtree, wf)
                            super_read_wf._dep_wfs.update(wf._dep_wfs)

                        for wf in write_wfs:
                            wf._dep_wfs = self.get_depedent_wfs(self.objtree, wf)
                    except DeadlockException as e:
                        deadlockrollback(
                            e._rb_wf, self.objtree, self.wf_list_pending, ev_queue, ev_time, self.records, self.runner
                        )
                        has_deadlock = True

                    if has_deadlock:
                        break
                    logger.debug("schedule: get candidate " + str(obj._regex) + " at " + str(ev_time))
                    sched_wf = self.get_candidate(super_read_wf, write_wfs)

                logger.debug("time= {}, sched_wf={}".format(ev_time, sched_wf._name))

                # try from earliest to latest until find one can get the lock
                # TODO: it seems that we don't sort? because we have filtered out impossible cases
                if sched_wf == super_read_wf:  # ask for read lock
                    # for all wfs in read_wfs, if they ask for a shlock of an obj in the containment, grant it
                    for wf in read_wfs:
                        remove = []
                        for ro in wf._ishlock:
                            if ro in contain_objs:
                                ro._ishlock.remove(wf)
                                ro._shlock.append(wf)
                                remove.append(ro)
                        for r in remove:
                            wf._ishlock.remove(r)
                            wf._shlock.append(r)
                else:  # ask for write lock
                    remove = []
                    # for this wf, if it asks for an exlock of an obj in the containment, grant it
                    for wo in sched_wf._iexlock:
                        if wo in contain_objs:
                            wo._iexlock.remove(sched_wf)
                            wo._exlock.append(sched_wf)
                            remove.append(wo)
                    for r in remove:
                        sched_wf._iexlock.remove(r)
                        sched_wf._exlock.append(r)
                    #       obj1
                    #       /  \
                    #     obj2 obj3
                    # in the above case, if obj1 releases a lock and obj2 get the lock, obj3 can still run, so we need to reschedule.
                    # TODO: this can be optimized maybe
                    reschedule = False

            # case 4: for shlock --> exlock of the same object
            if len(obj._shlock) == 1 and len(obj._iexlock) > 0:
                logger.debug("scheduling--case 4")
                this_wf = obj._shlock[0]
                # in the path and children there is only this wf, so it's safe to upgrade
                if only_wf_in_path(self.objtree, obj, this_wf) and only_wf_in_children(self.objtree, obj, this_wf):
                    remove = []
                    if this_wf in obj._iexlock:
                        remove.append(this_wf)
                        this_wf._iexlock.remove(obj)
                        obj._shlock.remove(this_wf)
                        this_wf._shlock.remove(obj)
                        obj._exlock.append(this_wf)
                        this_wf._exlock.append(obj)
                    for r in remove:
                        obj._iexlock.remove(r)

            # case 5: for shlock --> exlock upgrade in the same wf.
            # Let's consider a simpler implementation: The upgrade must not trigger split.
            # If the iexlock is in the path to the root, it needs to wait for other wfs to release their locks in its containment.
            # If the iexlock is in the children, the regex will be extended to the shlock's regex.
            if (
                len(obj._exlock) == 0
                and len(obj._shlock) == 0
                and len(obj._iexlock) > 0
                and has_lock_in_containment(self.objtree, obj, AccType.READ, proper=False)
            ):
                logger.debug("scheduling--case 5")
                remove = []
                contain_wfs = get_wfs_in_containment(self.objtree, obj, AccType.READ, proper=False)
                for wf in obj._iexlock:
                    # make sure the wf waited for exlock has got shlock
                    if wf not in contain_wfs:
                        continue
                    logger.debug("scheduling--case 5 hit")
                    # there is a path and there is only wf in the path
                    if only_wf_in_path(self.objtree, obj, wf) and obj not in self.objtree._root.get_children():
                        path_objs: List[TreeObjRegex] = self.objtree.find_path(self.objtree._root, obj)
                        path_objs.remove(obj)
                        target_obj = None
                        for ancestor in path_objs:
                            if wf in ancestor._shlock:
                                target_obj = ancestor
                                break
                        assert target_obj
                        self.upgrade_to_exlock_children(target_obj, wf)
                        wf._exlock.append(target_obj)
                        wf._iexlock.remove(obj)
                        target_obj._exlock.append(wf)
                        remove.append(wf)
                        target_obj._shlock.remove(wf)
                        wf._shlock.remove(target_obj)
                    # the obj has children and only wf in the children
                    elif only_wf_in_children(self.objtree, obj, wf) and self.objtree.get_all_children(obj):
                        # TODO: does this cause any problems? e.g., the program asks 0-5 exlock but given 0-10 exlock
                        # upgrade all locks in the children to the exlock of this obj
                        self.upgrade_to_exlock_children(obj, wf)
                        wf._exlock.append(obj)
                        wf._iexlock.remove(obj)
                        obj._exlock.append(wf)
                        remove.append(wf)

                for r in remove:
                    obj._iexlock.remove(r)

        # scan the wf_list to excute runnable workflows
        # TODO: This can also be done by check and run the workflow when changing the edge above?
        removed_wf = []
        for wf in self.wf_list_pending:
            if wf.runnable():
                wf._status = Status.RUNNING
                removed_wf.append(wf)
                heappush(ev_queue, (ev_time, alloc_event_id(), EvObjStart(ev_time=ev_time, wf=wf, runner=self.runner)))
        for wf in removed_wf:
            self.wf_list_pending.remove(wf)
            self.wf_list_running.append(wf)

        # deadlock check
        if len(self.wf_list_running) == 0 and len(self.wf_list_pending) > 0 and not has_deadlock:
            has_deadlock = True
            deadlockrollback(
                self.wf_list_pending[0],
                self.objtree,
                self.wf_list_pending,
                ev_queue,
                ev_time,
                self.records,
                self.runner,
            )

        if reschedule or has_deadlock:
            self.schedule(ev_queue=ev_queue, ev_time=ev_time)

        if self.enable_sanity_check:
            check_regextree(self.objtree)
            check_workflow(self.wf_list_pending + self.wf_list_running)

        # objtree.show()
        # for w in wf_list:
        #     show_scheduling_info(w)


class OccamDepSetScheduler(OccamScheduler):
    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)

    def get_candidate(self, super_read_wf, write_wfs):
        # def wf_comp(item1:Workflow, item2:Workflow):
        #     '''sort by the arrival time
        #     '''
        #     if len(item1._dep_wfs) > len(item2._dep_wfs):
        #         return 1
        #     elif len(item1._dep_wfs) < len(item2._dep_wfs):
        #         return -1
        #     else:
        #         if item1.get_cur_obj()._arrival_time < item2.get_cur_obj()._arrival_time:
        #             return 1
        #         elif item1.get_cur_obj()._arrival_time > item2.get_cur_obj()._arrival_time:
        #             return -1
        #         else:
        #             return 0

        sched_wf = super_read_wf
        max_depset: int = -1 if not super_read_wf else len(super_read_wf._dep_wfs)
        earliest_arrival: float = float("inf") if not super_read_wf else super_read_wf.get_cur_obj()._arrival_time
        for wf in write_wfs:
            logger.debug(
                "candidate: wf.name={}, depset={}, max_depset={}".format(wf._name, len(wf._dep_wfs), max_depset)
            )
            logger.debug(
                "candidate: wf.name={}, arrival={}, earliest_arrival={}".format(
                    wf._name, wf.get_cur_obj()._arrival_time, earliest_arrival
                )
            )
            if len(wf._dep_wfs) > max_depset:
                max_depset = len(wf._dep_wfs)
                earliest_arrival = wf.get_cur_obj()._arrival_time
                sched_wf = wf
            elif len(wf._dep_wfs) == max_depset and wf.get_cur_obj()._arrival_time < earliest_arrival:
                max_depset = len(wf._dep_wfs)
                earliest_arrival = wf.get_cur_obj()._arrival_time
                sched_wf = wf
        return sched_wf

        # candidates:Set[Workflow] = set()
        # if super_read_wf:
        #     candidates.add(super_read_wf)
        # candidates.update(write_wfs)
        # sorted_candidates = sorted(candidates, key=functools.cmp_to_key(wf_comp), reverse=True)
        # sn = [wf._name for wf in sorted_candidates]
        # sd = [len(wf._dep_wfs) for wf in sorted_candidates]
        # print(sn)
        # print(sd)
        # return sorted_candidates


class OccamFIFOScheduler(OccamScheduler):
    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)

    def get_candidate(self, super_read_wf, write_wfs):
        # def wf_comp(item1:Workflow, item2:Workflow):
        #     '''sort by the arrival time
        #     '''
        #     if item1.get_cur_obj()._arrival_time > item2.get_cur_obj()._arrival_time:
        #         return 1
        #     elif item1.get_cur_obj()._arrival_time == item2.get_cur_obj()._arrival_time:
        #         return 0
        #     else:
        #         return -1

        # candidates:Set[Workflow] = set()
        # if super_read_wf:
        #     candidates.add(super_read_wf)
        # candidates.update(write_wfs)
        # sorted_candidates = sorted(candidates, key=functools.cmp_to_key(wf_comp))
        # return sorted_candidates

        sched_wf = super_read_wf
        earliest_arrival: float = float("inf") if not super_read_wf else super_read_wf.get_cur_obj()._arrival_time
        for wf in write_wfs:
            if wf.get_cur_obj()._arrival_time < earliest_arrival:
                earliest_arrival = wf.get_cur_obj()._arrival_time
                sched_wf = wf
        return sched_wf
