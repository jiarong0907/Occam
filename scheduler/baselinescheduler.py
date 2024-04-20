from __future__ import annotations
import time
from typing import TYPE_CHECKING

from heapq import heappush, heappop
import re
from typing import Dict, List
from abc import ABCMeta, abstractmethod

from regex.regextool import RegexTool
from .workflow import *
from .events import *
from .scheduler import *
from tools.mylogging import logger

if TYPE_CHECKING:
    from scheduler.runner import Runner


class NetObj:
    """The granularity of the lock. Could be device or dc"""

    def __init__(self, name: str):
        self._name = name
        self._exlock: List[Workflow] = []
        self._shlock: List[Workflow] = []
        self._ishlock: List[Workflow] = []
        self._iexlock: List[Workflow] = []


class BaselineScheduler(Scheduler):
    __metaclass__ = ABCMeta

    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)
        self._netobj_dict: Dict[str, NetObj] = dict()
        self._opt_level = 1

    @abstractmethod
    def get_netobj_from_regex(self, regex: str):
        """implemented by subclass to get devices or dcs"""
        pass

    def delete_obj_if_possible(self, netobj):
        if (
            len(netobj._ishlock) == 0
            and len(netobj._iexlock) == 0
            and len(netobj._shlock) == 0
            and len(netobj._exlock) == 0
        ):
            if netobj._name in self._netobj_dict:
                self._netobj_dict.pop(netobj._name)

    def add_edges(self, netobj: NetObj, wf: Workflow, wfobj: WfObj):
        if wfobj._atype == AccType.READ:
            if not netobj._exlock:
                netobj._shlock.append(wf)
                wf._shlock.append(netobj)
            else:
                netobj._ishlock.append(wf)
                wf._ishlock.append(netobj)
        elif wfobj._atype == AccType.WRITE:
            if not netobj._exlock and not netobj._shlock:
                netobj._exlock.append(wf)
                wf._exlock.append(netobj)
            else:
                netobj._iexlock.append(wf)
                wf._iexlock.append(netobj)
        else:
            raise Exception("Undefined access type!")

    def acquire_lock(self, wf: Workflow, task_to_metadata: dict):
        wfobj: WfObj = wf.get_cur_obj()
        regex = wfobj._regex
        netobjs = self.get_netobj_from_regex(regex)
        assert netobjs
        for netobj_name in netobjs:
            # this is a device never used before
            if netobj_name not in self._netobj_dict:
                new_device = NetObj(netobj_name)
                self._netobj_dict[netobj_name] = new_device
                self.add_edges(new_device, wf, wfobj)
            else:
                exist_device = self._netobj_dict[netobj_name]
                assert not exist_device._exlock or not exist_device._shlock
                self.add_edges(exist_device, wf, wfobj)

        if task_to_metadata:
            task_to_metadata[wf._name]["schedule_time"] += self.lock_delay * 2 * len(netobjs) * 2

    def release_lock(self, wf: Workflow, task_to_metadata: dict):
        for netobj in wf._shlock:
            netobj._shlock.remove(wf)
        for netobj in wf._exlock:
            netobj._exlock.remove(wf)

        if task_to_metadata:
            task_to_metadata[wf._name]["schedule_time"] += self.lock_delay * len(wf._shlock + wf._exlock)

        for netobj in wf._shlock + wf._exlock:
            remove = []
            if (
                len(netobj._ishlock) == 0
                and len(netobj._iexlock) == 0
                and len(netobj._shlock) == 0
                and len(netobj._exlock) == 0
            ):
                if netobj._name in self._netobj_dict:
                    remove.append(netobj._name)
            for r in remove:
                self._netobj_dict.pop(r)

    @abstractmethod
    def get_candidate(self, netobj: NetObj) -> Workflow:
        """return the next scheduled wf. Implemented by different policies."""
        pass

    def reset_depset(self):
        for wf in self.wf_list_running + self.wf_list_pending:
            wf._dp_valid = False

    def schedule(self, ev_queue: list, ev_time: float) -> None:
        # reset the dp cache flag
        # self.reset_depset()
        alloc_dict = dict()
        has_deadlock = False
        logger.debug("schedule: {} netobjs to schedule".format(len(self._netobj_dict.values())))
        for netobj in self._netobj_dict.values():
            if len(netobj._shlock) > 0 and len(netobj._ishlock) > 0:
                # get the shlock immediately
                logger.debug("schedule: get shlock immedately")
                for wf in netobj._ishlock:
                    wf._shlock.append(netobj)
                    wf._ishlock.remove(netobj)
                netobj._shlock = netobj._shlock + netobj._ishlock
                netobj._ishlock.clear()

            if len(netobj._exlock) == 0 and len(netobj._shlock) == 0:
                assert len(netobj._iexlock) != 0 or len(netobj._ishlock) != 0
                logger.debug("schedule: get candidate " + str(netobj._name) + " at " + str(ev_time))
                try:
                    sched_wf = self.get_candidate(netobj)
                except DeadlockException as e:
                    deadlockrollback(
                        e._rb_wf, self.objtree, self.wf_list_pending, ev_queue, ev_time, self.records, self.runner
                    )
                    has_deadlock = True
                if has_deadlock:
                    break
                assert sched_wf
                # This is an optimization: when scheduling, we assume the dep set does not change.
                # It is used to avoid the case that 2 same wfs asking for 10k devices. When the 2 wfs have the same
                # dep set at the beginning, one is selected and its dep set +1. For the next device, we need to anaylze
                # one more device when computing its dep set. Evevtually, we need to compute 1+2+3+4+....+10k.
                # This optimization makes it to compute only 10k.
                # TODO: But this might not select the wf with largest dep set. For example, wf1.depset=3. wf2.depset=2.
                # They both get devices in the scheduling. For a certain device, when using regular method, wf2 could be
                # selected if its depset become larger than wf1 in this process. But when using the optimization, wf1 is selected.
                if self._opt_level >= 2 and (
                    self.__class__.__name__ == "PerDeviceDepSetScheduler"
                    or self.__class__.__name__ == "PerDcDepSetScheduler"
                ):
                    alloc_wfs = []
                    if sched_wf.get_cur_obj()._atype == AccType.READ:
                        for wf in netobj._ishlock:
                            alloc_wfs.append(wf)
                    else:
                        alloc_wfs.append(sched_wf)
                    alloc_dict[netobj] = alloc_wfs
                else:
                    if sched_wf.get_cur_obj()._atype == AccType.READ:
                        for wf in netobj._ishlock:
                            wf._ishlock.remove(netobj)
                            wf._shlock.append(netobj)
                        netobj._shlock = netobj._shlock + netobj._ishlock
                        netobj._ishlock.clear()
                    else:
                        netobj._iexlock.remove(sched_wf)
                        netobj._exlock.append(sched_wf)
                        sched_wf._iexlock.remove(netobj)
                        sched_wf._exlock.append(netobj)

            # exlock auto get exlock
            if len(netobj._exlock) > 0 and len(netobj._iexlock) > 0:
                removed = []
                for wf in netobj._iexlock:
                    if wf in netobj._exlock:
                        removed.append(wf)
                for wf in removed:
                    netobj._iexlock.remove(wf)
                    wf._iexlock.remove(netobj)
                if removed:
                    logger.debug("schedule: exlock auto get exlock")

            # exlock auto get shlock
            if len(netobj._exlock) > 0 and len(netobj._ishlock) > 0:
                removed = []
                for wf in netobj._ishlock:
                    if wf in netobj._exlock:
                        removed.append(wf)
                for wf in removed:
                    netobj._ishlock.remove(wf)
                    wf._ishlock.remove(netobj)
                if removed:
                    logger.debug("schedule: exlock auto get shlock")

            # shlock upgrade to exlock
            if len(netobj._shlock) > 0 and len(netobj._iexlock) > 0:
                removed = []
                for wf in netobj._iexlock:
                    if wf in netobj._shlock:
                        removed.append(wf)
                for wf in removed:
                    if len(netobj._shlock) == 1:
                        netobj._shlock.remove(wf)
                        wf._shlock.remove(netobj)

                        netobj._iexlock.remove(wf)
                        wf._iexlock.remove(netobj)
                        netobj._exlock.append(wf)
                        wf._exlock.append(netobj)
                if removed:
                    logger.debug("schedule: shlock upgrade to exlock")

        # To allocate the device at once
        if self._opt_level >= 2 and (
            self.__class__.__name__ == "PerDeviceDepSetScheduler" or self.__class__.__name__ == "PerDcDepSetScheduler"
        ):
            logger.debug("schedule: opt=2, To allocate the device at once")
            for netobj in alloc_dict:
                if alloc_dict[netobj][0].get_cur_obj()._atype == AccType.READ:
                    for wf in alloc_dict[netobj]:
                        wf._ishlock.remove(netobj)
                        wf._shlock.append(netobj)
                    netobj._shlock = netobj._shlock + netobj._ishlock
                    netobj._ishlock.clear()
                else:
                    wf = alloc_dict[netobj][0]
                    netobj._iexlock.remove(wf)
                    netobj._exlock.append(wf)
                    wf._iexlock.remove(netobj)
                    wf._exlock.append(netobj)

        remove_wf = []
        for wf in self.wf_list_pending:
            # self.show_scheduling_info(wf)
            if wf.runnable():
                wf._status = Status.RUNNING
                remove_wf.append(wf)
                heappush(ev_queue, (ev_time, alloc_event_id(), EvObjStart(ev_time=ev_time, wf=wf, runner=self.runner)))
        for wf in remove_wf:
            self.wf_list_pending.remove(wf)
            self.wf_list_running.append(wf)

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

        if has_deadlock:
            self.schedule(ev_queue=ev_queue, ev_time=ev_time)

    def show_scheduling_info(self, wf: Workflow):
        print("=" * 40, "scheduling info", "=" * 40)
        print("wf = ", wf._name)
        print("objid = ", wf._cur_obj)
        print("iexlock:", [obj._name for obj in wf._iexlock])
        print("ishlock:", [obj._name for obj in wf._ishlock])
        print("exlock:", [obj._name for obj in wf._exlock])
        print("shlock:", [obj._name for obj in wf._shlock])

    def handle_EvWfArrival(
        self, ev_queue: list, ev_id: int, ev_time: float, event: EvWfArrival, task_to_metadata: dict
    ):
        self.records.append("EvWfArrival: ev_time = {}, wf_name = {}\n".format(ev_time, event._wf._name))
        logger.debug(
            "ev_time = {}, ev_id = {}, event = {}, wf_name = {}".format(
                ev_time, ev_id, "handle_EvWfArrival", event._wf._name
            )
        )
        start = time.time()
        wf: Workflow = event._wf
        self.wf_list_pending.append(wf)
        # insert the first obj into the objtree
        wf._cur_obj = 0
        wfobj = wf.get_cur_obj()
        wfobj._arrival_time = ev_time
        self.acquire_lock(wf, task_to_metadata)
        # if wf._name.startswith('230644241'):
        #     self.show_scheduling_info(wf)
        #     for d in wf._iexlock:
        #         print(d._name)
        #         print([wf._name for wf in d._exlock])
        #         print([wf._name for wf in d._shlock])
        #         print([wf._name for wf in d._iexlock])
        #         print([wf._name for wf in d._ishlock])
        self.schedule(ev_queue=ev_queue, ev_time=ev_time)
        end = time.time()
        self.pending_q_len.append((ev_time, len(self.wf_list_pending)))
        self.active_netobj.append((ev_time, len(self._netobj_dict)))
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
        start = time.time()
        # step 1: remove the allocated edges and the obj if no wf waiting for it
        self.release_lock(event._wf, task_to_metadata)
        # step 2: remove the wf from the active list
        self.wf_list_running.remove(event._wf)
        self.wf_list_complete.append(event._wf)
        # step 3: record completion time
        if task_to_metadata:
            task_to_metadata[event._wf._name]["finish_time"] = ev_time
        # step 4: reschedule active workflows
        self.schedule(ev_queue=ev_queue, ev_time=ev_time)
        end = time.time()
        self.pending_q_len.append((ev_time, len(self.wf_list_pending)))
        self.active_netobj.append((ev_time, len(self._netobj_dict)))
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
            self.acquire_lock(wf, task_to_metadata)
            # set the status to pending before scheduling the next obj
            wf._status = Status.PENDING
            self.wf_list_running.remove(wf)
            self.wf_list_pending.append(wf)
            self.schedule(ev_queue=ev_queue, ev_time=ev_time)
        # This is the last obj in this workflow, we need to insert a flow completion event
        else:
            heappush(ev_queue, (ev_time, alloc_event_id(), EvWfCompletion(ev_time=ev_time, wf=wf, runner=self.runner)))


class BaselineFIFOScheduler(BaselineScheduler):
    __metaclass__ = ABCMeta

    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)
        self._netobj_dict: Dict[str, NetObj] = dict()

    @abstractmethod
    def get_netobj_from_regex(self, regex: str):
        """implemented by subclass to get devices or dcs"""
        pass

    def get_candidate(self, netobj: NetObj) -> Workflow:
        """return the next scheduled wf. Implemented by different policies."""
        if len(netobj._iexlock) == 0 and len(netobj._ishlock) != 0:
            sched_wf = netobj._ishlock[0]
        elif len(netobj._iexlock) != 0 and len(netobj._ishlock) == 0:
            sched_wf = netobj._iexlock[0]
        elif len(netobj._iexlock) != 0 and len(netobj._ishlock) != 0:
            earliest_read = netobj._ishlock[0]
            earliest_write = netobj._iexlock[0]
            read_arrival = earliest_read.get_cur_obj()._arrival_time
            write_arrival = earliest_write.get_cur_obj()._arrival_time
            sched_wf = earliest_read if read_arrival < write_arrival else earliest_write
        else:
            raise Exception("A device must have an iexlock or ishlock. Otherwise, it should be deleted.")
        return sched_wf


def get_device_from_regex(runner, using_trace, regex: str) -> List[str]:
    if using_trace:
        if regex in runner.reg2dev_map:
            return runner.reg2dev_map[regex]
        else:
            devices = runner.device_cache
            return [d for d in devices if re.match(regex, d)]
    else:
        fsm: fsm = RegexTool.to_fsm(regex)
        res: List[str] = []
        for string in fsm:
            device_name = "".join(string)
            res.append(device_name)
        return res


def get_dc_from_regex(runner, using_trace, regex: str) -> List[str]:
    if using_trace:
        return get_matched_dcs(runner, regex)
    else:
        fsm: fsm = RegexTool.to_fsm(regex)
        res: List[str] = []
        for string in fsm:
            s_str = "".join(string)
            dc_str = s_str[s_str.find("dc") :]
            if dc_str not in res:
                res.append(dc_str)
        return res


class PerDeviceFIFOScheduler(BaselineFIFOScheduler):
    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)

    def get_netobj_from_regex(self, regex: str) -> List[str]:
        return get_device_from_regex(self.runner, self.using_trace, regex)


class PerDcFIFOScheduler(BaselineFIFOScheduler):
    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)

    def get_netobj_from_regex(self, regex: str) -> List[str]:
        return get_dc_from_regex(self.runner, self.using_trace, regex)


class BaselineDepSetScheduler(BaselineScheduler):
    __metaclass__ = ABCMeta

    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)
        self._netobj_dict: Dict[str, NetObj] = dict()

    @abstractmethod
    def get_netobj_from_regex(self, regex: str):
        """implemented by subclass to get devices or dcs"""
        pass

    def get_depedent_wfs(self, wf: Workflow) -> Set[Workflow]:
        # print("get_depedent_wfs", wf._name)
        if wf._dp_valid:
            return wf._dep_wfs
        dep_wfs = set()
        dep_wfs.add(wf)
        # print("wf._exlock + wf._shlock", len(wf._exlock + wf._shlock))
        for netobj in wf._exlock + wf._shlock:  # 14471
            # print("netobj._iexlock + netobj._ishlock", len(netobj._iexlock + netobj._ishlock))
            for waited_wf in netobj._iexlock + netobj._ishlock:  # 4
                # print("waited_wf:", waited_wf._name)
                try:
                    dep_wfs.update(self.get_depedent_wfs(waited_wf))
                except RecursionError:
                    logger.debug("----- deadlock detected -----")
                    raise DeadlockException(waited_wf)
        wf._dp_valid = True
        wf._dep_wfs = dep_wfs
        return dep_wfs

    def get_candidate(self, netobj: NetObj) -> Workflow:
        """Return the next scheduled wf. Implemented by different policies.
        We use dynamic programming to optimize it.
        """
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

        # print("get_condidate start", netobj._name)
        if len(netobj._iexlock) == 0 and len(netobj._ishlock) != 0:
            logger.debug("candidate: netobj._iexlock=0")
            sched_wf = netobj._ishlock[0]
        elif len(netobj._iexlock) == 1 and len(netobj._ishlock) == 0:
            logger.debug("candidate: netobj._iexlock=1")
            sched_wf = netobj._iexlock[0]
        else:
            # reset the dp cache flag
            self.reset_depset()

            logger.debug("candidate: compute depesent set")
            assert not (len(netobj._iexlock) == 0 and len(netobj._ishlock) == 0)
            super_read_wf = None
            for wf in netobj._ishlock:
                if not super_read_wf:
                    super_read_wf = Workflow(name="super")
                    # this regex can be any; it has no use.
                    super_read_wf.add_obj(WfObj("pod[1-8]dc1", 1, AccType.READ))
                    super_read_wf._cur_obj = 0
                    super_read_wf.get_cur_obj()._arrival_time = float("inf")
                wf._dep_wfs = self.get_depedent_wfs(wf)
                super_read_wf._dep_wfs.update(wf._dep_wfs)
                if super_read_wf.get_cur_obj()._arrival_time > wf.get_cur_obj()._arrival_time:
                    super_read_wf.get_cur_obj()._arrival_time = wf.get_cur_obj()._arrival_time

            sched_wf = super_read_wf
            max_depset: int = -1 if not super_read_wf else len(super_read_wf._dep_wfs)
            earliest_arrival: float = float("inf") if not super_read_wf else super_read_wf.get_cur_obj()._arrival_time
            for wf in netobj._iexlock:
                wf._dep_wfs = self.get_depedent_wfs(wf)
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
                    sched_wf = wf
                    earliest_arrival = wf.get_cur_obj()._arrival_time
                elif len(wf._dep_wfs) == max_depset and wf.get_cur_obj()._arrival_time < earliest_arrival:
                    max_depset = len(wf._dep_wfs)
                    earliest_arrival = wf.get_cur_obj()._arrival_time
                    sched_wf = wf

            # candidates:Set[Workflow] = set()
            # if super_read_wf:
            #     candidates.add(super_read_wf)
            # candidates.update(netobj._iexlock)
            # sorted_candidates = sorted(candidates, key=functools.cmp_to_key(wf_comp), reverse=True)
            # return sorted_candidates[0]

        logger.debug("candidate: schedule {}".format(sched_wf._name))
        # print("get_condidate end", netobj._name)
        return sched_wf


class PerDeviceDepSetScheduler(BaselineDepSetScheduler):
    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)

    def get_netobj_from_regex(self, regex: str) -> List[str]:
        return get_device_from_regex(self.runner, self.using_trace, regex)


class PerDcDepSetScheduler(BaselineDepSetScheduler):
    def __init__(self, using_trace=True) -> None:
        super().__init__(using_trace)

    def get_netobj_from_regex(self, regex: str) -> List[str]:
        return get_dc_from_regex(self.runner, self.using_trace, regex)
