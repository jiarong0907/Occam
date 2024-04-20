from heapq import heappush
from tools.util import *
from .events import EvWfArrival
from regex.regextree import RegexTreeRegex
from .workflow import Workflow


class DeadlockException(Exception):
    """Exception raised for deadlock in scheduling.

    Attributes:
        rb_wf: The workflow that needs to rollback
    """

    def __init__(self, rb_wf, message="deadlock exception"):
        self._rb_wf = rb_wf
        self._message = "Deadlock casued by " + rb_wf._name
        super().__init__(self._message)


def deadlockrollback(rb_wf: Workflow, objtree: RegexTreeRegex, wf_list, ev_queue, ev_time, records: List[str], runner):
    for obj in rb_wf._exlock:
        obj._exlock.remove(rb_wf)
        if runner.scheduler.__class__.__name__.startswith("Occam"):
            objtree.delete_obj_if_possible(obj)
        else:
            runner.scheduler.delete_obj_if_possible(obj)
    for obj in rb_wf._shlock:
        obj._shlock.remove(rb_wf)
        if runner.scheduler.__class__.__name__.startswith("Occam"):
            objtree.delete_obj_if_possible(obj)
        else:
            runner.scheduler.delete_obj_if_possible(obj)
    for obj in rb_wf._ishlock:
        obj._ishlock.remove(rb_wf)
        if runner.scheduler.__class__.__name__.startswith("Occam"):
            objtree.delete_obj_if_possible(obj)
        else:
            runner.scheduler.delete_obj_if_possible(obj)
    for obj in rb_wf._iexlock:
        obj._iexlock.remove(rb_wf)
        if runner.scheduler.__class__.__name__.startswith("Occam"):
            objtree.delete_obj_if_possible(obj)
        else:
            runner.scheduler.delete_obj_if_possible(obj)

    rb_wf.reset()

    wf_list.remove(rb_wf)
    heappush(ev_queue, (ev_time, alloc_event_id(), EvWfArrival(ev_time=ev_time, wf=rb_wf, runner=runner)))
    records.append("Deadlock: ev_time = {}, wf_name = {}\n".format(ev_time, rb_wf._name))

    # rb_wf._cur_obj = 0
    # wfobj = rb_wf.get_cur_obj()
    # objtree.insert(objtree._root, TreeObjRegex(wfobj._regex), rb_wf)
