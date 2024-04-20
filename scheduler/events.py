from __future__ import annotations
from typing import TYPE_CHECKING

from abc import ABCMeta, abstractmethod

if TYPE_CHECKING:
    from .workflow import Workflow


class SimEvent:
    """Base class of events, which is going to be queued in event queue.

    Attributes:
        ev_type (str): Explicitly illustrates the event's type
        ev_time (float64): Time of event occurence
    """

    __metaclass__ = ABCMeta

    def __init__(self, ev_type: str, ev_time: float, wf: Workflow, runner):
        """Constructor of Event class. Will be overriden by child classes."""
        self._ev_type: str = ev_type
        self._ev_time: float = ev_time
        self._wf: Workflow = wf
        self._wf_name: str = wf._name
        self._num_objs: int = len(wf._objs)
        self._runner = runner

    def __str__(self):
        # Header line shows event type
        ret = "Event type: %s\n" % (self._ev_type)
        ret += "    Event time: %.6f\n" % (self._ev_time)

        attrs = [
            attr for attr in dir(self) if not attr.startswith("__") and not attr == "_ev_type" and not attr == "ev_time"
        ]
        # Print attribute name and value line by line
        for attr in attrs:
            ret += "    %s: %s\n" % (attr, getattr(self, attr))
        return ret

    def __repr__(self):
        return str(self)

    @abstractmethod
    def call_handler(self, ev_queue: list, ev_id: int, ev_time: float, task_to_metadata: dict):
        pass


class EvWfArrival(SimEvent):
    """Event that signals the arrival of a workflow"""

    def __init__(self, ev_time: float, wf: Workflow, runner):
        SimEvent.__init__(self, ev_type="EvWfArrival", ev_time=ev_time, wf=wf, runner=runner)

    def __str__(self):
        ret = "\n    Event type: %s\n" % (self._ev_type)
        ret += "    Event time: %.6f\n" % (self._ev_time)
        ret += "    Workflow name: %s\n" % (self._wf_name)
        ret += "    # objects: %d\n" % (self._num_objs)
        return ret

    def call_handler(self, ev_queue: list, ev_id: int, ev_time: float, task_to_metadata: dict):
        self._runner.scheduler.handle_EvWfArrival(ev_queue, ev_id, ev_time, self, task_to_metadata)


class EvWfCompletion(SimEvent):
    """Event that signals the completion of a workflow"""

    def __init__(self, ev_time: float, wf: Workflow, runner):
        SimEvent.__init__(self, ev_type="EvWfCompletion", ev_time=ev_time, wf=wf, runner=runner)

    def __str__(self):
        ret = "\n    Event type: %s\n" % (self._ev_type)
        ret += "    Event time: %.6f\n" % (self._ev_time)
        ret += "    Workflow name: %s\n" % (self._wf_name)
        return ret

    def call_handler(self, ev_queue: list, ev_id: int, ev_time: float, task_to_metadata: dict):
        self._runner.scheduler.handle_EvWfCompletion(ev_queue, ev_id, ev_time, self, task_to_metadata)


class EvObjStart(SimEvent):
    """Event that signals the start of an obj"""

    def __init__(self, ev_time: float, wf: Workflow, runner):
        SimEvent.__init__(self, ev_type="EvObjStart", ev_time=ev_time, wf=wf, runner=runner)

    def call_handler(self, ev_queue: list, ev_id: int, ev_time: float, task_to_metadata: dict):
        self._runner.scheduler.handle_EvObjStart(ev_queue, ev_id, ev_time, self, task_to_metadata)


class EvObjEnd(SimEvent):
    """Event that signals the completion of an obj"""

    def __init__(self, ev_time: float, wf: Workflow, runner):
        SimEvent.__init__(self, ev_type="EvObjEnd", ev_time=ev_time, wf=wf, runner=runner)

    def call_handler(self, ev_queue: list, ev_id: int, ev_time: float, task_to_metadata: dict):
        self._runner.scheduler.handle_EvObjEnd(ev_queue, ev_id, ev_time, self, task_to_metadata)
