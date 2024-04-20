from __future__ import annotations
from typing import TYPE_CHECKING, Tuple

from typing import List, Tuple
from abc import ABCMeta, abstractmethod

from regex.regextree import RegexTreeRegex
from .workflow import *
from .events import *
from tools.util import *
from .deadlock import *

if TYPE_CHECKING:
    from scheduler.runner import Runner


class Scheduler:
    __metaclass__ = ABCMeta

    def __init__(self, using_trace=True) -> None:
        # All active workflows. A workflow will be added when it arrives and removed when it completes
        self.wf_list_running: List[Workflow] = []
        self.wf_list_pending: List[Workflow] = []
        self.wf_list_complete: List[Workflow] = []
        # The log of the scheduling. Can use it for testing and drawing figures
        self.records: List[str] = []
        self.pending_q_len: List[Tuple[float, int]] = []
        self.active_netobj: List[Tuple[float, int]] = []
        self.using_trace = using_trace
        self.enable_sanity_check = False
        self.lock_delay = 10  # us

    @abstractmethod
    def handle_EvWfArrival(
        self, ev_queue: list, ev_id: int, ev_time: float, event: EvWfArrival, task_to_metadata: dict
    ):
        pass

    @abstractmethod
    def handle_EvWfCompletion(
        self, ev_queue: list, ev_id: int, ev_time: float, event: EvWfCompletion, task_to_metadata: dict
    ):
        pass

    @abstractmethod
    def handle_EvObjStart(self, ev_queue: list, ev_id: int, ev_time: float, event: EvObjStart, task_to_metadata: dict):
        pass

    @abstractmethod
    def handle_EvObjEnd(self, ev_queue: list, ev_id: int, ev_time: float, event: EvObjEnd, task_to_metadata: dict):
        pass

    def set_runner(self, runner: Runner):
        self.runner = runner
        self.objtree: RegexTreeRegex = RegexTreeRegex(self.using_trace, runner)

    @abstractmethod
    def show_scheduling_info(self, wf: Workflow):
        pass

    @abstractmethod
    def schedule(self, ev_queue: list, ev_time: float):
        pass
