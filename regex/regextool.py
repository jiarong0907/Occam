from __future__ import annotations
import re
from typing import TYPE_CHECKING

from regex.greenery.lego import lego, parse, from_fsm
from regex.greenery.fsm import fsm, anything_else
from tools.util import get_matched_devices

if TYPE_CHECKING:
    from scheduler.runner import Runner
    from regex.regextree import TreeObjRegex


class RegexTool:
    @classmethod
    def to_fsm(cls, regex) -> fsm:
        parsed = parse(regex)
        alphabet = parsed.alphabet()
        fsm = parsed.to_fsm(alphabet)
        return fsm.reduce()

    @classmethod
    def to_regex(cls, fsm) -> str:
        return str(from_fsm(fsm.reduce()))

    @classmethod
    def equal_regex(cls, fsm1: fsm, fsm2: fsm):
        return fsm1 == fsm2

    @classmethod
    def equal_regex_opt(cls, obj1: TreeObjRegex, obj2: TreeObjRegex, runner: Runner):
        if (
            runner.use_regextree_dev_opt
            and obj1._dev
            and obj2._dev
            and (len(obj1._dev) < runner.regextree_dev_opt_thresh or len(obj2._dev) < runner.regextree_dev_opt_thresh)
        ):
            return obj1._dev == obj2._dev

        obj1_dev = cls.is_single_device(obj=obj1, runner=runner)
        obj2_dev = cls.is_single_device(obj=obj2, runner=runner)

        if obj1_dev and obj2_dev:
            return obj1._regex == obj2._regex
        elif obj1_dev and not obj2_dev:
            return False
        elif not obj1_dev and obj2_dev:
            return False
        else:
            return obj1._fsm == obj2._fsm

    @classmethod
    def contain_regex(cls, fsm1: fsm, fsm2: fsm):
        return fsm1 >= fsm2

    @classmethod
    def is_single_device(cls, obj: TreeObjRegex, runner: Runner):
        if runner.use_regextree_dev_opt:
            return len(obj._dev) == 1

        regex = obj._regex
        regex = regex.replace("\\.", ".")
        return runner and regex in runner.device_cache_dict

    @classmethod
    def contain_regex_opt(cls, obj1: TreeObjRegex, obj2: TreeObjRegex, runner: Runner):
        if (
            runner.use_regextree_dev_opt
            and obj1._dev
            and obj2._dev
            and (len(obj1._dev) < runner.regextree_dev_opt_thresh or len(obj2._dev) < runner.regextree_dev_opt_thresh)
        ):
            return obj1._dev >= obj2._dev

        obj1_dev = cls.is_single_device(obj=obj1, runner=runner)
        obj2_dev = cls.is_single_device(obj=obj2, runner=runner)

        if obj1_dev and obj2_dev:
            return obj1._regex == obj2._regex
        elif obj1_dev and not obj2_dev:
            return False
        elif not obj1_dev and obj2_dev:
            return re.match(obj1._regex, obj2._regex)
        else:
            return obj1._fsm >= obj2._fsm

    @classmethod
    def contain_proper_regex(cls, fsm1: fsm, fsm2: fsm):
        return fsm1 > fsm2

    @classmethod
    def contain_proper_regex_opt(cls, obj1: TreeObjRegex, obj2: TreeObjRegex, runner: Runner):
        if (
            runner.use_regextree_dev_opt
            and obj1._dev
            and obj2._dev
            and (len(obj1._dev) < runner.regextree_dev_opt_thresh or len(obj2._dev) < runner.regextree_dev_opt_thresh)
        ):

            return obj1._dev > obj2._dev

        obj1_dev = cls.is_single_device(obj=obj1, runner=runner)
        obj2_dev = cls.is_single_device(obj=obj2, runner=runner)

        if obj1_dev and obj2_dev:
            return False
        elif obj1_dev and not obj2_dev:
            return False
        else:
            return obj1._fsm > obj2._fsm

    @classmethod
    def intersec_regex(cls, fsm1: fsm, fsm2: fsm):
        intersec = fsm1 & fsm2
        lhs = fsm1 - intersec
        rhs = fsm2 - intersec
        # return intersec.reduce(), lhs.reduce(), rhs.reduce()
        return intersec, lhs, rhs

    @classmethod
    def overlap_regex(cls, fsm1: fsm, fsm2: fsm):
        return not fsm1.isdisjoint(fsm2)

    @classmethod
    def overlap_regex_opt(cls, obj1: TreeObjRegex, obj2: TreeObjRegex, runner: Runner):
        if (
            runner.use_regextree_dev_opt
            and obj1._dev
            and obj2._dev
            and (len(obj1._dev) < runner.regextree_dev_opt_thresh or len(obj2._dev) < runner.regextree_dev_opt_thresh)
        ):
            return not obj1._dev.isdisjoint(obj2._dev)

        obj1_dev = cls.is_single_device(obj=obj1, runner=runner)
        obj2_dev = cls.is_single_device(obj=obj2, runner=runner)

        if obj1_dev and obj2_dev:
            return False
        elif obj1_dev and not obj2_dev:
            return re.match(obj2._regex, obj1._regex)
        elif not obj1_dev and obj2_dev:
            return re.match(obj1._regex, obj2._regex)
        else:
            return not obj1._fsm.isdisjoint(obj2._fsm)

    @classmethod
    def subtract_regex(cls, fsm1: fsm, fsm2: fsm) -> fsm:
        return fsm1.difference(fsm2)
