import ast
import random
import pickle
import csv
import re
from regex.regextool import RegexTool
from scheduler.scheduler import Scheduler
import sys
import time
from heapq import heappush, heappop
from tools.util import alloc_event_id
from .workflow import WfObj, Workflow, AccType
from scheduler.events import EvWfArrival

trace_type = AccType.RW
cache_hit_rate = 0.95
use_regextree_dev_opt = False
regextree_dev_opt_thresh = 1e3
random.seed(0)


class Runner:
    def __init__(self, folder, output_file_path, num_wf, gs, es):
        self.use_regextree_dev_opt = use_regextree_dev_opt
        self.regextree_dev_opt_thresh = regextree_dev_opt_thresh

        path_prefix_regex = "data_process/synthetic_regex/" + folder
        path_prefix_workload = "workload/" + folder

        self.path_dc_database = path_prefix_regex + "/dcs.txt"
        self.path_device_database = path_prefix_regex + "/devices.txt"
        self.path_reg2dev = path_prefix_regex + "/regex_device_map.long"
        self.path_fsm_cache = path_prefix_regex + "/reg2fsm.pkl"
        self.path_workload = path_prefix_workload + "/workload_synthetic_gs" + str(gs) + "_es" + str(es) + ".txt"

        self.task_to_metadata = {}
        self.ev_queue = []
        self.output_file_path = output_file_path
        self.num_wf = num_wf

        self.dc_cache = open(self.path_dc_database, "r").read().splitlines()
        self.device_cache = open(self.path_device_database, "r").read().splitlines()
        self.device_cache_dict = dict()
        for d in self.device_cache:
            self.device_cache_dict[d] = d

        print("loading the fsm cache...")
        with open(self.path_fsm_cache, "rb") as f:
            original_fsm_cache = pickle.load(f)

        if len(original_fsm_cache) == 0:
            print("empty cache:", self.path_fsm_cache)
            assert 0

        self.fsm_cache = dict()
        hit_count = 0
        for reg in original_fsm_cache:
            if random.randint(1, 100) > cache_hit_rate * 100:
                continue
            self.fsm_cache[reg] = original_fsm_cache[reg]
            hit_count += 1
        print("cache hit rate = ", hit_count / len(original_fsm_cache))

        print("generating the regex to device list cache...")
        self.get_reg2dev_map()
        print("generating the regex to dc list cache...")
        self.get_reg2dc_map()

        self.acctype_map = {
            "cableguy_ping_test": AccType.READ,
            "bb_circuit_turnup": AccType.WRITE,
            "vmhandler": AccType.WRITE,
            "dne_device_state_change": AccType.WRITE,
            "device_data_audit": AccType.READ,
            "drain_undrain_devices": AccType.WRITE,
            "ens_ops_breakfix_base_workflow": AccType.READ,
            "collection_analysis_troubleshooting": AccType.READ,
            "px_lock_and_push_to_routers_sub": AccType.WRITE,
            "dc_matryoshka_configen_runner": AccType.WRITE,
            "rdam_dc": AccType.READ,
        }
        self.acctype_map_writeheavy = {
            "cableguy_ping_test": AccType.READ,
            "bb_circuit_turnup": AccType.WRITE,
            "vmhandler": AccType.WRITE,
            "dne_device_state_change": AccType.WRITE,
            "device_data_audit": AccType.WRITE,
            "drain_undrain_devices": AccType.WRITE,
            "ens_ops_breakfix_base_workflow": AccType.WRITE,
            "collection_analysis_troubleshooting": AccType.WRITE,
            "px_lock_and_push_to_routers_sub": AccType.WRITE,
            "dc_matryoshka_configen_runner": AccType.WRITE,
            "rdam_dc": AccType.WRITE,
        }
        self.acctype_map_balance = {
            "cableguy_ping_test": AccType.READ,
            "bb_circuit_turnup": AccType.WRITE,
            "vmhandler": AccType.WRITE,
            "dne_device_state_change": AccType.WRITE,
            "device_data_audit": AccType.READ,
            "drain_undrain_devices": AccType.WRITE,
            "ens_ops_breakfix_base_workflow": AccType.READ,
            "collection_analysis_troubleshooting": AccType.WRITE,
            "px_lock_and_push_to_routers_sub": AccType.WRITE,
            "dc_matryoshka_configen_runner": AccType.WRITE,
            "rdam_dc": AccType.READ,
        }
        self.acctype_map_readheavy = {
            "cableguy_ping_test": AccType.READ,
            "bb_circuit_turnup": AccType.WRITE,
            "vmhandler": AccType.WRITE,
            "dne_device_state_change": AccType.READ,
            "device_data_audit": AccType.READ,
            "drain_undrain_devices": AccType.READ,
            "ens_ops_breakfix_base_workflow": AccType.READ,
            "collection_analysis_troubleshooting": AccType.READ,
            "px_lock_and_push_to_routers_sub": AccType.READ,
            "dc_matryoshka_configen_runner": AccType.WRITE,
            "rdam_dc": AccType.READ,
        }

        self.get_worklod()

    def get_reg2dc_map(self):
        def get_dcs(regex):
            matched_dcs = set()
            dc_substr = re.findall(r"(_dc\d{4}_\d+|_dc\d{4}_\\d\+)", regex)
            assert dc_substr
            if len(dc_substr) == 1 and re.match(r"_dc\d{4}_\d+", dc_substr[0]):
                matched_dcs.add(dc_substr[0])
            else:
                dcs = self.dc_cache
                for dc_s in dc_substr:
                    all_match = [d for d in dcs if re.match(dc_s, d)]
                    matched_dcs.update(set(all_match))
            return list(matched_dcs)

        def get_dcs_synthetic(regex):
            dc_substr = regex[regex.find("_dc") :]
            assert len(dc_substr) > 3
            dcs = self.dc_cache
            all_match = [d for d in dcs if re.match(dc_substr, d)]
            return list(set(all_match))

            # dc_substr =re.findall(r'(_dc\d{4}_\d+|_dc\d{4}_\\d\+)', regex)
            # assert dc_substr
            # if len(dc_substr) == 1 and re.match(r'_dc\d{4}_\d+', dc_substr[0]):
            #     matched_dcs.add(dc_substr[0])
            # else:
            #     dcs = self.dc_cache
            #     for dc_s in dc_substr:
            #         all_match = [d for d in dcs if re.match(dc_s, d)]
            #         matched_dcs.update(set(all_match))
            # return list(matched_dcs)

        self.reg2dc_map = dict()
        for reg in self.reg2dev_map:
            self.reg2dc_map[reg] = get_dcs_synthetic(reg)

    def get_reg2dev_map(self):
        self.reg2dev_map = dict()
        rows = open(self.path_reg2dev, "r").read().splitlines()
        for r in rows:
            reg, dev = r.split("&")
            reg = reg.strip()
            dev = dev.strip()
            if reg in self.fsm_cache:
                self.reg2dev_map[reg] = ast.literal_eval(dev)

    def get_worklod(self):
        wf_list = []
        if self.path_workload.endswith(".csv"):
            with open(self.path_workload, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    (start_time, wf_name, exec_time, regex, device_list,) = (
                        int(row["start_time"]),
                        row["wf_name"],
                        int(row["exec_time"]),
                        row["regex"],
                        row["device_list"],
                    )

                    device_list = ast.literal_eval(device_list)
                    wf = Workflow(name=f"{start_time}_{wf_name}")
                    wf.add_obj(WfObj(regex, exec_time, AccType.WRITE))
                    wf_list.append((wf, start_time, exec_time, regex))

        elif self.path_workload.endswith(".txt"):
            workload = open(self.path_workload, "r").read().splitlines()
            read = 0
            single_dev = 0
            for wl in workload:
                start_time, wf_name, exec_time, regex = wl.split()
                start_time = int(start_time)
                exec_time = int(exec_time)

                regex2 = regex.replace("\\.", ".")
                if regex2 in self.device_cache_dict:
                    single_dev += 1

                wf = Workflow(name=f"{start_time}_{wf_name}")
                if trace_type == AccType.READ:
                    wf.add_obj(WfObj(regex, exec_time, AccType.READ))
                elif trace_type == AccType.WRITE:
                    wf.add_obj(WfObj(regex, exec_time, AccType.WRITE))
                elif trace_type == AccType.RW:
                    wf.add_obj(WfObj(regex, exec_time, self.acctype_map_readheavy[wf_name]))
                if self.acctype_map_readheavy[wf_name] == AccType.READ:
                    read += 1
                wf_list.append((wf, start_time, exec_time, regex))
            print("read:", read, "ratio:", float(read) / len(workload))
            print("single_dev:", single_dev)
            print("all:", len(workload))

        for wf, start_time, exec_time, regex in wf_list:
            heappush(self.ev_queue, (start_time, alloc_event_id(), EvWfArrival(ev_time=start_time, wf=wf, runner=self)))
            self.task_to_metadata[wf._name] = {
                "start_time": start_time,
                "wf_name": wf._name,
                "exec_time": exec_time,
                "regex": regex,
                "schedule_time": 0,
                "insert_time": 0,
                "delete_time": 0,
                "actual_start_time": None,
                "finish_time": None,
            }

            if self.num_wf > 0 and len(self.ev_queue) >= self.num_wf:
                break

    def set_scheduler(self, scheduler):
        self.scheduler: Scheduler = scheduler
        self.scheduler.set_runner(self)

    def run(self):
        timer = 0.0
        ev_queue = self.ev_queue
        task_to_metadata = self.task_to_metadata
        current_arrival = 0
        last_arrival = 0
        current_complete = 0
        count_after_all_arrival = 0
        total_arrival = len(ev_queue)
        show_progress = True
        exec_st_time = time.time()  # start time

        scheduler = self.scheduler

        print("=" * 40, "Start simulation", "=" * 40)

        while len(ev_queue):
            if show_progress:
                if (
                    current_arrival > 0
                    and current_arrival % 10 == 0
                    and (last_arrival == 0 or current_arrival != last_arrival)
                ) or (current_arrival == total_arrival and count_after_all_arrival % 20 == 0):
                    sys.stdout.write(
                        "Sim_t: %d ms     " % (timer)
                        + "Exec_t: %-5.3f s    " % (time.time() - exec_st_time)
                        + "WF arr: %d (%.2f%%)    " % (current_arrival, float(current_arrival / total_arrival * 100))
                        + "WF com: %d (%.2f%%)    " % (current_complete, float(current_complete / total_arrival * 100))
                        + "running: %d    " % (len(self.scheduler.wf_list_running))
                        + "pending: %d\n" % (len(self.scheduler.wf_list_pending))
                    )
                    sys.stdout.flush()
                    last_arrival = current_arrival

            timer = ev_queue[0][0]  # Set timer to next event's ev_time

            event_tuple = heappop(ev_queue)
            ev_time = event_tuple[0]
            ev_id = event_tuple[1]
            event = event_tuple[2]
            ev_type = event._ev_type

            # ---- Handle Events ----
            event.call_handler(ev_queue, ev_id, ev_time, task_to_metadata)
            if ev_type == "EvWfArrival":
                current_arrival += 1
            elif ev_type == "EvWfCompletion":
                current_complete += 1
            if current_arrival == total_arrival:
                count_after_all_arrival += 1

        sys.stdout.write(
            "Sim_t: %d ms     " % (timer)
            + "Exec_t: %-5.3f s    " % (time.time() - exec_st_time)
            + "WF arr: %d (%.2f%%)    " % (current_arrival, float(current_arrival / total_arrival * 100))
            + "WF com: %d (%.2f%%)    " % (current_complete, float(current_complete / total_arrival * 100))
            + "running: %d    " % (len(self.scheduler.wf_list_running))
            + "pending: %d\n" % (len(self.scheduler.wf_list_pending))
        )
        sys.stdout.flush()
        expected = "\n".join(scheduler.records)
        with open(self.output_file_path + ".log", "w") as output_file:
            for entry in scheduler.records:
                output_file.write(entry)

        with open(self.output_file_path + "_q_len.txt", "w") as output_file:
            for entry in scheduler.pending_q_len:
                output_file.write(str(entry) + "\n")

        with open(self.output_file_path + "_active_netobj.txt", "w") as output_file:
            for entry in scheduler.active_netobj:
                output_file.write(str(entry) + "\n")

        assert expected.count("EvWfArrival") == expected.count("EvWfCompletion") + expected.count("Deadlock")
        assert len(scheduler.objtree._root.get_children()) == 0

        return scheduler.records

    def output_result(self):
        ordered_by_start = sorted(self.task_to_metadata.values(), key=lambda x: x["start_time"])
        with open(self.output_file_path + ".txt", "w") as output_file:
            for entry in ordered_by_start:
                output_file.write(
                    f"{entry['start_time']:12}\t"
                    f"{entry['actual_start_time']: 12}\t"
                    f"{entry['finish_time']: 12}\t"
                    f"{entry['wf_name']:50}\t"
                    f"{entry['exec_time']:12}\t"
                    f"{entry['schedule_time']:10}\t"
                    f"{entry['regex']}\n"
                )

        with open(self.output_file_path + "_sch.txt", "w") as output_file:
            for entry in ordered_by_start:
                output_file.write(
                    f"{entry['wf_name']:50}\t"
                    f"{entry['schedule_time']:10}\t"
                    f"{entry['insert_time']:10}\t"
                    f"{entry['delete_time']:10}\t"
                    f"{entry['regex']}\n"
                )


class TestRunner:
    def __init__(self, ev_queue: list):
        self.ev_queue = ev_queue
        self.fsm_cache = dict()
        self.reg2dev_map = dict()
        self.device_cache = list()
        self.device_cache_dict = dict()
        self.use_regextree_dev_opt = False

    def set_scheduler(self, scheduler):
        self.scheduler: Scheduler = scheduler
        self.scheduler.set_runner(self)

    def run(self):
        timer = 0.0
        ev_queue = self.ev_queue
        task_to_metadata = {}
        current_arrival = 0
        last_arrival = 0
        current_complete = 0
        total_arrival = len(ev_queue)
        show_progress = True
        exec_st_time = time.time()  # start time

        scheduler = self.scheduler

        print("=" * 40, "Start simulation", "=" * 40)

        while len(ev_queue):
            if show_progress:
                if (
                    current_arrival > 0
                    and current_arrival % 10 == 0
                    and (last_arrival == 0 or current_arrival != last_arrival)
                ):
                    sys.stdout.write(
                        "Sim Time: %dms     " % (timer)
                        + "Exec Time: %-5.3f seconds    " % (time.time() - exec_st_time)
                        + "Workflow arrival: %d (%f%%)    "
                        % (current_arrival, float(current_arrival / total_arrival * 100))
                        + "Workflow complete: %d (%f%%)    \n"
                        % (current_complete, float(current_complete / total_arrival * 100))
                    )
                    sys.stdout.flush()
                    last_arrival = current_arrival

            timer = ev_queue[0][0]  # Set timer to next event's ev_time

            event_tuple = heappop(ev_queue)
            ev_time = event_tuple[0]
            ev_id = event_tuple[1]
            event = event_tuple[2]
            ev_type = event._ev_type

            # ---- Handle Events ----
            event.call_handler(ev_queue, ev_id, ev_time, task_to_metadata)
            if ev_type == "EvWfArrival":
                current_arrival += 1
            elif ev_type == "EvWfCompletion":
                current_complete += 1

        sys.stdout.write(
            "Sim Time: %dms     " % (timer)
            + "Exec Time: %-5.3f seconds    " % (time.time() - exec_st_time)
            + "Workflow arrival: %d (%f%%)    " % (current_arrival, float(current_arrival / total_arrival * 100))
            + "Workflow complete: %d (%f%%)    \n" % (current_complete, float(current_complete / total_arrival * 100))
        )
        sys.stdout.flush()
        return scheduler.records
