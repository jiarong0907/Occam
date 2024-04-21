import argparse
import csv
from scheduler.runner import Runner
from scheduler.occamscheduler import OccamDepSetScheduler, OccamFIFOScheduler
from scheduler.baselinescheduler import PerDcFIFOScheduler, PerDeviceFIFOScheduler
from scheduler.baselinescheduler import PerDcDepSetScheduler, PerDeviceDepSetScheduler
import sys

scheduler_choice_mapping = {
    "dc_fifo": PerDcFIFOScheduler,
    "dev_fifo": PerDeviceFIFOScheduler,
    "dc_depset": PerDcDepSetScheduler,
    "dev_depset": PerDeviceDepSetScheduler,
    "occam_depset": OccamDepSetScheduler,
    "occam_fifo": OccamFIFOScheduler,
}


def _configure() -> argparse.Namespace:
    """Sets up arg parser"""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-f",
        dest="folder",
        required=False,
        default="lessdc",
        help="The folder of run",
    )
    parser.add_argument(
        "-gs",
        dest="gs",
        required=False,
        default=1.0,
        help="The scale of the gap time",
    )
    parser.add_argument(
        "-es",
        dest="es",
        required=False,
        default=1.0,
        help="The scale of the exec time",
    )
    # parser.add_argument(
    #     "-w",
    #     dest="workload_file",
    #     # default="workload/workload_1.0_useFilter.txt",
    #     # default="workload/workload_no_single_dev_1.0_useFilter_const_exec_28544885.txt",
    #     default="workload/workload_no_single_dev_1.0_useFilter.txt",
    #     # default="vldb_tests/workload_test_vldb_fig4.txt",
    #     help="Workload file in txt or csv format",
    # )
    parser.add_argument(
        "-s",
        dest="scheduler",
        choices=["dc_fifo", "dev_fifo", "dc_depset", "dev_depset", "occam_depset", "occam_fifo"],
        required=False,
        default="occam_depset",
        help="The choice of scheduler",
    )
    parser.add_argument(
        "-o",
        dest="result_file",
        default="occam_depset.txt",
        help="Result file path",
    )
    parser.add_argument(
        "-n",
        dest="num_wf",
        default=1000,
        type=int,
        help="The number of wfs you want to run, -1 means run all",
    )
    parser.add_argument(
        "-l",
        dest="log_file",
        default=None,
        help="Log file path",
    )

    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    """The main program"""
    csv.field_size_limit(sys.maxsize)

    runner = Runner(args.folder, args.result_file, args.num_wf, args.gs, args.es)
    scheduler_cls = scheduler_choice_mapping[args.scheduler]
    scheduler = scheduler_cls()
    runner.set_scheduler(scheduler)

    runner.run()

    runner.output_result()


if __name__ == "__main__":
    main(_configure())
