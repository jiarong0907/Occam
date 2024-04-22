# Occam
Occam is a workflow system for reliable network management. It provides restricted APIs for network management programming, a regex-based locker and scheduler, and support for failure recovery. Read our paper [Occam (Eurosys'24)](https://jxing.me/pdf/occam-eurosys.pdf) for more details.

This repo includes the implementation of Occam's regex tree, locker, and scheduler. It also provides a simulator with management traces.

## Setup

A Vagrantfile has been provided in `${REPOROOT}`, which allows you to deploy the system with on command.

#### Software versions
```
Vagrant 2.2.10
VirtualBox Version: 5.1.38r122592
Guest Additions Version: 6.1.16
```
#### CPU & memory
In the Vagrantfile, we configure the VM to use 8 CPUs and 16GB memory. This is overkilling for most cases. Please consider adjusting the number based on your machine's available resources.

```
vb.cpus = 8
vb.memory = "16384"
```

#### Start VM and login

From `${REPOROOT}`:
```
vagrant up
# The vm uses default user name 'vagrant'. ssh to the VM by
vagrant ssh
```

This will create a Ubuntu-20.04 VM and automatically install our dependency using script `${REPOROOT}/vagrant_setup.sh`.


## Usage

To run a simulation
```
python3 main.py -f <folder_name> -gs <num> -es <num> -s <scheduler> -o <result_file> -n <number_of_workflows>
```

- `f`: The folder under workload that includes the trace. Default is `wait_time`.
- `gs`: The scale of the workflow arrival interval. Default is 1.0.
- `es`: The scale of the workflow execution time. Default is 1.0.
- `s`: Scheduler type. Default is `occam_depset` (LDSF used by Occam).
- `o`: Output file name. Default is `occam_depset.txt`.
- `n`: Number of workflow to simulate. Default is 1000.

## Development

Install pre-commit to enforce code style consistency and static checks. From `${REPOROOT}`:
```
pip install pre-commit
pre-commit install
```

## Citation

```
@inproceedings {occam-xing,
    author = {Xing, Jiarong and Hsu, Kuo-Feng and Xia, Yiting and Cai, Yan and Li, Yanping and Zhang, Ying and Chen, Ang},
    title = {Occam: A Programming System for Reliable Network Management},
    booktitle = {Proc. ACM EuroSys},
    year = {2024}
}
```

## License
The code is released under the [GNU Affero General Public License v3](https://www.gnu.org/licenses/agpl-3.0.html).
