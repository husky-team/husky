# This points to a file, which should contains hostnames (one per line).
# E.g.,
#
# worker1
# worker2
# worker3
#
MACHINE_CFG=

# Number of machines (workers)
MACHINE_NUM=
time mpiexec -np ${MACHINE_NUM} -f ${MACHINE_CFG} $@
