from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

print('Process No.' + str(rank) + "/" + str(size))

if rank == 0:
    data = 128
    req = comm.send(data, dest=1, tag=0)
    print("msg content:" + str(data) + " sent by process No." + str(rank))
elif rank == 1:
    data = comm.recv(source=0, tag=0)
    print("msg content:" + str(data) + " received by process No." + str(rank))
else:
    pass
