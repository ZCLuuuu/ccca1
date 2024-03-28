from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

print('Process No.' + str(rank) + "/" + str(size))

if rank == 0:
    data = [i for i in range(size)]

else:
    data = None

## broadcast
# data = comm.bcast(data, root=0)
## scatter
# data = comm.scatter(data, root=0)
## gather
data = rank
gathered_data = comm.gather(data, root=0)
if rank == 0:
    print("Gathered data at root:", gathered_data)


print("data of process No." + str(rank) + ":" + str(data))