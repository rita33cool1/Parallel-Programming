#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <omp.h>
#define READ_FILE   MPI_MODE_RDONLY
#define WRITE_FILE  MPI_MODE_WRONLY | MPI_MODE_CREATE
#define BLACK 0
#define WHITE 1
#define MOORE_TAG 0
#define TOKEN_TAG 1
#define TERMINATE_TAG 2
#define INF 9999999
#define MAX 9999999
#define MAX_QUEUE 800000

int size, rank, vertex_num, edge_num, svertex, evertex, front, rear;
int *buf, *graph, *distance, *local_distance, *update, *local_update;
bool flag;

void ReadFile(char *filename, int mode){
    MPI_File fh;
    MPI_Status status;
    int mpi_err;

    MPI_File_open(MPI_COMM_WORLD, filename, mode, MPI_INFO_NULL, &fh);

    if(mode == READ_FILE){
      int buf_size;
      MPI_File_read_at( fh, 0, &vertex_num, 1, MPI_INT, &status );
      MPI_File_read_at( fh, 4, &edge_num, 1, MPI_INT, &status );
      if(edge_num*3 > vertex_num*vertex_num) buf_size = edge_num*3;
      else  buf_size = vertex_num*vertex_num;
      buf = new int[buf_size];
      MPI_File_read_at( fh, 8, buf, edge_num*3, MPI_INT, &status);
    }else if (mode == WRITE_FILE){
      int output_num = (evertex-svertex+1);
      exit(0);
      MPI_File_write_at_all( fh, svertex*sizeof(int), &distance[svertex], output_num, MPI_INT, MPI_STATUS_IGNORE);
      exit(0);
    }
    MPI_File_close(&fh);
}

void BuildAdjMatrix(){
    int src, dst, weight, index;
    for (int i=0; i<edge_num*3; i+=3){
        src = buf[i];
        dst = buf[i+1];
        weight = buf[i+2];
        index = src*vertex_num+dst;
        graph[index] = weight;
    }
}

int main(int argc, char* argv[]){
    // Initialization
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // execution time
    double start, cpu_time, io_time, network_time, total, tmp;
    // begining timestamp
    if (rank == 0) start = MPI_Wtime();

    // Read file
    ReadFile(argv[1], READ_FILE);
    graph = new int[vertex_num*vertex_num];
    distance = new int[vertex_num];
    update = new int[vertex_num];
    local_distance = new int[vertex_num];
    local_update = new int[vertex_num];
    // IO time
    if (rank == 0) io_time = MPI_Wtime() - start;


    // Build Adj matrix
    bool need_relax;
    int cnt;
    cnt = 0;
    need_relax= true;
    for(int i=0; i<vertex_num; i++){
       for(int j=0; j<vertex_num; j++){
          graph[i*vertex_num+j] = INF;
	  if(i==j) graph[i*vertex_num+j] =0;
       }
       update[i] = 0;
    }
    BuildAdjMatrix();

    // Assign distance value
    int local_vertex_num;
    local_vertex_num = (vertex_num< rank) ? 1:vertex_num/size;
    svertex = rank*local_vertex_num;
    evertex = svertex + local_vertex_num -1;
    evertex = (evertex >= vertex_num) ? vertex_num-1:evertex;
    evertex = (rank == size-1) ? vertex_num-1:evertex;
    if(rank == 0){
        distance[0] = 0;
        for(int i=1; i<vertex_num; i++){
	    if(graph[i]==INF) {
	        distance[i] = INF;
		update[i] = 0;
		continue;
	    }else{
                distance[i] = graph[i];
	        update[i] = 1;
	    }
        }
    }
    if(size != 1){
        // Network timestamp
	if (rank == 0) tmp = MPI_Wtime();
        MPI_Bcast(distance, vertex_num, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(update, vertex_num, MPI_INT, 0, MPI_COMM_WORLD);
	// Network time
	if (rank == 0) network_time += MPI_Wtime() - tmp;
    }

    // Moore
    while(need_relax){
        // Initialization
        for(int j=0; j<vertex_num; j++){
        local_update[j] = 0;
	local_distance[j] = distance[j];
        }

        // Relax node
        for(int i=svertex; i<=evertex; i++){
	    // Relax vertex i
            for(int j=1; j<vertex_num; j++){
                distance[j] = local_distance[i] + graph[i*vertex_num+j];
            }
	    for(int j=0; j<vertex_num; j++){
	        if(i!=j){
	            if(distance[j] < local_distance[j]){
		        local_update[j] = 1;
		        local_distance[j] = distance[j];
	            }
	        }
	    }
        }
        if(size != 1){
	    // Network timestamp
	    if (rank == 0) tmp = MPI_Wtime();
            MPI_Allreduce(local_distance, distance, vertex_num, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(local_update, update, vertex_num, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
            //Network time
	    if (rank == 0) network_time += MPI_Wtime() - tmp;
	}else{
            for(int i=0; i<vertex_num; i++){
                update[i] = local_update[i];
	        distance[i] = local_distance[i];
            }
        }
        for(int i=0; i<vertex_num; i++){
	    need_relax = false;
            if(update[i] !=0){
                need_relax = true;
	        break;
	    }
        }
    }
    // Cpu time
    if (rank == 0) cpu_time = MPI_Wtime() - start - network_time - io_time;

    // Write file
    if(rank == 0){
	// IO timestamp
	tmp = MPI_Wtime();
        FILE *w;
        w = fopen(argv[2], "wb");
        fwrite(distance, vertex_num*sizeof(int), 1, w);
	// IO time
        io_time += MPI_Wtime() - tmp;
	total = MPI_Wtime() - start;
	printf("%d,%d,%d,%d\n", total, cpu_time, network_time, io_time);
    }

    MPI_Finalize();
    return 0;
}
