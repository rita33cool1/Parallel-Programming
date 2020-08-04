#include <mpi.h>
#include <iostream>
#include <stdlib.h>
#include <limits>
#include <algorithm>
#include <cstring>
#include <omp.h>

using namespace std;

void BuildAdjMetrix(int* input, int* adj_matrix){
    int v_num = input[0];
    int e_num = input[1];
    // Initialization, set diagonal being 0 and others being infinity
    for (int i=0; i<v_num; i++){
        for (int j=0; j<v_num; j++){
            if (i != j){
                adj_matrix[i*v_num+j] = std::numeric_limits<int>::max();
	    }
	    else
                adj_matrix[i*v_num+j] = 0;
	}
    }
    // Put the weights from input array into adj_matrix
    for (int i=2; i<2+3*e_num; i+=3){
        adj_matrix[input[i]*v_num+input[i+1]] = input[i+2];
    }
}

int* ReadFile(char* in_file){
    MPI_File mf;
    MPI_File_open(MPI_COMM_WORLD, in_file, MPI_MODE_RDONLY, MPI_INFO_NULL, &mf);
    // Get the size of the file //
    MPI_Offset offset;
    MPI_File_get_size(mf, &offset);
    // Calculate how many elements that is //
    int* num = new int[2];
    int* input = new int[offset];
    MPI_File_read_all(mf,
		      input,
		      offset,
		      MPI_INT,
		      MPI_STATUS_IGNORE);
    MPI_File_close(&mf);
    return input;
}

void FloydWarshall(int k, int v_num, int* matrix, int offset, int end, int rank) {
    int chunk = (end-offset+1)/omp_get_max_threads();
    #pragma omp parallel num_threads(omp_get_max_threads())   
	{
            #pragma omp for schedule(dynamic)   
            for (int i=offset; i<end; i++){
                for (int j=0; j<v_num; j++){
	            int inf = std::numeric_limits<int>::max();
                    if (matrix[i*v_num+k] != inf && matrix[k*v_num+j] != inf){
                        if (matrix[i*v_num+j] > matrix[i*v_num+k] + matrix[k*v_num+j]) {
                            matrix[i*v_num+j] = matrix[i*v_num+k] + matrix[k*v_num+j];
		        }
		    }
                }
            }
        }
}

int main(int argc, char* argv[]) {
    // Initialization
    MPI_Init(&argc, &argv);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Read file
    char* in_file = argv[1];
    char* out_file = argv[2];
    char* par_file = argv[3];
    int* input;
    input = ReadFile(in_file);

    // Build adjacent matrix
    int v_num = input[0];
    int e_num = input[1];
    int batch_size = v_num / size;
    int part_size = batch_size;
    if (rank == size-1) part_size = batch_size + v_num % size;
    int *adj_matrix = new int[v_num*v_num];
    BuildAdjMetrix(input, adj_matrix);

    // Floyd Warshall
    int start = batch_size*rank;
    int end = start + part_size;
    int batch = batch_size * v_num;
    int v2 = v_num * v_num;
    for (int k=0; k<v_num; k++){
	int change_rank = k / batch_size;
	if (change_rank > size-1) change_rank = size-1;
	// Broadcast the changed "k"th row
	MPI_Bcast(adj_matrix+k*v_num, v_num, MPI_INT, change_rank, MPI_COMM_WORLD);
	FloydWarshall(k, v_num, adj_matrix, start, end, rank);
    }

    // Gather result
    int* result = new int[v2];
    if (rank == size-1) 
	memcpy(result+v2-part_size*v_num, adj_matrix+v2-part_size*v_num, part_size*v_num*sizeof(int));
    MPI_Gather(adj_matrix+rank*batch, batch, MPI_INT, result, batch, MPI_INT, size-1, MPI_COMM_WORLD);
    
    // Write File
    if (rank == size-1){
        FILE *wmf;
        wmf = fopen(out_file, "w");
        fwrite( result, sizeof(int), v2, wmf);
	fclose(wmf);
    }
    MPI_Finalize();
}

