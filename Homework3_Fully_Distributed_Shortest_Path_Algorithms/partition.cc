#include <mpi.h>
#include <iostream>
#include <fstream>
#include <limits>

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
    /* Get the size of the file */
    MPI_Offset offset;
    MPI_File_get_size(mf, &offset);
    /* Calculate how many elements that is */
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

int Grouping(int v_num, int group_length, int* adj_matrix, int* v_list, int root, int proc){
    int inf = std::numeric_limits<int>::max();
    int node = root;
    int remain = group_length;
    int flag = 0;
    for (int i=0; i<group_length; i++){
        for (int j=0; j<v_num; j++){
            if (adj_matrix[node*v_num+j] != inf && v_list[j] == -1){
                v_list[j] = proc;
		node = j;
		break;
	    }
	    else if (j == v_num-1){
		flag = 1;
	    }
        }
	if (flag == 1) break;
	remain--;
    }
    return remain;
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
    int proc_num = atoi(argv[3]);
    int* input;
    input = ReadFile(in_file);
    
    if (rank == 0){
        // Build adjacent matrix
        int v_num = input[0];
        int e_num = input[1];
        int batch_size = v_num / proc_num;
        int *adj_matrix = new int[v_num*v_num];
        BuildAdjMetrix(input, adj_matrix);

        // Group
        int* v_list = new int[v_num];
        for (int i=0; i<v_num; i++) v_list[i] = -1; 
        for (int i=0; i<proc_num; i++){
            if (i == proc_num-1) batch_size = batch_size + v_num % proc_num;
            int remain = batch_size;
            for (int j=0; j<v_num; j++) {
	        if (v_list[j] == -1){
		    remain--;
		    v_list[j] = i;
		    if (remain > 0){
                       remain = Grouping(v_num, remain, adj_matrix, v_list, j, i);
		    }
	        }
	       if (remain == 0) break;
	    }
        }

        // Write File
        fstream file;
        file.open(out_file, ios::out);
        for(int i=0; i<v_num; i++){
            file << v_list[i] << endl;
        }
        file.close();
    }

    MPI_Finalize();
}

