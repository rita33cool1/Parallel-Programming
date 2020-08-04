#include <mpi.h>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <limits>
#include <cmath>
#include <ctime>
#include <algorithm>
using namespace std;

bool EphaseObatch(int is_begin, float *input, int world_rank, int world_size, int last, int batch_size);
bool OphaseEbatch(int is_begin, float *input, int world_rank, int world_size, int last, int batch_size);
bool OphaseObatch(int is_begin, float *input, int world_rank, int world_size, int last, int batch_size);
bool SingleProc(int OorE, float *input, int last);
bool SingleProcCheck(float *input, int last);
bool AnySwap(bool is_swap);
void CheckDesc(float *input, int last);

int main(int argc, char* argv[]) {
    clock_t t1, t2; // typedef time_t long;
    t1 = clock();
    MPI_Init(&argc, &argv);

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    int N = atoi(argv[1]);
    int batch_size = N/(world_size);
    int last = batch_size-1;
    if (world_rank == world_size-1) last += N%world_size;
    int input_size = last+1;

    // Read file
    float read_buff[N];
    float input[input_size];
    MPI_File mf;
    MPI_File_open(MPI_COMM_WORLD, argv[2], MPI_MODE_RDONLY, MPI_INFO_NULL, &mf);
    MPI_File_read_all(mf, &read_buff, N, MPI_FLOAT, MPI_STATUS_IGNORE);
    MPI_File_close(&mf);
    
    for (int i=batch_size*world_rank, j=0; i<batch_size*(world_rank)+input_size; i++, j++){
	input[j] = read_buff[i];
	//cout<<world_rank<<", input i: "<<i<<", j: "<<j<<", input[j]: "<<input[j]<<", read_buff[i]: "<<read_buff[i]<<endl;
    }

    //Check Descending to avoid worst case
    //CheckDesc(input, last);

    /**
    // randomize to avoid worst case
    if (last > 4000){
        random_shuffle(&input[0], &input[last]);
    }
    **/
    int stop_i = 0;

    bool even_is_swap = false;
    bool odd_is_swap = true;
    while (true){
	bool not_diff_size_last = true;
        //cout<<world_rank<<", stop_i: "<<stop_i<<endl;
	
	// Even phrase
	//MPI_Barrier(MPI_COMM_WORLD);
	if (world_rank == world_size-1 && last != batch_size-1) not_diff_size_last = false;
	if (batch_size > 0){
	    MPI_Barrier(MPI_COMM_WORLD);
	    // Even batch size
            if (batch_size % 2 == 0 || world_size == 1){
                even_is_swap = SingleProc(0, input, last);
                //cout<<world_rank<<", EVEN1"<<endl; 
                //cout<<world_rank<<", even_is_swap1"<<even_is_swap<<endl; 
            }
	    // Odd batch size
	    else{
	        even_is_swap = EphaseObatch((batch_size*world_rank)%2, input, world_rank, world_size, last, batch_size);
                //cout<<world_rank<<", EVEN2"<<endl; 
                //cout<<world_rank<<", even_is_swap2"<<even_is_swap<<endl; 
	    }
	}
	else if (world_rank < world_size-1){
            //cout<<world_rank<<", CK success"<<endl; 
	    break;
	}
	// proc number > data num
	else {
	    even_is_swap = SingleProc(0, input, last);
            //cout<<world_rank<<", EVEN3"<<endl;
            //cout<<world_rank<<", even_is_swap3"<<even_is_swap<<endl; 
	}
	/**
       	cout<<world_rank<<", output: ";
        for(int i=0; i<=last; i++) 
       	    cout<<input[i]<<" ";
	cout<<endl;
        **/
	
	// Check
        if (batch_size > 0){
	    //MPI_Barrier(MPI_COMM_WORLD);
	    even_is_swap = AnySwap(even_is_swap);
            //cout<<world_rank<<", even any_swap: "<<even_is_swap<<endl; 
            if (!(even_is_swap || odd_is_swap)) {
                //cout<<world_rank<<", CK success"<<endl; 
	        break;
	    }
	}
	else if (world_rank == world_size-1){
           if (SingleProcCheck(input, last)){
                //cout<<world_rank<<", CK success"<<endl; 
	        break;
	   }
	}
        //cout<<world_rank<<", CK fail"<<endl; 
        
	// Odd phrase
	//MPI_Barrier(MPI_COMM_WORLD);
        if (batch_size > 0){
	    MPI_Barrier(MPI_COMM_WORLD);
	    // Single process
	    if (world_size == 1){
		odd_is_swap = SingleProc(1, input, last);
                //cout<<world_rank<<", odd_is_swap1"<<odd_is_swap<<endl; 
	    }
	    // Even batch
	    else if (batch_size%2 == 0){
                odd_is_swap = OphaseEbatch((batch_size*world_rank+1)%2, input, world_rank, world_size, last, batch_size);
                //cout<<world_rank<<", odd_is_swap2"<<odd_is_swap<<endl; 
	    }
	    // Odd batch
	    else if (batch_size%2 == 1){
                odd_is_swap = OphaseObatch((batch_size*world_rank+1)%2, input, world_rank, world_size, last, batch_size);
                //cout<<world_rank<<", odd_is_swap3"<<odd_is_swap<<endl; 
	    }
	    //cout<<world_rank<<", ODD"<<endl; 
	}
	// proc number > data num
	else if (world_rank == world_size-1){
	    odd_is_swap = SingleProc(1, input, last);
            //cout<<world_rank<<", ODD"<<endl; 
            //cout<<world_rank<<", odd_is_swap4"<<odd_is_swap<<endl; 
	}
	/**
       	cout<<world_rank<<", output: ";
        for(int i=0; i<=last; i++) 
       	    cout<<input[i]<<" ";
	cout<<endl;
	**/

	// Check
        if (batch_size > 0){
	    odd_is_swap = AnySwap(odd_is_swap);
            //cout<<world_rank<<", odd any_swap: "<<odd_is_swap<<endl; 
            if (!(even_is_swap || odd_is_swap)) {
                //cout<<world_rank<<", CK success"<<endl; 
	        break;
	    }
	}
	else if (world_rank == world_size-1){
           if (SingleProcCheck(input, last)){
                //cout<<world_rank<<", CK success"<<endl; 
	        break;
	   }
	}
        //cout<<world_rank<<", CK fail"<<endl; 
	//stop_i++;
    }
    //for (int j=0; j<=last; j++){
    //	cout<<world_rank<<", result j: "<<j<<", input: "<<input[j]<<endl;
    //}
    float output[N];
    if (world_size != 1){
        float gather_buff;
        MPI_Request gather_request, gather_send_request;
        MPI_Status gather_status;
        MPI_Gather(&input[0], batch_size, MPI_FLOAT, &output[world_rank*batch_size], batch_size, MPI_FLOAT, 0, MPI_COMM_WORLD);
        //cout<<world_rank<<" Gather !!!!!!!!!!!!!!!"<<endl;
        if (N%world_size != 0 && world_rank == world_size -1){
	    MPI_Isend(&input[batch_size], N%world_size, MPI_FLOAT, 0, 15, MPI_COMM_WORLD, &gather_send_request);
	    //cout<<world_rank<<", send: "<<0<<",  EX "<<endl;
        }
        else if (N%world_size != 0 && world_rank == 0){
	    MPI_Irecv(&output[(world_size)*batch_size], N%world_size, MPI_FLOAT, world_size-1, 15, MPI_COMM_WORLD, &gather_request);
            //cout<<world_rank<<", receive: "<<world_size-1<<", EX "<<endl;
            MPI_Wait(&gather_request, &gather_status);
        }
    }
    else{
	for (int i=0; i<N; i++)
            output[i] = input[i];
    }
    MPI_File wmf;
    int is_open;
    is_open = MPI_File_open(MPI_COMM_WORLD, argv[3], MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &wmf);
    if (is_open != MPI_SUCCESS) cout << "OPEN"<<endl;
    if (world_rank == 0){
        /**
	cout<<"Final Result "<<endl;;
        for (int i=0; i<N; i++){
	    cout<<"i: "<<i<<", output: "<<output[i]<<endl;
        }
	**/
	
        MPI_File_write(wmf, &output[0], N, MPI_FLOAT, MPI_STATUS_IGNORE);
    
        MPI_File_close(&wmf);
    }
    MPI_File_close(&wmf);
    MPI_Finalize();
    t2 = clock();
    //cout<<world_rank<<", using time: "<< (t2-t1)/(double)(CLOCKS_PER_SEC)<<endl;
}

// EVEN Phase
bool EphaseObatch(int is_begin, float *input, int world_rank, int world_size, int last, int batch_size){
    bool is_swap = false;
    float swap_num;
    float compare_num;
    //cout<<world_rank<<", is_begin "<<is_begin<<endl;
    //cout<<world_rank<<", last "<<last<<endl;
    //for (int i=is_begin; i<=last || (i == 1 && last == 0); i+=2){
    MPI_Request compare_request, compare_requestr;
    MPI_Status compare_status;
    MPI_Request swap_request1, swap_requestr1, swap_request2, swap_requestr2;
    MPI_Status swap_status1, swap_status2;
    
    // Send compare_num at first
    if (world_rank %2 == 1){
	//cout<<world_rank<<", i: "<<is_begin<<", send: "<<world_rank-1<<" compare_num: "<<input[0]<<endl;
        MPI_Isend(&input[0], 1, MPI_FLOAT, world_rank-1, 0, MPI_COMM_WORLD, &compare_request);    
        //MPI_Wait(&compare_request, &compare_status);
    }

    // Receive compare_num and swap compare_num if needed
    else if ( world_rank < world_size-1 && world_rank%2 == 0){
        // Send swap num
        MPI_Isend(&input[last], 1, MPI_FLOAT, world_rank+1, 1, MPI_COMM_WORLD, &swap_request1);
        //cout<<world_rank<<", send: "<< world_rank+1<<", swap_num1: "<<input[last]<<endl;
    }
    int i = is_begin;

    //for (; i<last || (i == is_begin && last == 0) || (last != batch_size-1 && i < last && world_rank == world_size-1); i+=2){
    for (; i<last || (i == is_begin && last == 0) ; i+=2){
    //for (; i<last-1; i+=2){
	//cout<<world_rank<<", PHASE i "<<i<<endl;
        //cout<<world_rank<<", is_begin: "<<is_begin<<endl;
	
	if (i < last && last > 0){
	    if (input[i] > input[i+1]){ 
                //cout<<world_rank<<", swap inside, i: "<<i<<", input[i]: "<<input[i]<<", input[i+1]: "<<input[i+1]<<endl;
	        //swap(input[i], input[i+1]);
	        float tmp = input[i];
		input[i] = input[i+1];
		input[i+1] = tmp;
		//cout<<world_rank<<", swap in process"<<endl;
	        is_swap = true;
	    }
	}
    }
    // Send compare_num at first
    if (world_rank %2 == 1){
        MPI_Irecv(&swap_num, 1, MPI_FLOAT, world_rank-1, 1, MPI_COMM_WORLD, &swap_requestr1);
        MPI_Wait(&swap_requestr1, &swap_status1);
        //cout<<world_rank<<", receive: "<<world_rank-1<<", swap_num1: "<<swap_num<<endl;
        if (swap_num > input[0]){
            input[0] = swap_num;
	    is_swap = true;
	}
    }
    // Receive compare_num and swap compare_num if needed
    else if ( world_rank < world_size-1 && world_rank%2 == 0){
        MPI_Irecv(&compare_num, 1, MPI_FLOAT, world_rank+1, 0, MPI_COMM_WORLD, &compare_requestr);
        MPI_Wait(&compare_requestr, &compare_status);
        //cout<<world_rank<<", receive: "<<world_rank+1<<", compare_num: "<<compare_num<<endl;
        if (input[last] > compare_num){
	    //cout<<world_rank<<", i: "<<last<<", input[i]: "<<input[last]<<endl;
            input[last] = compare_num;	
	    is_swap = true;
        }
    }
    return is_swap;
}

//ODD Phase Even batch
bool OphaseEbatch(int is_begin, float *input, int world_rank, int world_size, int last, int batch_size){
    bool is_swap = false;
    float swap_num;
    float compare_num;
    //cout<<world_rank<<", is_begin "<<is_begin<<endl;
    //cout<<world_rank<<", last "<<last<<endl;
    //for (int i=is_begin; i<=last || (i == 1 && last == 0); i+=2){
    MPI_Request compare_request, compare_requestr;
    MPI_Status compare_status;
    MPI_Request swap_request1, swap_requestr1, swap_request2, swap_requestr2;
    MPI_Status swap_status1, swap_status2;
    // Send compare_num at first
    if (world_rank > 0){
	//cout<<world_rank<<", i: "<<is_begin<<", send: "<<world_rank-1<<" compare_num: "<<input[0]<<endl;
        MPI_Isend(&input[0], 1, MPI_FLOAT, world_rank-1, 0, MPI_COMM_WORLD, &compare_request);    
        //MPI_Wait(&compare_request, &compare_status);
    }

    // Receive compare_num and swap compare_num if needed
    if ( world_rank < world_size-1 ){
        MPI_Isend(&input[last], 1, MPI_FLOAT, world_rank+1, 1, MPI_COMM_WORLD, &swap_request1);
        //cout<<world_rank<<", send: "<< world_rank+1<<", swap_num1: "<<input[last]<<endl;
    }

    int i = is_begin;
    //for (; i<last || (last != batch_size-1 && i < last && world_rank == world_size-1); i+=2){
    for (; i<last; i+=2){
	//cout<<world_rank<<", PHASE i "<<i<<endl;
        //cout<<world_rank<<", is_begin: "<<is_begin<<endl;
	
	if (i < last && last > 0){
	    if (input[i] > input[i+1]){ 
                //cout<<world_rank<<", swap inside, i: "<<i<<", input[i]: "<<input[i]<<", input[i+1]: "<<input[i+1]<<endl;
	        //swap(input[i], input[i+1]);
	        float tmp = input[i];
		input[i] = input[i+1];
		input[i+1] = tmp;
	        //cout<<world_rank<<", swap in process"<<endl;
	        is_swap = true;
	    }
	}
    
    }
    // Send compare_num at first
    if (world_rank > 0){
        MPI_Irecv(&swap_num, 1, MPI_FLOAT, world_rank-1, 1, MPI_COMM_WORLD, &swap_requestr1);
        MPI_Wait(&swap_requestr1, &swap_status1);
        //cout<<world_rank<<", receive: "<<world_rank-1<<", swap_num1: "<<swap_num<<endl;
        if (swap_num > input[0]){
            input[0] = swap_num;
	    is_swap = true;
	}
    }
    // Receive compare_num and swap compare_num if needed
    if ( world_rank < world_size-1 ){
        MPI_Irecv(&compare_num, 1, MPI_FLOAT, world_rank+1, 0, MPI_COMM_WORLD, &compare_requestr);
        MPI_Wait(&compare_requestr, &compare_status);
        //cout<<world_rank<<", receive: "<<world_rank+1<<", compare_num: "<<compare_num<<endl;
        if (input[last] > compare_num){
            input[last] = compare_num;	
	    is_swap = true;
        }
    }
    return is_swap;
}

//ODD Phase Odd batch
bool OphaseObatch(int is_begin, float *input, int world_rank, int world_size, int last, int batch_size){
    bool is_swap = false;
    float swap_num;
    float compare_num = false;
    //cout<<world_rank<<", is_begin "<<is_begin<<endl;
    //cout<<world_rank<<", last "<<last<<endl;
    //for (int i=is_begin; i<=last || (i == 1 && last == 0); i+=2){
    MPI_Request compare_request, compare_requestr;
    MPI_Status compare_status;
    MPI_Request swap_request1, swap_requestr1, swap_request2, swap_requestr2;
    MPI_Status swap_status1, swap_status2;
    // Send compare_num at first
    if (world_rank %2 == 0 && world_rank > 0){
	//cout<<world_rank<<", i: "<<is_begin<<", send: "<<world_rank-1<<" compare_num: "<<input[0]<<endl;
        MPI_Isend(&input[0], 1, MPI_FLOAT, world_rank-1, 0, MPI_COMM_WORLD, &compare_request);    
        //MPI_Wait(&compare_request, &compare_status);
    }
    // Receive compare_num and swap compare_num if needed
    else if ( world_rank < world_size-1 && world_rank%2 == 1){
        MPI_Isend(&input[last], 1, MPI_FLOAT, world_rank+1, 1, MPI_COMM_WORLD, &swap_request1);
        //cout<<world_rank<<", send: "<< world_rank+1<<", swap_num1: "<<input[last]<<endl;
    }
    int i = is_begin;

    //for (; i<last-1; i+=2){
    //for (; i<last || (last != batch_size-1 && i < last && world_rank == world_size-1); i+=2){
    for (; i<last; i+=2){
	//cout<<world_rank<<", PHASE i "<<i<<endl;
        //cout<<world_rank<<", is_begin: "<<is_begin<<endl;
	
	if (i < last && last > 0){
	    if (input[i] > input[i+1]){ 
                //cout<<world_rank<<", swap inside, i: "<<i<<", input[i]: "<<input[i]<<", input[i+1]: "<<input[i+1]<<endl;
	        //swap(input[i], input[i+1]);
	        float tmp = input[i];
		input[i] = input[i+1];
		input[i+1] = tmp;
	        //cout<<world_rank<<", swap in process"<<endl;
	        is_swap = true;
	    }
	}
    
    }
    // Send compare_num at first
    if (world_rank %2 == 0 && world_rank > 0){
        MPI_Irecv(&swap_num, 1, MPI_FLOAT, world_rank-1, 1, MPI_COMM_WORLD, &swap_requestr1);
        MPI_Wait(&swap_requestr1, &swap_status1);
        //cout<<world_rank<<", receive: "<<world_rank-1<<", swap_num1: "<<swap_num<<endl;
        if (swap_num > input[0]){
            input[0] = swap_num;
	    is_swap = true;
	}
    }
    // Receive compare_num and swap compare_num if needed
    else if ( world_rank < world_size-1 && world_rank%2 == 1){
        MPI_Irecv(&compare_num, 1, MPI_FLOAT, world_rank+1, 0, MPI_COMM_WORLD, &compare_requestr);
        MPI_Wait(&compare_requestr, &compare_status);
        //cout<<world_rank<<", receive: "<<world_rank+1<<", compare_num: "<<compare_num<<endl;
        if (input[last] > compare_num){
            input[last] = compare_num;	
	    is_swap = true;
        }
    }
    return is_swap;
}

bool SingleProc(int OorE, float *input, int last){
    //cout<<"enter SinProc"<<endl;
    bool is_swap = false;
    for(int i=OorE; i< last; i+=2){
        if(input[i] > input[i+1]){
	    //swap(input[i], input[i+1]);
	    float tmp = input[i];
	    input[i] = input[i+1];
	    input[i+1] = tmp;
	    //cout<<"SinProc swap i: "<<i<<", input["<<i<<"]: "<<input[i+1]<<", input["<<i+1<<"]: "<<input[i]<<endl;
	    is_swap = true;
	}
    }
    return is_swap;
}

bool SingleProcCheck(float *input, int last){
    bool is_success = true;
    for(int i=0; i < last; i++){
        if(input[i] > input[i+1]){
            is_success = false;
	    break;
	}
    }
    return is_success;
}

bool AnySwap(bool is_swap){
    bool any_swap;
    MPI_Reduce(&is_swap, &any_swap, 1, MPI_INT, MPI_BOR, 0, MPI_COMM_WORLD);
    //cout<<world_rank<<", after CHECK reduce"<<endl;
    MPI_Bcast(&any_swap, 1, MPI_INT, 0, MPI_COMM_WORLD);
    //cout<<world_rank<<", bcast_buff: "<<bcast_buff<<endl;
    return any_swap;
}

void CheckDesc(float *input, int last){
    bool is_desc = true;
    for (int i=0; i<last; i++){
        if (input[i] < input[i+1]){
            is_desc = false;
	}
    }
    if (is_desc){
        for (int i=0; i<last/2; i++){
            //swap(input[i], input[last-i]);
	    float tmp = input[i];
	    input[i] = input[last-i];
	    input[last-i] = tmp;
	}
    }
}
