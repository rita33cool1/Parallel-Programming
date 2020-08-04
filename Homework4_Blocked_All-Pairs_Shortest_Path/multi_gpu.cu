#include <stdio.h>
#include <stdlib.h>
#include "cuda.h"

const int INF = 1000000000;
const int BLOCK_SIZE = 32;
int* input(char *inFileName);

__global__ void ApspPhase1 (const int k_block, size_t pitch, int* const matrix) {
    __shared__ int temp_matrix[BLOCK_SIZE][BLOCK_SIZE];

    // initialize parameters
    const int x_id = threadIdx.x;
    const int y_id = threadIdx.y;
    int base_1D_addr = BLOCK_SIZE * k_block;
    const int k_row = base_1D_addr + y_id;
    const int k_column = base_1D_addr + x_id;

    // Load Data
    const int ori_id = k_row * pitch + k_column;
		temp_matrix[x_id][y_id] = matrix[ori_id];

    // Synchronize
    __syncthreads();

    // Relax Edge
    int new_dist;
    for (int u = 0; u < BLOCK_SIZE; ++u) {
        new_dist = temp_matrix[x_id][u] + temp_matrix[u][y_id];
        if (new_dist < temp_matrix[x_id][y_id]) {
            temp_matrix[x_id][y_id] = new_dist;
        }
        __syncthreads();
    }
		matrix[ori_id] = temp_matrix[x_id][y_id];
}

__global__ void ApspPhase2(const int k_block, size_t pitch, int* const matrix) {
	  if (blockIdx.x == k_block) return;

    // initialize parameters
    const int x_id = threadIdx.x;
    const int y_id = threadIdx.y;
    int base_1D_addr = BLOCK_SIZE * k_block;
    int k_row = base_1D_addr + y_id;
    int k_column = base_1D_addr + x_id;
    __shared__ int temp_ori_matrix[BLOCK_SIZE][BLOCK_SIZE];

    // Load Data
    int ori_id = k_row * pitch + k_column;
    temp_ori_matrix[y_id][x_id] = matrix[ori_id];

    if (blockIdx.y == 0) {
        k_column = BLOCK_SIZE * blockIdx.x + x_id;
    } else {
        k_row = BLOCK_SIZE * blockIdx.x + y_id;
    }
    __shared__ int temp_matrix[BLOCK_SIZE][BLOCK_SIZE];
    int ori_dist;
    ori_id = k_row * pitch + k_column;
    temp_matrix[y_id][x_id] = matrix[ori_id];
		ori_dist = matrix[ori_id];

    // Synchronize Data
    __syncthreads();

    // Relax Data
    int new_dist;
    if (blockIdx.y == 0) {
        for (int u = 0; u < BLOCK_SIZE; ++u) {
            new_dist = temp_ori_matrix[y_id][u] + temp_matrix[u][x_id];
            if (new_dist < ori_dist) {
                ori_dist = new_dist;
            }
            temp_matrix[y_id][x_id] = ori_dist;
            __syncthreads();
        }
    } else {
        for (int u = 0; u < BLOCK_SIZE; ++u) {
            new_dist = temp_matrix[y_id][u] + temp_ori_matrix[u][x_id];

            if (new_dist < ori_dist) {
                ori_dist = new_dist;
            }
            __syncthreads();
            temp_matrix[y_id][x_id] = ori_dist;
            __syncthreads();
        }
    }
    matrix[ori_id] = ori_dist;
}

__global__ void ApspPhase3(const int k_block, size_t pitch, int* const matrix) {
	  if (blockIdx.x == k_block || blockIdx.y == k_block) return;

    // Calculate addresses
    const int x_id = threadIdx.x;
    const int y_id = threadIdx.y;
    // blockDim.y is the number of elements in a row
    const int row_in_matrix = blockDim.y * blockIdx.y + y_id;
    // blockDim.x is the number of elements in a column
    const int col_in_matrix = blockDim.x * blockIdx.x + x_id;
    __shared__ int row_temp[BLOCK_SIZE][BLOCK_SIZE];
    __shared__ int column_temp[BLOCK_SIZE][BLOCK_SIZE];
    int base_1D_addr = BLOCK_SIZE * k_block;
    int k_row = base_1D_addr + y_id;
    int k_column = base_1D_addr + x_id;

    // Load Data
    int ori_id;
    ori_id = k_row * pitch + col_in_matrix;
    row_temp[y_id][x_id] = matrix[ori_id];
    ori_id = row_in_matrix * pitch + k_column;
    column_temp[y_id][x_id] = matrix[ori_id];

    // Synchronize
    __syncthreads();

    // Edge Relax
    int ori_dist;
    int new_dist;
    ori_id = row_in_matrix * pitch + col_in_matrix;
    ori_dist = matrix[ori_id];
    for (int u = 0; u < BLOCK_SIZE; ++u) {
        new_dist = column_temp[y_id][u] + row_temp[u][x_id];
        if (ori_dist > new_dist) {
            ori_dist = new_dist;
        }
    }
   matrix[ori_id] = ori_dist;

}

int main(int argc, char* argv[]) {
  // Initial parameters
	int n, m;
	char* inFile;
	char* outFile;
	inFile = argv[1];
	outFile = argv[2];
	FILE* file = fopen(inFile, "rb");
	fread(&n, sizeof(int), 1, file);
	fread(&m, sizeof(int), 1, file);

  // Build expanded adjacent matrix
	int* Dist;
	int block_size = BLOCK_SIZE;
  // Move the last and unused space in the final block to the last columan and the last row
	int expand = block_size - (n % block_size);
  int expand_dim = n + expand;
	Dist = (int * )malloc(expand_dim * expand_dim * sizeof(int));
	// printf("expand = %d\n", expand);
	for (int i = 0; i < expand_dim; i++){
			for (int j = 0; j < expand_dim; j++){
					if(i < n && j < n){
							if (i == j) {
									Dist[i*expand_dim+j] = 0;
							} else {
									Dist[i*expand_dim+j] = INF;
							}
					}
					else{
							Dist[i*expand_dim+j] = INF;
					}
			}
	}
	int pair[3];
	for (int i = 0; i < m; i++) {
			fread(pair, sizeof(int), 3, file);
			Dist[pair[0]*expand_dim + pair[1]] = pair[2];
	}
	fclose(file);
	 printf("%d\n", n);
	 for( int i = 0; i < expand_dim; i++){
	 		for( int j = 0; j < expand_dim; j++){
	 		}printf("\n");
	 }
  fflush(stdout);

  // Move data from host to device
	int* device_s;
	int* device_t;
	size_t height = expand_dim;
	size_t width = height * sizeof(int);
	size_t pitch;
  // Transfer 1D matrix into 2D matrix
	cudaMallocPitch(&device_s, &pitch, width, height);
	cudaMallocPitch(&device_t, &pitch, width, height);
	cudaMemcpy2D(device_s, pitch, Dist, width, width, height, cudaMemcpyHostToDevice);
	cudaMemcpy2D(device_t, pitch, Dist, width, width, height, cudaMemcpyHostToDevice);

  // Floyd Warshall
  int round = expand_dim / block_size;
	dim3 dimGrid1( 1, 1, 1);
	dim3 dimGrid2( round , 2 , 1);
	dim3 dimGrid3( round , round, 1);
  // threads in a block
	dim3 dimBlock(block_size, block_size, 1);
        //#pragma omp parallel num_threads(2)
	for( int i = 0; i < round; i++) {
      ApspPhase1<<<dimGrid1, dimBlock>>>(i, pitch/sizeof(int), device_s);
			ApspPhase2<<<dimGrid2, dimBlock>>>(i, pitch/sizeof(int), device_s);
			ApspPhase3<<<dimGrid3, dimBlock>>>(i, pitch/sizeof(int), device_s);
	}

  // Move data from device to host
	int* result = (int *) malloc(expand_dim*expand_dim*sizeof(int));
	cudaMemcpy2D(result , width, device_s, pitch, width, height, cudaMemcpyDeviceToHost);
	 for( int i = 0; i < expand_dim; i++){
	 		for( int j = 0; j < expand_dim; j++){
	 				//printf("%d ", result[i*expand_dim+j ]);
	 		}printf("\n");
	 }


  // Write file
	FILE *outfile = fopen(outFile, "w");
	for (int i = 0; i < n; ++i) {
		for (int j = 0; j < n; ++j) {
            if (result[i*expand_dim+j] >= INF)
                result[i*expand_dim+j] = INF;
		}
		fwrite(result+i*expand_dim, sizeof(int), n, outfile);
	}
  fclose(outfile);
	free(result);
	free(Dist);

	return 0;
}

