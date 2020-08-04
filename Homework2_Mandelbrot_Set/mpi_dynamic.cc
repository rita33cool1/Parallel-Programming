#define PNG_NO_SETJMP
#include <assert.h>
#include <png.h>
#include <stdio.h>
#include <stdlib.h>
//#include <string.h>
#include <cstring>
#include <mpi.h>
#include <iostream>

using namespace std;

#define MAX_ITER 10000

void write_png(const char* filename, const int width, const int height, const int* buffer) {
    FILE* fp = fopen(filename, "wb");
    assert(fp);
    png_structp png_ptr = png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
    assert(png_ptr);
    png_infop info_ptr = png_create_info_struct(png_ptr);
    assert(info_ptr);
    png_init_io(png_ptr, fp);
    png_set_IHDR(png_ptr, info_ptr, width, height, 8, PNG_COLOR_TYPE_RGB, PNG_INTERLACE_NONE,
                 PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT);
    png_write_info(png_ptr, info_ptr);
    size_t row_size = 3 * width * sizeof(png_byte);
    png_bytep row = (png_bytep)malloc(row_size);
    for (int y = 0; y < height; ++y) {
        memset(row, 0, row_size);
        for (int x = 0; x < width; ++x) {
            int p = buffer[(height - 1 - y) * width + x];
            png_bytep color = row + x * 3;
            if (p != MAX_ITER) {
                if (p & 16) {
                    color[0] = 240;
                    color[1] = color[2] = p % 16 * 16;
                } else {
                    color[0] = p % 16 * 16;
                }
            }
        }
        png_write_row(png_ptr, row);
    }
    free(row);
    png_write_end(png_ptr, NULL);
    png_destroy_write_struct(&png_ptr, &info_ptr);
    fclose(fp);
}

// mandelbrot set 
int mandelbrotset(int *image, double left, double right, double lower, double upper, int width, int height, int size, int rank){
    for (int j = 0; j <height; ++j) {
        if (j % size == rank) {
            double y0 = j * ((upper - lower) / height) + lower;
            for (int i = 0; i < width; ++i) {
                 double x0 = i * ((right - left) / width) + left;
                 int repeats = 0;
                 double x = 0;
                 double y = 0;
                 double length_squared = 0;
                 while (repeats < MAX_ITER && length_squared < 4) {
                     double temp = x * x - y * y + x0;
                     y = 2 * x * y + y0;
                     x = temp;
                     length_squared = x * x + y * y;
                     ++repeats;
                 }
                 image[j * width + i] = repeats;
            }
    	}
    }
    return 0;
}

// mandelbrot set 
int mandelbrotPerJ(int *image, double left, double right, double lower, double upper, int width, int height, int j){
    double y0 = j * ((upper - lower) / height) + lower;
    for (int i = 0; i < width; ++i) {
         double x0 = i * ((right - left) / width) + left;
         int repeats = 0;
         double x = 0;
         double y = 0;
         double length_squared = 0;
         while (repeats < MAX_ITER && length_squared < 4) {
             double temp = x * x - y * y + x0;
             y = 2 * x * y + y0;
             x = temp;
             length_squared = x * x + y * y;
             ++repeats;
         }
         image[j * width + i] = repeats;
    }
    return 0;
}

int main(int argc, char** argv) {
    /* argument parsing */
    assert(argc == 9);
    int num_threads = strtol(argv[1], 0, 10);
    double left = strtod(argv[2], 0);
    double right = strtod(argv[3], 0);
    double lower = strtod(argv[4], 0);
    double upper = strtod(argv[5], 0);
    int width = strtol(argv[6], 0, 10);
    int height = strtol(argv[7], 0, 10);
    const char* filename = argv[8];

    /* MPI Initialization */
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* allocate memory for image */
    int batch = height / size;
    int remainder = height % size;
    //int *image = new int[(batch+remainder)*width];
    int *image = new int[height*width];
    int *result = new int[height*width];
    assert(image);
    assert(result);
    
    // Dynamically Schedule
    // Only one process, use mpi_static
    if (size == 1){
        mandelbrotset(image, left, right, lower, upper, width, height, size, rank);
    }
    // Master
    else if (rank == 0){
        int job_j = size-1;
	int recv_flag;
	MPI_Request irecv_req, isend_req;
	MPI_Status irecv_st, isend_st;
	MPI_Recv(&recv_flag, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &irecv_st);
	MPI_Send(&job_j, 1, MPI_INT, irecv_st.MPI_SOURCE, 1, MPI_COMM_WORLD);
	job_j++;
	while (job_j < height) {
	    MPI_Recv(&recv_flag, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &irecv_st);
	    MPI_Send(&job_j, 1, MPI_INT, irecv_st.MPI_SOURCE, 1, MPI_COMM_WORLD);
	    job_j++;
	}
	// Check all process are finish
	int finish_sum = 0;
	job_j = height + 1;
	while (finish_sum < size-1){
	    MPI_Recv(&recv_flag, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &irecv_st);
	    // Inform other process the work has been completed
	    MPI_Send(&job_j, 1, MPI_INT, ++finish_sum, 1, MPI_COMM_WORLD);
	}
    }
    // Slave
    else{
	int send_j = rank-1;
	int recv_j = 0;
	// Calculate mandelbrot set
        mandelbrotPerJ(image, left, right, lower, upper, width, height, send_j);
	// Finish calculation then inform master (rank 0) 
	MPI_Request irecv_req, isend_req;
	MPI_Status irecv_st, isend_st;
	while (recv_j < height){
	    MPI_Send(&send_j, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
	    MPI_Recv(&recv_j, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, &irecv_st);
	    if (recv_j > height) break;
	    // Calculate mandelbrot set
            mandelbrotPerJ(image, left, right, lower, upper, width, height, recv_j);
	    send_j = recv_j;
	}        
    }


    MPI_Reduce(image, result, height*width, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);


    /* draw and cleanup */
    if (rank == 0) {
        write_png(filename, width, height, result);
    }

    MPI_Finalize();

    return 0;
}
