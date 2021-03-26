#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include "timing.h"
#include <string.h>
#include <omp.h>
//#define N 1500 /* matrix size */

int main (int argc, char *argv[]) 
{
int	i, j, k;
int N = 1500;
timing_t tstart, tend;
double	a[N][N],           /* matrix A to be multiplied */
	b[N][N],           /* matrix B to be multiplied */
	c[N][N];           /* result matrix C */



  for (i=0; i<N; i++)
    for (j=0; j<N; j++)
      a[i][j]= i+j;

  for (i=0; i<N; i++)
    for (j=0; j<N; j++)
      b[i][j]= i*j;

  for (i=0; i<N; i++)
    for (j=0; j<N; j++)
      c[i][j]= 0;


  get_time(&tstart);
/*
  for (i=0; i<N; i++)
    {
    for(j=0; j<N; j++){
      for (k=0; k<N; k+=2) {
        c[i][j] += a[i][k] * b[k][j];
        c[i][j] += a[i][k+1] * b[k+1][j];

        }
      }
    }
*/

 //Blocking + unrolling innermost 2x
  int blockSize = N / 3;
  int ii,jj,kk;

  //#pragma omp parallel shared(blockSize,a,b,c,N) private(i,j,k,ii,kk,jj)
  //#pragma omp for
  for (ii = 0; ii < N; ii+=blockSize) {
    for (kk = 0; kk < N; kk+=blockSize) {
      for (jj = 0; jj < N; jj+=blockSize) {
        for (i = ii; i < ii+blockSize; i+=2) {
          for (k = kk; k < kk+blockSize; k++) {
            for (j = jj; j < jj+blockSize; j++) {
              c[i][j] += a[i][k]*b[k][j];
              c[i+1][j] += a[i+1][k]*b[k][j];
            }
          }
        }
      }
    }
  }
  
    get_time(&tend);



  printf("*****************************************************\n");

  double c_F = 0.0;
  for (i=0; i<N; i++) {
      for (j=0; j<N; j++) {
          c_F += c[i][j] * c[i][j];
          //printf("%6.2f   ", c[i][j]);
      }
      //printf("\n");
  }
  printf("||c||_F = %6.2f\n", sqrt(c_F));
  printf ("Elapsed Time: %g s.\n", timespec_diff(tstart,tend));
}



